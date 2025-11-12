use crate::Error;
use bytes::Bytes;
use std::fmt;
use std::fs::File;
use std::io::{self, Cursor, Read};
use wasi::http::types::IncomingBody;
use wasi::io::streams::InputStream;
use wasi::io::*;

/// An asynchronous request body.
#[derive(Debug)]
pub struct Body {
    kind: Option<Kind>,
}

#[allow(dead_code)]
struct ChunkIterator<'a> {
    bytes: &'a [u8],
    chunk_size: usize,
}

#[allow(dead_code)]
impl<'a> ChunkIterator<'a> {
    fn new(bytes: &'a [u8], chunk_size: usize) -> Self {
        ChunkIterator { bytes, chunk_size }
    }
}

#[allow(dead_code)]
impl<'a> Iterator for ChunkIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes.is_empty() {
            return None;
        }

        let chunk_size = std::cmp::min(self.chunk_size, self.bytes.len());
        let (chunk, rest) = self.bytes.split_at(chunk_size);
        self.bytes = rest;

        Some(chunk)
    }
}

impl Body {
    /// Instantiate a `Body` from a reader.
    ///
    /// # Note
    ///
    /// While allowing for many types to be used, these bodies do not have
    /// a way to reset to the beginning and be reused. This means that when
    /// encountering a 307 or 308 status code, instead of repeating the
    /// request at the new location, the `Response` will be returned with
    /// the redirect status code set.
    ///
    /// ```rust
    /// # use std::fs::File;
    /// # use reqwest::Body;
    /// # fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let file = File::open("national_secrets.txt")?;
    /// let body = Body::new(file);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// If you have a set of bytes, like `String` or `Vec<u8>`, using the
    /// `From` implementations for `Body` will store the data in a manner
    /// it can be reused.
    ///
    /// ```rust
    /// # use reqwest::Body;
    /// # fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = "A stringy body";
    /// let body = Body::from(s);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<R: Read + 'static>(reader: R) -> Body {
        Body {
            kind: Some(Kind::Reader(Box::from(reader), None)),
        }
    }

    /// Create a `Body` from a `Read` where the size is known in advance
    /// but the data should not be fully loaded into memory. This will
    /// set the `Content-Length` header and stream from the `Read`.
    ///
    /// ```rust
    /// # use std::fs::File;
    /// # use reqwest::Body;
    /// # fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let file = File::open("a_large_file.txt")?;
    /// let file_size = file.metadata()?.len();
    /// let body = Body::sized(file, file_size);
    /// # Ok(())
    /// # }
    /// ```
    pub fn sized<R: Read + 'static>(reader: R, len: u64) -> Body {
        Body {
            kind: Some(Kind::Reader(Box::from(reader), Some(len))),
        }
    }

    /// Creates a Body that consumes the given stream
    #[cfg(feature = "async")]
    pub fn from_stream<S>(stream: S) -> Body
    where
        S: futures::stream::Stream<Item = Result<Vec<u8>, Error>> + 'static,
    {
        Body {
            kind: Some(Kind::Stream(Box::pin(stream))),
        }
    }

    /// Returns the body as a byte slice if the body is already buffered in
    /// memory. For streamed requests this method returns `None`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self.kind {
            Some(Kind::Reader(_, _)) => None,
            Some(Kind::Bytes(ref bytes)) => Some(bytes.as_ref()),
            Some(Kind::Incoming { .. }) => None,
            #[cfg(feature = "async")]
            Some(Kind::Stream(_)) => None,
            None => None,
        }
    }

    /// Converts streamed requests to their buffered equivalent and
    /// returns a reference to the buffer. If the request is already
    /// buffered, this has no effect.
    ///
    /// Be aware that for large requests this method is expensive
    /// and may cause your program to run out of memory.
    #[cfg(not(feature = "async"))]
    pub fn buffer(&mut self) -> Result<&[u8], Error> {
        match self.kind {
            Some(Kind::Reader(ref mut reader, maybe_len)) => {
                let mut bytes = if let Some(len) = maybe_len {
                    Vec::with_capacity(len as usize)
                } else {
                    Vec::new()
                };
                io::copy(reader, &mut bytes).map_err(crate::error::builder)?;
                self.kind = Some(Kind::Bytes(bytes.into()));
                self.buffer()
            }
            Some(Kind::Bytes(ref bytes)) => Ok(bytes.as_ref()),
            Some(Kind::Incoming { ref stream, .. }) => {
                let mut body = Vec::new();
                let mut eof = false;
                while !eof {
                    match stream.blocking_read(u64::MAX) {
                        Ok(mut body_chunk) => {
                            body.append(&mut body_chunk);
                        }
                        Err(streams::StreamError::Closed) => {
                            eof = true;
                        }
                        Err(streams::StreamError::LastOperationFailed(err)) => {
                            return Err(Error::new(
                                crate::error::Kind::Body,
                                Some(err.to_debug_string()),
                            ));
                        }
                    }
                }
                self.kind = Some(Kind::Bytes(body.into()));
                self.buffer()
            }
            None => panic!("Body has already been extracted"),
        }
    }

    /// Converts streamed requests to their buffered equivalent and
    /// returns a reference to the buffer. If the request is already
    /// buffered, this has no effect.
    ///
    /// Be aware that for large requests this method is expensive
    /// and may cause your program to run out of memory.
    #[cfg(feature = "async")]
    pub async fn buffer(&mut self) -> Result<&[u8], Error> {
        match self.kind {
            Some(Kind::Reader(ref mut reader, maybe_len)) => {
                let mut bytes = if let Some(len) = maybe_len {
                    Vec::with_capacity(len as usize)
                } else {
                    Vec::new()
                };
                io::copy(reader, &mut bytes).map_err(crate::error::builder)?;
                self.kind = Some(Kind::Bytes(bytes.into()));
                let Some(Kind::Bytes(bytes)) = &self.kind else {
                    unreachable!()
                };
                Ok(bytes.as_ref())
            }
            Some(Kind::Bytes(ref bytes)) => Ok(bytes.as_ref()),
            Some(Kind::Incoming { ref stream, .. }) => {
                let mut body = Vec::new();
                let mut eof = false;
                while !eof {
                    match stream.blocking_read(u64::MAX) {
                        Ok(mut body_chunk) => {
                            body.append(&mut body_chunk);
                        }
                        Err(streams::StreamError::Closed) => {
                            eof = true;
                        }
                        Err(streams::StreamError::LastOperationFailed(err)) => {
                            return Err(Error::new(
                                crate::error::Kind::Body,
                                Some(err.to_debug_string()),
                            ));
                        }
                    }
                }
                self.kind = Some(Kind::Bytes(body.into()));
                let Some(Kind::Bytes(bytes)) = &self.kind else {
                    unreachable!()
                };
                Ok(bytes.as_ref())
            }
            Some(Kind::Stream(ref mut stream)) => {
                use futures::StreamExt;

                let mut bytes = Vec::new();

                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(data) => bytes.extend(data),
                        Err(err) => return Err(err),
                    }
                }

                self.kind = Some(Kind::Bytes(Bytes::from(bytes)));
                let Some(Kind::Bytes(bytes)) = &self.kind else {
                    unreachable!()
                };
                Ok(bytes.as_ref())
            }
            None => panic!("Body has already been extracted"),
        }
    }

    pub(crate) fn from_incoming(stream: InputStream, incoming_body: IncomingBody) -> Body {
        Body {
            kind: Some(Kind::Incoming {
                stream,
                incoming_body,
            }),
        }
    }

    pub(crate) fn into_raw_input_stream(mut self) -> (InputStream, IncomingBody) {
        match self.kind.take() {
            Some(Kind::Reader(_, _)) => panic!("Body is not backed up by an input stream"),
            Some(Kind::Bytes(_)) => panic!("Body is not backed up by an input stream"),
            Some(Kind::Incoming {
                stream,
                incoming_body,
            }) => (stream, incoming_body),
            #[cfg(feature = "async")]
            Some(Kind::Stream(_)) => panic!("Body is not backed up by an input stream"),
            None => panic!("Body has already been extracted"),
        }
    }

    pub(crate) fn into_reader(mut self) -> Reader {
        match self.kind.take() {
            Some(Kind::Reader(r, _)) => Reader::Reader(r),
            Some(Kind::Bytes(b)) => Reader::Bytes(Cursor::new(b)),
            Some(Kind::Incoming {
                stream,
                incoming_body,
            }) => Reader::Wasi {
                body_stream: stream,
                _incoming_body: incoming_body,
            },
            #[cfg(feature = "async")]
            Some(Kind::Stream(_)) => {
                panic!("Stream cannot be converted to Reader, use into_async_reader instead")
            }
            None => panic!("Body has already been extracted"),
        }
    }

    #[cfg(feature = "async")]
    pub(crate) fn into_async_reader(mut self) -> AsyncReader {
        if matches!(self.kind, Some(Kind::Stream(_))) {
            match self.kind.take() {
                Some(Kind::Stream(stream)) => AsyncReader::StreamBased { stream },
                _ => unreachable!(),
            }
        } else {
            self.into_reader().into_async()
        }
    }

    pub(crate) fn try_clone(&self) -> Option<Body> {
        self.kind
            .as_ref()
            .unwrap()
            .try_clone()
            .map(|kind| Body { kind: Some(kind) })
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> Option<u64> {
        match self.kind.as_ref()? {
            Kind::Reader(_, len) => *len,
            Kind::Bytes(bytes) => Some(bytes.len() as u64),
            Kind::Incoming { .. } => None,
            #[cfg(feature = "async")]
            Kind::Stream(_) => None,
        }
    }

    #[cfg(not(feature = "async"))]
    pub(crate) fn write(
        mut self,
        mut f: impl FnMut(&[u8]) -> Result<(), Error>,
    ) -> Result<(), Error> {
        match self.kind.take().expect("Body has already been extracted") {
            Kind::Reader(mut reader, _) => {
                let mut buf = [0; 4 * 1024];
                loop {
                    let len = reader.read(&mut buf).map_err(crate::error::builder)?;
                    if len == 0 {
                        break;
                    }
                    f(&buf[..len])?;
                }
                Ok(())
            }
            Kind::Bytes(bytes) => ChunkIterator::new(&bytes, 4 * 1024).try_for_each(&mut f),
            Kind::Incoming { ref stream, .. } => {
                let mut eof = false;
                while !eof {
                    match stream.blocking_read(u64::MAX) {
                        Ok(body_chunk) => {
                            f(&body_chunk)?;
                        }
                        Err(streams::StreamError::Closed) => {
                            eof = true;
                        }
                        Err(streams::StreamError::LastOperationFailed(err)) => {
                            return Err(Error::new(
                                crate::error::Kind::Body,
                                Some(err.to_debug_string()),
                            ));
                        }
                    }
                }
                Ok(())
            }
        }
    }

    #[cfg(feature = "async")]
    pub(crate) async fn async_write(
        self,
        mut target: impl crate::wasi::async_client::AsyncWriteTarget,
    ) -> Result<(), Error> {
        use async_iterator::Iterator;

        let mut async_reader = self.into_async_reader();
        while let Some(chunk) = async_reader.next().await {
            match chunk {
                Ok(data) => target.write(&data).await?,
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }
}

enum Kind {
    Reader(Box<dyn Read>, Option<u64>),
    Bytes(Bytes),
    Incoming {
        stream: InputStream,
        incoming_body: IncomingBody,
    },
    #[cfg(feature = "async")]
    Stream(std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Vec<u8>, Error>>>>),
}

impl Kind {
    fn try_clone(&self) -> Option<Kind> {
        match self {
            Kind::Reader(..) => None,
            Kind::Bytes(v) => Some(Kind::Bytes(v.clone())),
            Kind::Incoming { .. } => None,
            #[cfg(feature = "async")]
            Kind::Stream(_) => None,
        }
    }
}

impl From<Vec<u8>> for Body {
    #[inline]
    fn from(v: Vec<u8>) -> Body {
        Body {
            kind: Some(Kind::Bytes(v.into())),
        }
    }
}

impl From<String> for Body {
    #[inline]
    fn from(s: String) -> Body {
        s.into_bytes().into()
    }
}

impl From<&'static [u8]> for Body {
    #[inline]
    fn from(s: &'static [u8]) -> Body {
        Body {
            kind: Some(Kind::Bytes(Bytes::from_static(s))),
        }
    }
}

impl From<&'static str> for Body {
    #[inline]
    fn from(s: &'static str) -> Body {
        s.as_bytes().into()
    }
}

impl From<File> for Body {
    #[inline]
    fn from(f: File) -> Body {
        let len = f.metadata().map(|m| m.len()).ok();
        Body {
            kind: Some(Kind::Reader(Box::new(f), len)),
        }
    }
}

impl From<Bytes> for Body {
    #[inline]
    fn from(b: Bytes) -> Body {
        Body {
            kind: Some(Kind::Bytes(b)),
        }
    }
}

impl fmt::Debug for Kind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Kind::Reader(_, ref v) => f
                .debug_struct("Reader")
                .field("length", &DebugLength(v))
                .finish(),
            Kind::Bytes(ref v) => fmt::Debug::fmt(v, f),
            Kind::Incoming { .. } => f.debug_struct("Incoming").finish(),
            #[cfg(feature = "async")]
            Kind::Stream(_) => f.debug_struct("Stream").finish(),
        }
    }
}

struct DebugLength<'a>(&'a Option<u64>);

impl<'a> fmt::Debug for DebugLength<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self.0 {
            Some(ref len) => fmt::Debug::fmt(len, f),
            None => f.write_str("Unknown"),
        }
    }
}

pub(crate) enum Reader {
    Reader(Box<dyn Read>),
    Bytes(Cursor<Bytes>),
    Wasi {
        body_stream: InputStream,
        _incoming_body: IncomingBody,
    },
}

impl Reader {
    #[cfg(feature = "async")]
    pub(crate) fn into_async(self) -> AsyncReader {
        AsyncReader::ReaderBased { reader: self }
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Reader::Reader(rdr) => rdr.read(buf),
            Reader::Bytes(rdr) => rdr.read(buf),
            Reader::Wasi { body_stream, .. } => match body_stream.blocking_read(buf.len() as u64) {
                Ok(body_chunk) => {
                    let len = body_chunk.len();
                    buf[..len].copy_from_slice(&body_chunk);
                    Ok(len)
                }
                Err(streams::StreamError::Closed) => Ok(0),
                Err(streams::StreamError::LastOperationFailed(err)) => {
                    Err(io::Error::new(io::ErrorKind::Other, err.to_debug_string()))
                }
            },
        }
    }
}

#[cfg(feature = "async")]
pub(crate) enum AsyncReader {
    ReaderBased {
        reader: Reader,
    },
    StreamBased {
        stream: std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Vec<u8>, Error>>>>,
    },
}

#[cfg(feature = "async")]
impl async_iterator::Iterator for AsyncReader {
    type Item = Result<Vec<u8>, Error>;

    async fn next(&mut self) -> Option<Self::Item> {
        use futures::StreamExt;

        const CHUNK_SIZE: usize = 4096; // Define a constant chunk size

        match self {
            Self::ReaderBased { reader } => match reader {
                Reader::Reader(rdr) => {
                    let mut buf = vec![0; CHUNK_SIZE];
                    rdr.read(&mut buf)
                        .map(|n| {
                            if n == 0 {
                                None
                            } else {
                                Some(buf[..n].to_vec())
                            }
                        })
                        .map_err(|err| Error::new(crate::error::Kind::Body, Some(err)))
                        .transpose()
                }
                Reader::Bytes(rdr) => {
                    let mut buf = vec![0; CHUNK_SIZE];
                    rdr.read(&mut buf)
                        .map(|n| {
                            if n == 0 {
                                None
                            } else {
                                Some(buf[..n].to_vec())
                            }
                        })
                        .map_err(|err| Error::new(crate::error::Kind::Body, Some(err)))
                        .transpose()
                }
                Reader::Wasi { body_stream, .. } => {
                    let pollable = body_stream.subscribe();
                    wstd::runtime::AsyncPollable::new(pollable).wait_for().await;

                    let mut buf = vec![0; CHUNK_SIZE];
                    let result = body_stream.read(&mut buf);
                    match result {
                        Ok(n) => {
                            if n == 0 {
                                None
                            } else {
                                Some(Ok(buf))
                            }
                        }
                        Err(err) => Some(Err(Error::new(
                            crate::error::Kind::Body,
                            Some(err.to_string()),
                        ))),
                    }
                }
            },
            AsyncReader::StreamBased { stream } => stream.next().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_iterator_regular_slices() {
        let data = b"hello world".to_vec();
        let mut chunk_iter = ChunkIterator::new(&data, 5);

        assert_eq!(chunk_iter.next(), Some(&b"hello"[..]));
        assert_eq!(chunk_iter.next(), Some(&b" worl"[..]));
        assert_eq!(chunk_iter.next(), Some(&b"d"[..]));
        assert_eq!(chunk_iter.next(), None);
    }

    #[test]
    fn test_chunk_iterator_only_one_chunk() {
        let data = b"hello world".to_vec();
        let mut chunk_iter = ChunkIterator::new(&data, 11);

        assert_eq!(chunk_iter.next(), Some(&b"hello world"[..]));
        assert_eq!(chunk_iter.next(), None);
    }

    #[test]
    fn test_chunk_iterator_single_byte() {
        let data = b"x".to_vec();
        let mut chunk_iter = ChunkIterator::new(&data, 2);

        assert_eq!(chunk_iter.next(), Some(&b"x"[..]));
        assert_eq!(chunk_iter.next(), None);
    }

    #[test]
    fn test_chunk_iterator_empty_slice() {
        let data: Vec<u8> = vec![];
        let mut chunk_iter = ChunkIterator::new(&data, 5);

        assert_eq!(chunk_iter.next(), None);
    }
}
