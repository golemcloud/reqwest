use crate::Body;
use bytes::Bytes;
use encoding_rs::{Encoding, UTF_8};
use http::header::CONTENT_LENGTH;
use http::{Extensions, HeaderMap, StatusCode, Version};
use mime::Mime;
#[cfg(feature = "json")]
use serde::de::DeserializeOwned;
#[cfg(feature = "json")]
use serde_json;
use std::borrow::Cow;
use std::io;
use std::io::Read;
use std::net::SocketAddr;
use url::Url;

use wasi::http::types::IncomingBody;
use wasi::http::*;
use wasi::io::streams::InputStream;

/// A Response to a submitted `Request`.
#[derive(Debug)]
pub struct Response {
    status: StatusCode,
    headers: HeaderMap,
    body: Option<Body>,
    #[allow(dead_code)]
    incoming_response: types::IncomingResponse,
    url: Url,
    extensions: Extensions,
}

impl Response {
    pub(crate) fn new(
        status: StatusCode,
        headers: HeaderMap,
        body: Body,
        incoming_response: types::IncomingResponse,
        url: Url,
    ) -> Response {
        Response {
            status,
            headers,
            body: Some(body),
            incoming_response,
            url,
            extensions: Extensions::default(),
        }
    }

    /// Get the `StatusCode` of this `Response`.
    ///
    /// # Examples
    ///
    /// Checking for general status class:
    ///
    /// ```rust
    /// # #[cfg(feature = "json")]
    /// # fn run() -> Result<(), Box<std::error::Error>> {
    /// let resp = reqwest::blocking::get("http://httpbin.org/get")?;
    /// if resp.status().is_success() {
    ///     println!("success!");
    /// } else if resp.status().is_server_error() {
    ///     println!("server error!");
    /// } else {
    ///     println!("Something else happened. Status: {:?}", resp.status());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Get the `Headers` of this `Response`.
    #[inline]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Get a mutable reference to the `Headers` of this `Response`.
    #[inline]
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    /// Get the HTTP `Version` of this `Response`.
    #[inline]
    pub fn version(&self) -> Version {
        Version::HTTP_11
    }

    /// Get the final `Url` of this `Response`.
    #[inline]
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Get the remote address used to get this `Response`.
    pub fn remote_addr(&self) -> Option<SocketAddr> {
        None
    }

    /// Returns a reference to the associated extensions.
    pub fn extensions(&self) -> &http::Extensions {
        &self.extensions
    }

    /// Returns a mutable reference to the associated extensions.
    pub fn extensions_mut(&mut self) -> &mut http::Extensions {
        &mut self.extensions
    }

    /// Get the content-length of the response, if it is known.
    ///
    /// Reasons it may not be known:
    ///
    /// - The server didn't send a `content-length` header.
    /// - The response is gzipped and automatically decoded (thus changing
    ///   the actual decoded length).
    pub fn content_length(&self) -> Option<u64> {
        self.headers
            .get(CONTENT_LENGTH)
            .and_then(|ct_len| ct_len.to_str().ok())
            .and_then(|ct_len| ct_len.parse().ok())
    }

    /// Try and deserialize the response body as JSON using `serde`.
    ///
    /// # Optional
    ///
    /// This requires the optional `json` feature enabled.
    /// # Errors
    ///
    /// This method fails whenever the response body is not in JSON format
    /// or it cannot be properly deserialized to target type `T`. For more
    /// details please see [`serde_json::from_reader`].
    ///
    /// [`serde_json::from_reader`]: https://docs.serde.rs/serde_json/fn.from_reader.html
    #[cfg(feature = "json")]
    #[cfg(not(feature = "async"))]
    #[cfg_attr(docsrs, doc(cfg(feature = "json")))]
    pub fn json<T: DeserializeOwned>(self) -> crate::Result<T> {
        let full = self.bytes()?;
        serde_json::from_slice(&full).map_err(crate::error::decode)
    }

    /// Try and deserialize the response body as JSON using `serde`.
    ///
    /// # Optional
    ///
    /// This requires the optional `json` feature enabled.
    /// # Errors
    ///
    /// This method fails whenever the response body is not in JSON format
    /// or it cannot be properly deserialized to target type `T`. For more
    /// details please see [`serde_json::from_reader`].
    ///
    /// [`serde_json::from_reader`]: https://docs.serde.rs/serde_json/fn.from_reader.html
    #[cfg(feature = "json")]
    #[cfg(feature = "async")]
    #[cfg_attr(docsrs, doc(cfg(feature = "json")))]
    pub async fn json<T: DeserializeOwned>(self) -> crate::Result<T> {
        let full = self.bytes().await?;
        serde_json::from_slice(&full).map_err(crate::error::decode)
    }

    /// Get the full response body as `Bytes`.
    #[cfg(not(feature = "async"))]
    pub fn bytes(mut self) -> crate::Result<Bytes> {
        let bytes: Bytes = Bytes::copy_from_slice(self.body.take().unwrap().buffer()?);
        Ok(bytes)
    }

    /// Get the full response body as `Bytes`.
    #[cfg(feature = "async")]
    pub async fn bytes(mut self) -> crate::Result<Bytes> {
        let bytes: Bytes = Bytes::copy_from_slice(self.body.take().unwrap().buffer().await?);
        Ok(bytes)
    }

    /// Gets an async iterator yielding the response body in chunks.
    #[cfg(feature = "async")]
    pub fn async_chunks(
        mut self,
    ) -> impl async_iterator::Iterator<Item = Result<Vec<u8>, crate::Error>> {
        let sync_reader = self.body.take().unwrap().into_reader();
        let async_reader = sync_reader.into_async();
        async_reader
    }

    /// Gets the underlying WASI response body stream. If the body was already consumed and/or buffered,
    /// it fails with a panic.
    pub fn get_raw_input_stream(&mut self) -> (InputStream, IncomingBody) {
        let body = self.body.take().unwrap();
        body.into_raw_input_stream()
    }

    /// Get the response text.
    ///
    /// This method decodes the response body with BOM sniffing
    /// and with malformed sequences replaced with the REPLACEMENT CHARACTER.
    /// Encoding is determined from the `charset` parameter of `Content-Type` header,
    /// and defaults to `utf-8` if not presented.
    #[cfg(not(feature = "async"))]
    pub fn text(self) -> crate::Result<String> {
        self.text_with_charset("utf-8")
    }

    /// Get the response text.
    ///
    /// This method decodes the response body with BOM sniffing
    /// and with malformed sequences replaced with the REPLACEMENT CHARACTER.
    /// Encoding is determined from the `charset` parameter of `Content-Type` header,
    /// and defaults to `utf-8` if not presented.
    #[cfg(feature = "async")]
    pub async fn text(self) -> crate::Result<String> {
        self.text_with_charset("utf-8").await
    }

    /// Get the response text given a specific encoding.
    ///
    /// This method decodes the response body with BOM sniffing
    /// and with malformed sequences replaced with the REPLACEMENT CHARACTER.
    /// You can provide a default encoding for decoding the raw message, while the
    /// `charset` parameter of `Content-Type` header is still prioritized. For more information
    /// about the possible encoding name, please go to [`encoding_rs`] docs.
    ///
    /// [`encoding_rs`]: https://docs.rs/encoding_rs/0.8/encoding_rs/#relationship-with-windows-code-pages
    #[cfg(not(feature = "async"))]
    pub fn text_with_charset(self, default_encoding: &str) -> crate::Result<String> {
        let content_type = self
            .headers()
            .get(crate::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<Mime>().ok());
        let encoding_name = content_type
            .as_ref()
            .and_then(|mime| mime.get_param("charset").map(|charset| charset.as_str()))
            .unwrap_or(default_encoding);
        let encoding = Encoding::for_label(encoding_name.as_bytes()).unwrap_or(UTF_8);

        let full = self.bytes()?;

        let (text, _, _) = encoding.decode(&full);
        if let Cow::Owned(s) = text {
            return Ok(s);
        }
        unsafe {
            // decoding returned Cow::Borrowed, meaning these bytes
            // are already valid utf8
            Ok(String::from_utf8_unchecked(full.to_vec()))
        }
    }

    /// Get the response text given a specific encoding.
    ///
    /// This method decodes the response body with BOM sniffing
    /// and with malformed sequences replaced with the REPLACEMENT CHARACTER.
    /// You can provide a default encoding for decoding the raw message, while the
    /// `charset` parameter of `Content-Type` header is still prioritized. For more information
    /// about the possible encoding name, please go to [`encoding_rs`] docs.
    ///
    /// [`encoding_rs`]: https://docs.rs/encoding_rs/0.8/encoding_rs/#relationship-with-windows-code-pages
    #[cfg(feature = "async")]
    pub async fn text_with_charset(self, default_encoding: &str) -> crate::Result<String> {
        let content_type = self
            .headers()
            .get(crate::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<Mime>().ok());
        let encoding_name = content_type
            .as_ref()
            .and_then(|mime| mime.get_param("charset").map(|charset| charset.as_str()))
            .unwrap_or(default_encoding);
        let encoding = Encoding::for_label(encoding_name.as_bytes()).unwrap_or(UTF_8);

        let full = self.bytes().await?;

        let (text, _, _) = encoding.decode(&full);
        if let Cow::Owned(s) = text {
            return Ok(s);
        }
        unsafe {
            // decoding returned Cow::Borrowed, meaning these bytes
            // are already valid utf8
            Ok(String::from_utf8_unchecked(full.to_vec()))
        }
    }

    /// Copy the response body into a writer.
    ///
    /// This function internally uses [`std::io::copy`] and hence will continuously read data from
    /// the body and then write it into writer in a streaming fashion until EOF is met.
    ///
    /// On success, the total number of bytes that were copied to `writer` is returned.
    ///
    /// [`std::io::copy`]: https://doc.rust-lang.org/std/io/fn.copy.html
    pub fn copy_to<W: ?Sized>(&mut self, w: &mut W) -> crate::Result<u64>
    where
        W: io::Write,
    {
        io::copy(self, w).map_err(crate::error::decode_io)
    }

    /// Turn a response into an error if the server returned an error.
    pub fn error_for_status(self) -> crate::Result<Self> {
        let status = self.status();
        if status.is_client_error() || status.is_server_error() {
            Err(crate::error::status_code(self.url.clone(), status))
        } else {
            Ok(self)
        }
    }

    /// Turn a reference to a response into an error if the server returned an error.
    pub fn error_for_status_ref(&self) -> crate::Result<&Self> {
        let status = self.status();
        if status.is_client_error() || status.is_server_error() {
            Err(crate::error::status_code(self.url.clone(), status))
        } else {
            Ok(self)
        }
    }
}

impl Read for Response {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.body.take().unwrap().into_reader().read(buf)
    }
}
