use super::conversions::{encode_method, failure_point};
use super::request::{Request, RequestBuilder};
use super::response::Response;
use crate::error::Kind;
use crate::{Body, Error, IntoUrl};
use futures_concurrency::future::Join;
use futures_concurrency::future::Race;
use http::header::{HeaderMap, HeaderValue, ACCEPT, USER_AGENT};
use http::{HeaderName, Method, StatusCode};
use std::cmp::min;
use std::convert::{TryFrom, TryInto};
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use wasi_async_runtime::Reactor;

use wasi::clocks::*;
use wasi::http::*;
use wasi::io::*;

#[derive(Debug)]
struct Config {
    headers: HeaderMap,
    connect_timeout: Option<Duration>,
    timeout: Option<Duration>,
    error: Option<crate::Error>,
}

/// A `Client` to make Requests with.
///
/// The Client has various configuration values to tweak, but the defaults
/// are set to what is usually the most commonly desired value. To configure a
/// `Client`, use `Client::builder()`.
///
/// The `Client` holds a connection pool internally, so it is advised that
/// you create one and **reuse** it.

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientRef>,
}

#[derive(Debug)]
struct ClientRef {
    reactor: Reactor,
    pub headers: HeaderMap,
    pub connect_timeout: Option<Duration>,
    pub first_byte_timeout: Option<Duration>,
    pub between_bytes_timeout: Option<Duration>,
}

/// A `ClientBuilder` can be used to create a `Client` with  custom configuration.
#[must_use]
#[derive(Debug)]
pub struct ClientBuilder {
    config: Config,
    reactor: Reactor,
}

impl Client {
    /// Constructs a new `Client`.
    pub fn new(reactor: Reactor) -> Self {
        Client::builder(reactor).build().expect("Client::new()")
    }

    /// Creates a `ClientBuilder` to configure a `Client`.
    ///
    /// This is the same as `ClientBuilder::new()`.
    pub fn builder(reactor: Reactor) -> ClientBuilder {
        ClientBuilder::new(reactor)
    }

    /// Convenience method to make a `GET` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever supplied `Url` cannot be parsed.
    pub fn get<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.request(Method::GET, url)
    }

    /// Convenience method to make a `POST` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever supplied `Url` cannot be parsed.
    pub fn post<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.request(Method::POST, url)
    }

    /// Convenience method to make a `PUT` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever supplied `Url` cannot be parsed.
    pub fn put<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.request(Method::PUT, url)
    }

    /// Convenience method to make a `PATCH` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever supplied `Url` cannot be parsed.
    pub fn patch<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.request(Method::PATCH, url)
    }

    /// Convenience method to make a `DELETE` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever supplied `Url` cannot be parsed.
    pub fn delete<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.request(Method::DELETE, url)
    }

    /// Convenience method to make a `HEAD` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever supplied `Url` cannot be parsed.
    pub fn head<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.request(Method::HEAD, url)
    }

    /// Start building a `Request` with the `Method` and `Url`.
    ///
    /// Returns a `RequestBuilder`, which will allow setting headers and
    /// request body before sending.
    ///
    /// # Errors
    ///
    /// This method fails whenever supplied `Url` cannot be parsed.
    pub fn request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        let req = url.into_url().map(move |url| Request::new(method, url));
        RequestBuilder::new(self.clone(), req)
    }

    /// Executes a `Request`.
    ///
    /// A `Request` can be built manually with `Request::new()` or obtained
    /// from a RequestBuilder with `RequestBuilder::build()`.
    ///
    /// You should prefer to use the `RequestBuilder` and
    /// `RequestBuilder::send()`.
    ///
    /// # Errors
    ///
    /// This method fails if there was an error while sending request,
    /// redirect loop was detected or redirect limit was exhausted.
    pub async fn execute(&self, request: Request) -> Result<Response, crate::Error> {
        let mut header_key_values: Vec<(String, Vec<u8>)> = vec![];
        for (name, value) in self.inner.headers.iter() {
            if let Ok(value) = value.to_str() {
                header_key_values.push((name.as_str().to_string(), value.into()))
            }
        }

        let (method, url, headers, body, timeout, _version) = request.pieces();
        for (name, value) in headers.iter() {
            if let Ok(value) = value.to_str() {
                header_key_values.push((name.as_str().to_string(), value.into()))
            }
        }

        let scheme = match url.scheme() {
            "http" => types::Scheme::Http,
            "https" => types::Scheme::Https,
            other => types::Scheme::Other(other.to_string()),
        };
        let headers = types::Fields::from_list(&header_key_values)?;
        let request = types::OutgoingRequest::new(headers);
        let path_with_query = match url.query() {
            Some(query) => format!("{}?{}", url.path(), query),
            None => url.path().to_string(),
        };
        request
            .set_method(&encode_method(method))
            .map_err(|e| failure_point("set_method", e))?;
        request
            .set_path_with_query(Some(&path_with_query))
            .map_err(|e| failure_point("set_path_with_query", e))?;
        request
            .set_scheme(Some(&scheme))
            .map_err(|e| failure_point("set_scheme", e))?;
        request
            .set_authority(Some(url.authority()))
            .map_err(|e| failure_point("set_authority", e))?;

        let options = types::RequestOptions::new();
        options
            .set_connect_timeout(self.inner.connect_timeout.map(|d| d.as_nanos() as u64))
            .map_err(|e| failure_point("set_connect_timeout", e))?;
        options
            .set_first_byte_timeout(
                timeout
                    .or(self.inner.first_byte_timeout)
                    .map(|d| d.as_nanos() as u64),
            )
            .map_err(|e| failure_point("set_first_byte_timeout", e))?;
        options
            .set_between_bytes_timeout(
                timeout
                    .or(self.inner.between_bytes_timeout)
                    .map(|d| d.as_nanos() as u64),
            )
            .map_err(|e| failure_point("set_between_bytes_timeout", e))?;

        let maybe_outgoing_body = if let Some(body) = body {
            let request_body = request.body().map_err(|e| failure_point("body", e))?;
            Some((body, request_body))
        } else {
            None
        };

        let future_incoming_response = outgoing_handler::handle(request, Some(options))?;

        let target = if let Some((body, outgoing_body)) = maybe_outgoing_body {
            let outgoing_body_stream = outgoing_body
                .write()
                .map_err(|e| failure_point("write", e))?;

            let target = OutputStreamWriter::new(outgoing_body_stream, self.inner.reactor.clone());
            Some((body, outgoing_body, target))
        } else {
            None
        };

        let request_body_future = async move {
            if let Some((body, outgoing_body, target)) = target {
                let write_result = body.async_write(self.inner.reactor.clone(), target).await;
                if let Ok(_) = write_result {
                    types::OutgoingBody::finish(outgoing_body, None)
                        .map_err(|error_code| error_code.into())
                } else {
                    write_result
                }
            } else {
                Ok(())
            }
        };

        let receive_timeout = timeout.or(self.inner.first_byte_timeout);
        let incoming_response_future =
            self.get_incoming_response(&future_incoming_response, receive_timeout);
        let (incoming_response, _) = (incoming_response_future, request_body_future).join().await;
        let incoming_response = incoming_response?;

        let status = incoming_response.status();
        let status_code =
            StatusCode::from_u16(status).map_err(|e| Error::new(Kind::Decode, Some(e)))?;

        let response_fields = incoming_response.headers();
        let response_headers = Self::fields_to_header_map(&response_fields);

        let response_body = incoming_response
            .consume()
            .map_err(|e| failure_point("consume", e))?;
        let response_body_stream = response_body
            .stream()
            .map_err(|e| failure_point("stream", e))?;
        let body: Body = Body::from_incoming(response_body_stream, response_body);

        Ok(Response::new(
            status_code,
            response_headers,
            body,
            incoming_response,
            url,
            self.inner.reactor.clone(),
        ))
    }

    async fn get_incoming_response(
        &self,
        future_incoming_response: &types::FutureIncomingResponse,
        timeout: Option<Duration>,
    ) -> Result<types::IncomingResponse, Error> {
        match future_incoming_response.get() {
            Some(Ok(Ok(incoming_response))) => Ok(incoming_response),
            Some(Ok(Err(err))) => Err(err.into()),
            Some(Err(err)) => Err(failure_point("get_incoming_response", err)),
            None => {
                use futures::FutureExt;

                let deadline_pollable = monotonic_clock::subscribe_duration(
                    timeout
                        .unwrap_or(Duration::from_secs(10000000000))
                        .as_nanos() as u64,
                );
                let pollable = future_incoming_response.subscribe();
                let wait_for_deadline =
                    self.inner.reactor.wait_for(deadline_pollable).map(|_| None);
                let wait_for_response = self
                    .inner
                    .reactor
                    .wait_for(pollable)
                    .map(|_| future_incoming_response.get());
                let result = (wait_for_deadline, wait_for_response).race().await;

                match result {
                    Some(Ok(Ok(incoming_response))) => Ok(incoming_response),
                    Some(Ok(Err(err))) => Err(err.into()),
                    Some(Err(err)) => Err(failure_point("get_incoming_response", err)),
                    None => {
                        // Timeout occurred
                        Err(Error::new(
                            Kind::Request,
                            Some(std::io::Error::new(
                                ErrorKind::TimedOut,
                                "Request timed out",
                            )),
                        ))
                    }
                }
            }
        }
    }

    fn fields_to_header_map(fields: &types::Fields) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let entries = fields.entries();
        for (name, value) in entries {
            headers.insert(
                HeaderName::try_from(&name).expect("Invalid header name"),
                HeaderValue::from_bytes(&value).expect("Invalid header value"),
            );
        }
        headers
    }
}

impl ClientBuilder {
    /// Constructs a new `ClientBuilder`.
    ///
    /// This is the same as `Client::builder()`.
    pub fn new(reactor: Reactor) -> Self {
        let mut headers: HeaderMap<HeaderValue> = HeaderMap::with_capacity(2);
        headers.insert(ACCEPT, HeaderValue::from_static("*/*"));

        Self {
            reactor,
            config: Config {
                headers,
                connect_timeout: None,
                timeout: None,
                error: None,
            },
        }
    }

    /// Returns a `Client` that uses this `ClientBuilder` configuration.
    ///
    /// # Errors
    ///
    /// This method fails if TLS backend cannot be initialized, or the resolver
    /// cannot load the system configuration.
    pub fn build(self) -> Result<Client, crate::Error> {
        if let Some(err) = self.config.error {
            return Err(err);
        }

        Ok(Client {
            inner: Arc::new(ClientRef {
                reactor: self.reactor,
                headers: self.config.headers,
                connect_timeout: self.config.connect_timeout,
                first_byte_timeout: self.config.timeout,
                between_bytes_timeout: self.config.timeout,
            }),
        })
    }

    /// Sets the `User-Agent` header to be used by this client.
    pub fn user_agent<V>(mut self, value: V) -> ClientBuilder
    where
        V: TryInto<HeaderValue>,
        V::Error: Into<http::Error>,
    {
        match value.try_into() {
            Ok(value) => {
                self.config.headers.insert(USER_AGENT, value);
            }
            Err(e) => {
                self.config.error = Some(crate::error::builder(e.into()));
            }
        };
        self
    }

    /// Sets the default headers for every request
    pub fn default_headers(mut self, headers: HeaderMap) -> ClientBuilder {
        for (key, value) in headers.iter() {
            self.config.headers.insert(key, value.clone());
        }
        self
    }

    // TODO: cookie support
    // TODO: gzip support
    // TODO: brotli support
    // TODO: deflate support
    // TODO: redirect support
    // TODO: proxy support
    // TODO: TLS support

    // Timeout options

    /// Set a timeout for connect, read and write operations of a `Client`.
    ///
    /// Default is 30 seconds.
    ///
    /// Pass `None` to disable timeout.
    pub fn timeout<T>(mut self, timeout: T) -> ClientBuilder
    where
        T: Into<Option<Duration>>,
    {
        self.config.timeout = timeout.into();
        self
    }

    /// Set a timeout for only the connect phase of a `Client`.
    ///
    /// Default is `None`.
    pub fn connect_timeout<T>(mut self, timeout: T) -> ClientBuilder
    where
        T: Into<Option<Duration>>,
    {
        self.config.connect_timeout = timeout.into();
        self
    }
}

pub(crate) trait AsyncWriteTarget {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), crate::Error>;
}

pub(crate) struct OutputStreamWriter {
    stream: wasi::io::streams::OutputStream,
    reactor: Reactor,
}

impl OutputStreamWriter {
    pub fn new(stream: wasi::io::streams::OutputStream, reactor: Reactor) -> Self {
        Self { stream, reactor }
    }
}

impl AsyncWriteTarget for OutputStreamWriter {
    async fn write(&mut self, mut chunk: &[u8]) -> Result<(), Error> {
        while !chunk.is_empty() {
            let ready = self.stream.subscribe();
            self.reactor.wait_for(ready).await;
            let max_length = self.stream.check_write()?;
            if max_length == 0 {
                return Err(Error::new(
                    Kind::Request,
                    Some(std::io::Error::new(
                        ErrorKind::WriteZero,
                        "Request body stream is closed (check_write returned 0)",
                    )),
                ));
            } else {
                let len = min(max_length as usize, chunk.len());
                let (to_write, remaining) = chunk.split_at(len);
                match self.stream.write(to_write) {
                    Ok(()) => {
                        chunk = remaining;
                    }
                    Err(streams::StreamError::Closed) => {
                        return Err(Error::new(
                            Kind::Request,
                            Some(std::io::Error::new(
                                ErrorKind::WriteZero,
                                "Request body stream is closed",
                            )),
                        ));
                    }
                    Err(streams::StreamError::LastOperationFailed(error)) => {
                        return Err(Error::new(
                            Kind::Request,
                            Some(std::io::Error::new(
                                ErrorKind::Other,
                                error.to_debug_string(),
                            )),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}
