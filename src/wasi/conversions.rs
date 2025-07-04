use crate::error::Kind;
use http::Method;
use std::io::ErrorKind;
use wasi::http::types;
use wasi::io::streams;

pub(crate) fn encode_method(method: Method) -> types::Method {
    match method {
        Method::GET => types::Method::Get,
        Method::POST => types::Method::Post,
        Method::PUT => types::Method::Put,
        Method::DELETE => types::Method::Delete,
        Method::HEAD => types::Method::Head,
        Method::OPTIONS => types::Method::Options,
        Method::CONNECT => types::Method::Connect,
        Method::PATCH => types::Method::Patch,
        Method::TRACE => types::Method::Trace,
        other => types::Method::Other(other.to_string()),
    }
}

impl From<types::ErrorCode> for crate::Error {
    fn from(value: types::ErrorCode) -> Self {
        crate::Error::new(
            Kind::Request,
            Some(std::io::Error::new(
                ErrorKind::Other,
                format!("{:?}", value),
            )),
        )
    }
}

impl From<streams::StreamError> for crate::Error {
    fn from(value: streams::StreamError) -> Self {
        crate::Error::new(
            Kind::Request,
            Some(std::io::Error::new(
                ErrorKind::Other,
                format!("{:?}", value),
            )),
        )
    }
}

impl From<types::HeaderError> for crate::Error {
    fn from(value: types::HeaderError) -> Self {
        crate::Error::new(
            Kind::Request,
            Some(std::io::Error::new(
                ErrorKind::Other,
                format!("{:?}", value),
            )),
        )
    }
}

pub(crate) fn failure_point(s: &str, _: ()) -> crate::Error {
    crate::Error::new(
        Kind::Request,
        Some(std::io::Error::new(ErrorKind::Other, s)),
    )
}
