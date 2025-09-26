mod body;
pub(crate) mod conversions;
mod request;
mod response;

#[cfg(feature = "async")]
mod async_client;
#[cfg(not(feature = "async"))]
mod sync_client;

mod client {
    #[cfg(feature = "async")]
    pub use crate::wasi::async_client::*;
    #[cfg(not(feature = "async"))]
    pub use crate::wasi::sync_client::*;
}

#[cfg(feature = "multipart")]
pub mod multipart;
pub use body::Body;
pub use client::{Client, ClientBuilder};
pub use request::{Request, RequestBuilder};
pub use response::Response;

#[cfg(feature = "async")]
pub use client::{CustomRequestBodyWriter, CustomRequestExecution};
