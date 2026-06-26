//! `NATS` / `JetStream` broker implementation for `RustStream`.

#![forbid(unsafe_code)]

mod broker;
mod convert;
mod error;
mod message;
mod publisher;
mod request_reply;
mod subscribe_options;
mod subscriber;

pub mod context;

pub use broker::NatsBroker;
pub use error::NatsError;
pub use message::{CoreMessage, JetStreamMessage, NatsMessage, PARTITION_KEY_HEADER};
pub use publisher::NatsPublisher;
pub use subscribe_options::{DeliverPolicy, SubscribeOptions};
pub use subscriber::NatsSubscriber;

#[cfg(feature = "testing")]
pub mod testing;
