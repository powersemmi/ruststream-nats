//! `NATS` / `JetStream` broker implementation for `RustStream`.
//!
//! The lifecycle is the framework's ladder of consuming transitions: [`NatsBroker::new`] captures
//! the address synchronously, [`Broker::connect`](ruststream::Broker::connect) dials and yields a
//! [`ConnectedNatsBroker`], and
//! [`ConnectedBroker::shutdown`](ruststream::ConnectedBroker::shutdown) drains it into a
//! [`ClosedNatsBroker`]. Subscriptions and publishers exist only from the connected form.
//!
//! Publishing splits by transport rather than by flag: [`NatsPublish`] pairs into the Core NATS
//! [`NatsPublisher`] (fire-and-forget, plus request/reply), and [`JetStreamPublish`] pairs into
//! the [`JetStreamPublisher`], which awaits the stream's acknowledgement and can state what it
//! expects the stream to look like.

#![forbid(unsafe_code)]

mod broker;
mod convert;
mod error;
mod jetstream;
mod message;
mod publisher;
mod request_reply;
mod subscribe_options;
mod subscriber;

pub mod context;

pub use broker::{ClosedNatsBroker, ConnectedNatsBroker, NatsBroker};
pub use error::NatsError;
pub use jetstream::{JetStreamPublish, JetStreamPublisher, PublishAck};
pub use message::{CoreMessage, JetStreamMessage, NatsMessage, PARTITION_KEY_HEADER};
pub use publisher::{NatsPublish, NatsPublishPolicy, NatsPublisher};
pub use subscribe_options::{DeliverPolicy, SubscribeOptions};
pub use subscriber::NatsSubscriber;

#[cfg(feature = "testing")]
pub mod testing;
