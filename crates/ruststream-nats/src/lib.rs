#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

mod broker;
mod convert;
mod error;
mod jetstream;
mod message;
mod publisher;
mod request_reply;
mod subject;
mod subscriber;

pub mod context;
pub mod prelude;

pub use broker::{ClosedNatsBroker, ConnectedNatsBroker, NatsBroker};
pub use error::NatsError;
pub use jetstream::{
    JetStreamOptions, JetStreamPublish, JetStreamPublishSteps, JetStreamPublisher, PublishAck,
};
pub use message::{CoreMessage, JetStreamMessage, NatsMessage, PARTITION_KEY_HEADER};
pub use publisher::{NatsPublish, NatsPublishPolicy, NatsPublisher};
pub use subject::{
    CoreSubject, CoreWildcard, DeliverPolicy, JetStreamSubject, NatsSubscription, NonZeroDuration,
};
pub use subscriber::NatsSubscriber;

#[cfg(feature = "testing")]
pub mod testing;
