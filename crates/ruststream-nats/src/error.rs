//! Error type returned by NATS broker operations.

use std::error::Error as StdError;

use thiserror::Error;

/// Errors surfaced by the NATS broker implementation.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum NatsError {
    /// Failed to establish or use the underlying `async-nats` connection.
    #[error("nats connection error: {0}")]
    Connect(#[source] Box<dyn StdError + Send + Sync>),

    /// Failed to publish a message to the broker.
    #[error("nats publish error: {0}")]
    Publish(#[source] Box<dyn StdError + Send + Sync>),

    /// Failed to subscribe to the requested subject.
    #[error("nats subscribe error: {0}")]
    Subscribe(#[source] Box<dyn StdError + Send + Sync>),

    /// JetStream-specific operation failed (consumer creation, ack, etc.).
    #[error("nats jetstream error: {0}")]
    JetStream(#[source] Box<dyn StdError + Send + Sync>),

    /// A request / reply operation timed out before a reply was received.
    #[error("nats request timed out")]
    RequestTimeout,

    /// The supplied [`crate::SubscribeOptions`] combine fields in a way the broker cannot honour
    /// (for example `durable(_)` without `jetstream(_)`, or `queue_group(_)` together with
    /// `jetstream(_)`).
    #[error("invalid subscribe options: {0}")]
    InvalidOptions(String),
}
