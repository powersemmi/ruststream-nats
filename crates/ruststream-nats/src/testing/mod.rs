//! In-process NATS test driver used by handler integration tests.
//!
//! Gated by the `testing` cargo feature. The broker is a synchronous dispatcher: `publish`
//! fans the message out to every subscriber whose subject pattern matches. Public surface:
//!
//! * [`NatsTestBroker`] - `Broker` impl backed by an in-process subject router;
//! * [`NatsTestPublisher`] - `Publisher` + `RequestReply`;
//! * [`NatsTestSubscriber`] / [`NatsTestMessage`] - `Subscriber` and `IncomingMessage` impls
//!   with `nack(requeue=true)` redelivery (re-sent into the same subscriber's queue);
//! * [`NatsTestClient`] - `TestClient` driver consumed by the conformance harness.
//!
//! No `nats-server`, no docker, no network. Broker-specific edge cases (`JetStream` durable
//! cursor, `ack_wait` redelivery, `max_ack_pending`, retention) are out of scope here.
//! Exercise them against a real NATS server.

mod broker;
mod client;
mod publisher;
mod router;
mod subject;
mod subscriber;

pub use broker::NatsTestBroker;
pub use client::NatsTestClient;
pub use publisher::NatsTestPublisher;
pub use subscriber::{NatsTestMessage, NatsTestSubscriber};
