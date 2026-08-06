//! In-process NATS test transport used by handler integration tests and the conformance suite.
//!
//! Gated by the `testing` cargo feature. The broker follows the same ladder as the real one
//! (synchronous `new`, consuming `connect`, consuming `shutdown`) over a synchronous dispatcher:
//! `publish` fans the message out to every subscriber whose subject pattern matches. Public
//! surface:
//!
//! * [`NatsTestBroker`] / [`ConnectedNatsTestBroker`] - the ladder; the connected form implements
//!   [`TestableBroker`](ruststream::testing::TestableBroker), so it drives both the
//!   [`TestApp`](ruststream::testing::TestApp) harness and
//!   the framework's conformance suite in process;
//! * [`NatsTestPublish`] / [`NatsTestPublisher`] - the publish pair, `Publisher` + `RequestReply`;
//! * [`NatsTestSubscriber`] / [`NatsTestMessage`] - `Subscriber` and `IncomingMessage` impls
//!   with `nack(requeue=true)` redelivery (re-sent into the same subscriber's queue).
//!
//! No `nats-server`, no docker, no network. Broker-specific edge cases (`JetStream` durable
//! cursor, `ack_wait` redelivery, `max_ack_pending`, retention) are out of scope here.
//! Exercise them against a real NATS server.

mod broker;
mod publisher;
mod router;
mod subject;
mod subscriber;

pub use broker::{ConnectedNatsTestBroker, NatsTestBroker};
pub use publisher::{NatsTestPublish, NatsTestPublisher};
pub use subscriber::{NatsTestMessage, NatsTestSubscriber};
