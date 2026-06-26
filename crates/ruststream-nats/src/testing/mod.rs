//! In-process NATS test transport used by handler integration tests and the conformance suite.
//!
//! Gated by the `testing` cargo feature. The broker is a synchronous dispatcher: `publish`
//! fans the message out to every subscriber whose subject pattern matches. Public surface:
//!
//! * [`NatsTestBroker`] - `Broker` + `Subscribe` + `DescribeServer` impl backed by an in-process
//!   subject router; it implements
//!   [`TestableBroker`](ruststream::testing::TestableBroker) directly, so it drives both the
//!   [`TestApp`](ruststream::testing::TestApp) harness and
//!   [`conformance::harness::run_suite`](ruststream::conformance::harness::run_suite) in process;
//! * [`NatsTestPublisher`] - `Publisher` + `RequestReply`;
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

pub use broker::NatsTestBroker;
pub use publisher::NatsTestPublisher;
pub use subscriber::{NatsTestMessage, NatsTestSubscriber};
