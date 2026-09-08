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
//! * [`NatsTestPublisher`] / [`JetStreamTestPublisher`] - the live publishers the crate's
//!   production policies pair into here. There is no policy of the test transport's own: a routes
//!   file names [`NatsPublish`](crate::NatsPublish) or
//!   [`JetStreamPublish`](crate::JetStreamPublish) once and mounts on either ladder unchanged,
//!   and each live form carries exactly the capabilities its production counterpart does
//!   ([`Publisher`](ruststream::Publisher) + [`RequestReply`](ruststream::RequestReply) for Core,
//!   `Publisher` alone for `JetStream`);
//! * [`NatsTestSubscriber`] / [`NatsTestMessage`] - `Subscriber` and `IncomingMessage` impls
//!   with `nack(requeue=true)` redelivery (re-sent into the same subscriber's queue) and its
//!   delayed form, whose timer the harness drives.
//!
//! No `nats-server`, no docker, no network. Broker-specific edge cases (`JetStream` durable
//! cursor, `ack_wait` redelivery, `max_ack_pending`, retention) are out of scope here.
//! Exercise them against a real NATS server. On the publish side that exclusion is the
//! `JetStream` stream itself: the publish acknowledgement and the expectations
//! [`JetStreamPublish`](crate::JetStreamPublish) declares are server-side checks with no stream
//! in process to check them against, so a publish that violates one succeeds here where a server
//! would refuse it. Assert that against a real server, as
//! `the_stream_checks_the_expectations_the_publish_policy_declares` in
//! `tests/integration_nats.rs` does. [`JetStreamTestPublisher`] repeats the list on the type.

mod broker;
mod publisher;
mod router;
mod subject;
mod subscriber;

pub use broker::{ConnectedNatsTestBroker, NatsTestBroker};
pub use publisher::{JetStreamTestPublisher, NatsTestPublishPolicy, NatsTestPublisher};
pub use subscriber::{NatsTestMessage, NatsTestSubscriber};
