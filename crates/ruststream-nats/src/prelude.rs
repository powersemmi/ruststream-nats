//! The imports a service on NATS writes every time, in one glob.
//!
//! `use ruststream_nats::prelude::*;` brings in the broker type, the subscription descriptor a
//! mount site fills in, both publish policies, and the framework capabilities NATS actually has,
//! on top of the whole core prelude. One import then serves a service file: the application
//! object, the handler surface and the attribute macros arrive with it.
//!
//! The capability re-exports make the glob a manifest of what this transport can do, and they
//! carry the ones a service writes for itself: a trait it names in a bound, and a trait whose
//! method it calls on a value the runtime hands it, which needs the trait in scope just the same.
//! A handler that bounds a slot with [`RequestReply`](ruststream::RequestReply) compiles here
//! because NATS correlates replies natively; on a broker whose prelude does not carry that name,
//! the bound does not even resolve, so the mismatch reads as the missing capability it is.
//! Globbing two broker preludes stays safe for the same reason it is useful: each re-exports the
//! same core trait, and the compiler accepts a name that resolves to one item however many paths
//! reach it.
//!
//! A capability whose method the core already surfaces elsewhere is the one exception, and it is
//! listed below: two paths to the same method name are what the compiler will not resolve.
//!
//! # Examples
//!
//! ```
//! use ruststream_nats::prelude::*;
//!
//! // Both names come from the one glob: the application info from the core, the broker from here.
//! let info = AppInfo::new("orders", "0.1.0");
//! let broker = NatsBroker::new("nats://localhost:4222");
//! # let _ = (info, broker, NatsPublish, JetStreamPublish::default());
//! ```

// The core prelude stops short of brokers, because which broker a service runs on is the one
// thing every service states for itself. Importing *this* prelude is that statement: the broker
// is named by the crate path the glob comes from, so the core prelude rides along rather than
// asking a service file for a second import that says nothing new.
pub use ruststream::prelude::*;
// The capability this crate implements on a form a service holds: native request-reply on the
// Core publisher, named in a bound. NATS could serve a replayable-log capability from JetStream,
// but this crate implements none of that family, so the glob does not offer it.
pub use ruststream::RequestReply;

pub use crate::{JetStreamPublish, NatsBroker, NatsPublish, SubscribeOptions};

// What stays out, though this crate implements it.
//
// `Partitioned`: implemented here, but the core surfaces `partition_key` through
// `IncomingMessage`'s defaulted method - re-exporting the trait would make the natural call
// ambiguous (E0034).
//
// `BatchSubscriber`: subscriber-side, consumed by the runtime's plumbing. A service declares the
// batch form of a handler and never names the trait.
//
// `DescribeServer`: contract machinery, read by the runtime and the AsyncAPI generator rather
// than written by a service.
//
// The `testing` module: broker-author tooling behind a feature, not the surface a service is
// written against, so it is imported where it is used and says by that import what it is.
//
// The message and live-publisher types (`NatsMessage`, `NatsPublisher`, `JetStreamPublisher`,
// `PublishAck`, `PARTITION_KEY_HEADER`): a service publishes through the builder, which assembles
// the message itself, and receives a decoded payload. Code that names one of these is working a
// layer below the service, the same reason the core prelude leaves `OutgoingMessage` out.
//
// `NatsError`: a service names an error where it handles one, which is rarely the file that
// mounts the handlers.
