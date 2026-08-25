//! The imports a service on NATS writes every time, in one glob.
//!
//! `use ruststream_nats::prelude::*;` brings in the broker type, the subscription descriptor a
//! mount site fills in, and both publish policies, on top of the whole core prelude. One import
//! then serves a service file: the application object, the handler surface and the attribute
//! macros arrive with it.
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

pub use crate::{JetStreamPublish, NatsBroker, NatsPublish, SubscribeOptions};

// Three groups stay out.
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
