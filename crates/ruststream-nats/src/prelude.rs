//! The imports a service on NATS writes every time, in one glob.
//!
//! The broker, the subscription descriptor, the publish policies ([`Publish`] is plain Core NATS,
//! [`JetStreamPublish`] waits for the stream's acknowledgement), the [`RequestReply`] capability,
//! and the whole core prelude.
//!
//! Globbing this beside another broker's prelude is safe: the core items they share resolve to the
//! same types.
//!
//! # Examples
//!
//! ```
//! use ruststream_nats::prelude::*;
//!
//! let info = AppInfo::new("orders", "0.1.0");
//! let broker = NatsBroker::new("nats://localhost:4222");
//! # let _ = (info, broker, Publish, JetStreamPublish::default());
//! ```

pub use ruststream::RequestReply;
pub use ruststream::prelude::*;

pub use crate::NatsPublish as Publish;
pub use crate::{JetStreamPublish, NatsBroker, SubscribeOptions};

// `Partitioned` is kept out on purpose: the core also surfaces `partition_key` as a defaulted
// method on `IncomingMessage`, so re-exporting the trait makes that call ambiguous (E0034).
