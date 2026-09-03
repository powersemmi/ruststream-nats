//! The imports a service on NATS writes every time, in one glob.
//!
//! The broker, the subscription descriptor, the publish policies ([`NatsPublish`] is plain Core
//! NATS, [`JetStreamPublish`] waits for the stream's acknowledgement), the [`RequestReply`]
//! capability, and the whole core prelude.
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
//! # let _ = (info, broker, NatsPublish, JetStreamPublish::default());
//! ```

pub use ruststream::RequestReply;
pub use ruststream::prelude::*;

// The policies keep their prefixed names. `Publish` is the core's slot capability trait, carried
// by the glob above, and an explicit re-export beats a glob without a word: a policy named
// `Publish` here would silently take the trait away from every service that writes this glob, and
// the error would land in the service's file rather than in this one. `tests/prelude_names.rs`
// holds that line.
pub use crate::{JetStreamPublish, NatsBroker, NatsPublish, SubscribeOptions};

// `Partitioned` is kept out on purpose: the core also surfaces `partition_key` as a defaulted
// method on `IncomingMessage`, so re-exporting the trait makes that call ambiguous (E0034).
