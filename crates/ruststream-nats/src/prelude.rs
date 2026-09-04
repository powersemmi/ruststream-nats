//! The imports a mount site on NATS writes every time, in one glob.
//!
//! The broker, the subscription descriptor, the publish policies ([`Publish`] is plain Core NATS,
//! [`JetStreamPublish`] waits for the stream's acknowledgement), the [`RequestReply`] capability,
//! and the whole core prelude.
//!
//! Two vocabularies, one per file. A **handler body** names capabilities: it imports
//! `ruststream::prelude::*` and bounds an injected slot with the trait it needs
//! (`Out<impl Publisher>`, `Out<impl RequestReply>`), so the body says what it does with the slot
//! and never which broker fills it. A **routes file** names policies: it imports this prelude and
//! attaches one at the mount, where the broker is already chosen. The two never meet in a file, so
//! the mount-site names are uniform across brokers - `Publish` is whatever plain publishing is on
//! this transport - and a routes file reads the same whichever transport it was written against.
//!
//! There is no separate `Request` policy here: NATS correlates replies on the plain publisher, so
//! [`Publish`] pairs into the live form that carries [`RequestReply`] as well. A broker that opens
//! request-reply as a mode of its own aliases that policy `Request`.
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

pub use ruststream::prelude::*;

// The manifest layer: the capability traits this broker implements on its live values, named here
// so the glob that carries the policies also puts their operations in scope. `RequestReply` is
// also in the core prelude - it is named again because the manifest is this crate's statement
// about what its publisher can do, not a hole in the core glob.
pub use ruststream::RequestReply;

// The plain policy arrives under the uniform mount-site name; `JetStreamPublish` keeps its own,
// because JetStream is the concept rather than this broker's word for one. The prefixed originals
// stay at the crate root, for prose and for a file that wants to say NATS out loud.
pub use crate::NatsPublish as Publish;
pub use crate::{JetStreamPublish, NatsBroker, SubscribeOptions};

// `Partitioned` is kept out on purpose: the core also surfaces `partition_key` as a defaulted
// method on `IncomingMessage`, so re-exporting the trait makes that call ambiguous (E0034).
