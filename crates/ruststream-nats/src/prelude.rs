//! The imports a service on NATS writes every time, in one glob.
//!
//! `use ruststream_nats::prelude::*;` carries the core's own prelude plus this crate's broker, its
//! three subscription descriptors ([`CoreSubject`], [`CoreWildcard`], [`JetStreamSubject`]), the
//! publish policies ([`Publish`] for plain Core NATS, [`JetStreamPublish`] for a stream), the
//! [`RequestReply`] capability [`Publish`] serves, and the steps of a per-message `JetStream`
//! setting. The
//! [Prelude](https://powersemmi.github.io/ruststream-nats/latest/components/broker/prelude/)
//! page shows it in a service.
//!
//! # Two vocabularies, two files
//!
//! A handler body imports `ruststream::prelude::*` and bounds an injected slot with the
//! **capability trait** it needs (`Out<impl Publisher>`, `Out<impl RequestReply>`), so it names no
//! broker type at all. A mount site imports this prelude, which carries the core one plus the
//! **policies** under their concept names: [`Publish`] is whatever plain publishing is on this
//! transport, and [`JetStreamPublish`] keeps its own name. Include sites therefore read the same on
//! every broker, and the two vocabularies never meet in one file.
//!
//! One handler body imports this prelude too: the one that writes a per-message `JetStream`
//! setting. [`JetStreamPublishSteps`] puts `message_id` and the three expectations on the publish
//! builder, and a body that calls one bounds its slot
//! `Out<impl Publisher<Options = JetStreamOptions>, _>`; that bound is the stated exception to the
//! rule above.
//!
//! Globbing two broker preludes conflicts on these names where one is used (E0659); the prefixed
//! types at the crate root are the disambiguation.
//!
//! # Examples
//!
//! ```
//! # mod demo {
//! use ruststream_nats::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[derive(Deserialize, Serialize, Outgoing)]
//! #[outgoing(name = "orders.confirmed")]
//! struct Confirmation {
//!     id: u64,
//! }
//!
//! #[subscriber(JetStreamSubject::new("orders.created", "ORDERS").durable("confirmer"), reply)]
//! async fn confirm(order: &Order) -> Confirmation {
//!     Confirmation { id: order.id }
//! }
//!
//! #[subscriber(CoreSubject::new("orders.confirmed").queue_group("mailers"))]
//! async fn mail(confirmation: &Confirmation) -> HandlerOutcome {
//!     tracing::info!(confirmation.id, "mailed");
//!     HandlerOutcome::ack()
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
//!         NatsBroker::new("nats://localhost:4222"),
//!         |b| {
//!             b.include(confirm)
//!                 .out_reply(JetStreamPublish::default().expect_stream("ORDERS"));
//!             b.include(mail);
//!         },
//!     )
//! }
//! # }
//! # fn main() {}
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
pub use crate::{
    CoreSubject, CoreWildcard, JetStreamOptions, JetStreamPublish, JetStreamPublishSteps,
    JetStreamSubject, NatsBroker, NonZeroDuration,
};

// `Partitioned` is kept out on purpose: the core also surfaces `partition_key` as a defaulted
// method on `IncomingMessage`, so re-exporting the trait makes that call ambiguous (E0034).
