//! The crate prelude must not shadow a core name.
//!
//! It globs the core prelude and then re-exports this crate's own items. An explicit re-export
//! beats a glob silently, so a name this crate spells the same as a core one takes the core's
//! meaning away from every service that writes the glob - and the failure surfaces in the user's
//! file, not here. Each probe below asks for a core name through this crate's prelude in the
//! position the core gives it.

use ruststream_nats::prelude::*;

/// `Publish` is the core's slot capability trait: what a manual body bounds an injections-arena
/// entry with. This crate's plain publish policy is `NatsPublish`, and naming it `Publish` here
/// would turn this bound into `E0404: expected trait, found struct`.
fn _publish_is_the_core_trait<T: Publish>() {}

/// `Publisher` is the core's broker-side publish trait, the bound a `#[subscriber]` handler writes
/// on an injected slot (`Out<impl Publisher>`).
fn _publisher_is_the_core_trait<T: Publisher>() {}

/// `PublishPolicy` is the core's declaration half of a publisher; this crate's policies implement
/// it rather than replacing the name.
fn _publish_policy_is_the_core_trait<T: PublishPolicy<C>, C: ruststream::ConnectedBroker>() {}

/// `RequestReply` is the capability this crate's prelude deliberately carries; it must arrive as
/// the core trait, not as a name of this crate's own.
fn _request_reply_is_the_core_trait<T: RequestReply>() {}
