//! The two vocabularies the preludes keep apart.
//!
//! A handler body imports the core prelude and names capability traits; a routes file imports this
//! one and names policies. This prelude therefore has to deliver both halves: the core's trait
//! vocabulary unchanged through its glob, and the crate's policies under the uniform mount-site
//! names. Each probe below asks for one name in the position its vocabulary gives it, so a
//! re-export that shifted a name would fail here rather than in a user's file.

use ruststream_nats::prelude::*;

/// The routes-side name: `Publish` is this broker's plain publish policy, a value a mount site
/// attaches. Uniform across brokers, which is what lets a routes file read the same whichever
/// transport it was written against.
fn _publish_is_this_brokers_policy() {
    // Both positions have to be the policy: a name that resolved to a trait would fail in the
    // type position, and a name that resolved to anything else would fail in the value position.
    let _: Publish = Publish;
}

/// `JetStreamPublish` keeps its own name: `JetStream` is the concept, not this broker's word for
/// one.
fn _jetstream_publish_is_a_policy_too() {
    let _: JetStreamPublish = JetStreamPublish::default();
}

/// The body-side name: `Publisher` is the core's publish capability, the bound a `#[subscriber]`
/// handler writes on an injected slot (`Out<impl Publisher>`). It must survive this glob.
fn _publisher_is_the_core_capability<T: Publisher>() {}

/// `RequestReply` is the capability this prelude deliberately carries beside the core's: a handler
/// bounding a slot with it compiles here because NATS correlates replies natively.
fn _request_reply_is_the_core_capability<T: RequestReply>() {}

/// `PublishPolicy` is the core's declaration half of a publisher; this crate's policies implement
/// it rather than replacing the name.
fn _publish_policy_is_the_core_trait<T: PublishPolicy<C>, C: ruststream::ConnectedBroker>() {}
