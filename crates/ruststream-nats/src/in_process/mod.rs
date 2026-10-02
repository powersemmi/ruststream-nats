//! The broker's in-process mode, behind the `testing` feature: the transport a connected broker
//! carries when the test harness connects it through `InProcess::connect_in_process` rather than
//! through `connect`.
//!
//! The connected broker, its subscriber, its publishers and its `JetStream` delivery each carry
//! this transport as a variant of their own, so a service's descriptors and publish policies run
//! against it unchanged. It has no configuration of its own: the addresses are parsed as the
//! client parses them, and the connection options that change what the server delivers
//! (`no_echo`, the inbox prefix) are read from the broker. It never succeeds where a server
//! fails. A publish, a subscription or an acknowledgement the client or the server refuses (a
//! subject the frame cannot carry, a header with no NATS form, a message over the server's
//! payload limit, a `JetStream` publish no stream takes or whose expectation does not hold, a
//! request nobody answers, a handle outliving its connection) is refused here with the same
//! error.
//!
//! What it models: subject matching with `*` and `>`; queue groups, keyed by name as the server
//! keys them; request and reply, with the immediate "no responders"; streams that store what
//! their consumers' filters place in them, with sequences, the deduplication window and the
//! publish expectations; durable and ephemeral consumers with their deliver policy, their
//! metadata, negative acknowledgement, termination, delayed redelivery and the `ack_wait`
//! redelivery of a delivery dropped unsettled. What belongs to the server and is left to the
//! live mode: a stream's own subject list and its limits (a stream here stores what a consumer
//! of the app reads, a publish that names a stream reaches it until a consumer of the app says
//! what that stream serves, and from then on a subject outside it is refused), `max_ack_pending`,
//! a subscription's pending limits, and a stream or consumer the service expects to find (a
//! subscription here finds every stream it names).

mod bus;
mod delivery;
mod routes;
mod subject;

use std::str::FromStr;
use std::sync::Arc;

use async_nats::{ConnectOptions, ServerAddr};
use tokio::runtime::Handle;

pub(crate) use bus::{Bus, Origin, PublishMode, Settings};
pub(crate) use delivery::{Consumer, Feed, JetStreamDelivery, Release};
pub(crate) use routes::{Route, RouteBook};

use crate::error::NatsError;
use crate::subject::{NatsSubscription, SubscriptionPlan};
use crate::subscriber::NatsSubscriber;
use bus::ConsumerSpec;
use subject::{SubjectPattern, check_queue_group};

/// The in-process connection for a broker configured with `addrs` and `options`, keeping the
/// runtime it is called on as the one the broker connected on.
///
/// # Panics
///
/// Panics outside a Tokio runtime, as the client's `connect` does.
///
/// # Errors
///
/// Returns [`NatsError::Connect`] for an address the client does not parse, as `connect` does.
pub(crate) fn connect(addrs: &str, options: &ConnectOptions) -> Result<Arc<Bus>, NatsError> {
    for addr in addrs.split(',') {
        ServerAddr::from_str(addr.trim()).map_err(|err| NatsError::Connect(Box::new(err)))?;
    }
    Ok(Bus::new(settings(options)?, Handle::current()))
}

/// Reads the options that change what the server delivers.
///
/// Why through `Debug`: `ConnectOptions` keeps its fields private and offers no getter, and its
/// `Debug` output is the one place that states them. A version of the client that stops printing
/// one fails the in-process connect rather than letting it run on a default the service did not
/// set.
pub(crate) fn settings(options: &ConnectOptions) -> Result<Settings, NatsError> {
    let printed = format!("{options:?}");
    let field = |name: &str| -> Result<String, NatsError> {
        let key = format!("\"{name}\": ");
        let start = printed.find(&key).ok_or_else(|| {
            NatsError::Connect(
                format!("the client's connect options no longer state `{name}`").into(),
            )
        })? + key.len();
        let rest = &printed[start..];
        let Some(quoted) = rest.strip_prefix('"') else {
            return Ok(rest[..rest.find([',', '}']).unwrap_or(rest.len())].to_owned());
        };
        // A string prints as a Rust literal: read up to the closing quote, undoing the escapes
        // `Debug` wrote, so a quote or a backslash inside the value neither ends nor alters it.
        let mut value = String::new();
        let mut chars = quoted.chars();
        while let Some(c) = chars.next() {
            match c {
                '"' => return Ok(value),
                '\\' => value.push(chars.next().unwrap_or('\\')),
                c => value.push(c),
            }
        }
        Err(NatsError::Connect(
            format!("the client's connect options state `{name}` unterminated").into(),
        ))
    };
    let no_echo = field("no_echo")? == "true";
    let inbox_prefix = field("inbox_prefix")?;
    Ok(Settings {
        no_echo,
        inbox_prefix,
    })
}

/// A `JetStream` name the server accepts: a stream or a durable consumer.
fn check_name(what: &str, name: &str) -> Result<(), NatsError> {
    if name.is_empty()
        || name
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '.' | '*' | '>' | '/' | '\\'))
    {
        return Err(NatsError::JetStream(
            format!("invalid {what} name `{name}`").into(),
        ));
    }
    Ok(())
}

/// The route a live subscription the server accepted takes, for the harness's routing.
pub(crate) fn route<S: NatsSubscription>(source: &S) -> Option<Route> {
    let subject = source.subject();
    match source.plan() {
        SubscriptionPlan::Core { queue_group } => SubjectPattern::parse(subject)
            .ok()
            .map(|pattern| Route::core(subject, pattern, queue_group)),
        SubscriptionPlan::JetStream {
            stream,
            durable,
            filter_subject,
            ..
        } => SubjectPattern::parse(filter_subject)
            .ok()
            .map(|filter| Route::jetstream(subject, filter, stream, durable)),
    }
}

/// Opens the subscription `source` describes on the in-process connection, refusing what the
/// client or the server refuses, and records it in `book`.
///
/// # Errors
///
/// [`NatsError::Closed`] after shutdown; [`NatsError::Subscribe`] for a subject or a queue group
/// the client refuses; [`NatsError::JetStream`] for a stream, durable or filter the server
/// refuses.
pub(crate) fn subscribe<S: NatsSubscription>(
    bus: &Arc<Bus>,
    book: &RouteBook,
    source: &S,
) -> Result<NatsSubscriber, NatsError> {
    let subject = source.subject();
    bus.ensure_live(subject)?;
    match source.plan() {
        SubscriptionPlan::Core { queue_group } => {
            let pattern = SubjectPattern::parse(subject)
                .map_err(|err| NatsError::Subscribe(Box::new(err)))?;
            if let Some(queue) = queue_group {
                check_queue_group(queue).map_err(|err| NatsError::Subscribe(Box::new(err)))?;
            }
            book.record(Route::core(subject, pattern.clone(), queue_group));
            let feed = bus.subscribe_core(pattern, queue_group);
            Ok(NatsSubscriber::in_process_core(subject.to_owned(), feed))
        }
        SubscriptionPlan::JetStream {
            stream,
            durable,
            filter_subject,
            ack_wait,
            deliver_policy,
            ..
        } => {
            check_name("stream", stream)?;
            if let Some(durable) = durable {
                check_name("durable", durable)?;
            }
            let filter = SubjectPattern::parse(filter_subject)
                .map_err(|err| NatsError::JetStream(Box::new(err)))?;
            book.record(Route::jetstream(subject, filter.clone(), stream, durable));
            let (feed, requeue, consumer) = bus.subscribe_jetstream(ConsumerSpec {
                stream,
                durable,
                filter,
                deliver_policy,
            });
            Ok(NatsSubscriber::in_process_jetstream(
                subject.to_owned(),
                Consumer::new(feed, requeue, consumer, ack_wait),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_settings_are_the_ones_the_options_state() {
        let defaults = settings(&ConnectOptions::new()).expect("the options state both");
        assert!(!defaults.no_echo);
        assert_eq!(defaults.inbox_prefix, "_INBOX");

        let set = settings(&ConnectOptions::new().no_echo().custom_inbox_prefix("_SVC"))
            .expect("the options state both");
        assert!(set.no_echo);
        assert_eq!(set.inbox_prefix, "_SVC");

        let escaped = settings(&ConnectOptions::new().custom_inbox_prefix(r#"_S"V\C"#))
            .expect("the options state both");
        assert_eq!(escaped.inbox_prefix, r#"_S"V\C"#);
    }

    // A runtime to connect on, as the client's own connect needs one.
    #[tokio::test]
    async fn an_address_the_client_refuses_is_refused() {
        assert!(connect("nats://localhost:4222", &ConnectOptions::new()).is_ok());
        assert!(connect("nats://a:4222, nats://b:4222", &ConnectOptions::new()).is_ok());
        assert!(matches!(
            connect("http://localhost:4222", &ConnectOptions::new()),
            Err(NatsError::Connect(_))
        ));
    }
}
