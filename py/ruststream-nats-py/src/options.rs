//! Maps Python kwargs onto [`ruststream_nats::SubscribeOptions`].
//!
//! Used by both `PyNatsBroker.subscribe` and `PyNatsTestBroker.subscribe` so the public
//! Python signature is identical between real and handler-stub modes.

use std::time::Duration;

use pyo3::{exceptions::PyValueError, prelude::*};
use ruststream_nats::{DeliverPolicy, SubscribeOptions};

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_subscribe_options(
    topic: String,
    queue_group: Option<String>,
    jetstream: Option<String>,
    durable: Option<String>,
    filter_subject: Option<String>,
    ack_wait: Option<f64>,
    max_ack_pending: Option<i64>,
    deliver_policy: Option<String>,
    start_sequence: Option<u64>,
) -> PyResult<SubscribeOptions> {
    let mut opts = SubscribeOptions::new(topic);
    if let Some(q) = queue_group {
        opts = opts.queue_group(q);
    }
    if let Some(s) = jetstream {
        opts = opts.jetstream(s);
    }
    if let Some(d) = durable {
        opts = opts.durable(d);
    }
    if let Some(f) = filter_subject {
        opts = opts.filter_subject(f);
    }
    if let Some(w) = ack_wait {
        if w <= 0.0 {
            return Err(PyValueError::new_err("ack_wait must be > 0 seconds"));
        }
        opts = opts.ack_wait(Duration::from_secs_f64(w));
    }
    if let Some(m) = max_ack_pending {
        opts = opts.max_ack_pending(m);
    }
    if let Some(p) = deliver_policy {
        let policy = match p.as_str() {
            "all" => DeliverPolicy::All,
            "last" => DeliverPolicy::Last,
            "new" => DeliverPolicy::New,
            "by_start_sequence" => {
                let seq = start_sequence.ok_or_else(|| {
                    PyValueError::new_err(
                        "deliver_policy='by_start_sequence' requires start_sequence",
                    )
                })?;
                DeliverPolicy::ByStartSequence {
                    start_sequence: seq,
                }
            }
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown deliver_policy: {other:?} (expected one of all/last/new/by_start_sequence)"
                )));
            }
        };
        opts = opts.deliver_policy(policy);
    }
    Ok(opts)
}
