//! Python binding for the in-process handler-stub transport.
//!
//! Exposed as `ruststream_nats._native.NatsTestBroker` and consumed privately by
//! `ruststream_nats.testing.TestNatsBroker`, which swaps it in as the transport of an
//! existing `NatsBroker` so the broker's own handlers, middleware, codec and DI are
//! exercised with no network.

use std::sync::Arc;

use pyo3::prelude::*;
use ruststream::{Headers, OutgoingMessage, Publisher, RawMessage};
use ruststream_nats::testing::NatsTestBroker;
use ruststream_pyo3::{pump_subscriber, to_pyerr};

use crate::{options::build_subscribe_options, subscriber::PySubscriber};

/// Handler-stub NATS broker used for synchronous, in-process tests of user handlers.
#[pyclass(name = "NatsTestBroker", frozen)]
pub struct PyNatsTestBroker {
    inner: Arc<NatsTestBroker>,
}

impl std::fmt::Debug for PyNatsTestBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyNatsTestBroker").finish_non_exhaustive()
    }
}

#[pymethods]
impl PyNatsTestBroker {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(NatsTestBroker::new()),
        }
    }

    fn publish<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        payload: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let publisher = inner.publisher();
            publisher
                .publish(OutgoingMessage::new(topic.as_str(), payload.as_slice()))
                .await
                .map_err(|err| to_pyerr(&err))
        })
    }

    #[pyo3(signature = (
        topic,
        *,
        queue_group=None,
        jetstream=None,
        durable=None,
        filter_subject=None,
        ack_wait=None,
        max_ack_pending=None,
        deliver_policy=None,
        start_sequence=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        queue_group: Option<String>,
        jetstream: Option<String>,
        durable: Option<String>,
        filter_subject: Option<String>,
        ack_wait: Option<f64>,
        max_ack_pending: Option<i64>,
        deliver_policy: Option<String>,
        start_sequence: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let opts = build_subscribe_options(
            topic,
            queue_group,
            jetstream,
            durable,
            filter_subject,
            ack_wait,
            max_ack_pending,
            deliver_policy,
            start_sequence,
        )?;
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let subscriber = inner.subscribe(opts).await.map_err(|err| to_pyerr(&err))?;
            let (rx, cancel) = pump_subscriber(subscriber);
            Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                let obj = Py::new(py, PySubscriber::new(rx, cancel))?;
                Ok(obj.into_any())
            })
        })
    }

    /// Awaits until `count` messages have been published on `topic` (or `timeout_secs` elapses)
    /// and returns the recorded payloads + headers as a list of dicts.
    #[pyo3(signature = (topic, count, timeout_secs=1.0))]
    fn expect_published<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        count: usize,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        if timeout_secs <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "timeout_secs must be > 0",
            ));
        }
        let inner = Arc::clone(&self.inner);
        let timeout_dur = std::time::Duration::from_secs_f64(timeout_secs);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let messages = inner
                .expect_published(topic.as_str(), count, timeout_dur)
                .await;
            Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                let list = pyo3::types::PyList::empty(py);
                for msg in messages {
                    let dict = raw_message_to_pydict(py, &msg)?;
                    list.append(dict)?;
                }
                Ok(list.into_any().unbind())
            })
        })
    }

    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            ruststream::Broker::shutdown(&*inner)
                .await
                .map_err(|err| to_pyerr(&err))?;
            Ok::<_, PyErr>(())
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "NatsTestBroker(id=0x{:x})",
            Arc::as_ptr(&self.inner) as usize
        )
    }
}

fn raw_message_to_pydict(py: Python<'_>, msg: &RawMessage) -> PyResult<Py<PyAny>> {
    let dict = pyo3::types::PyDict::new(py);
    dict.set_item("topic", msg.topic())?;
    dict.set_item("payload", pyo3::types::PyBytes::new(py, msg.payload()))?;
    let headers = headers_to_pydict(py, msg.headers())?;
    dict.set_item("headers", headers)?;
    Ok(dict.into_any().unbind())
}

fn headers_to_pydict(py: Python<'_>, headers: &Headers) -> PyResult<Py<PyAny>> {
    let dict = pyo3::types::PyDict::new(py);
    for (name, value) in headers.iter() {
        dict.set_item(name, pyo3::types::PyBytes::new(py, value))?;
    }
    Ok(dict.into_any().unbind())
}
