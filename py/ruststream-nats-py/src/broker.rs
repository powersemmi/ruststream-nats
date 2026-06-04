//! `PyNatsBroker`: connects to a real NATS server and exposes pub/sub to Python.

use std::sync::Arc;

use pyo3::{prelude::*, types::PyType};
use ruststream::{OutgoingMessage, Publisher};
use ruststream_nats::NatsBroker;
use ruststream_pyo3::{pump_subscriber, to_pyerr};

use crate::{options::build_subscribe_options, subscriber::PySubscriber};

/// NATS broker handle backed by the real `async-nats` client.
#[pyclass(name = "NatsBroker", frozen)]
pub struct PyNatsBroker {
    inner: Arc<NatsBroker>,
}

impl std::fmt::Debug for PyNatsBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyNatsBroker").finish_non_exhaustive()
    }
}

#[pymethods]
impl PyNatsBroker {
    /// Connects to a NATS server. Returns an awaitable resolving to a `NatsBroker` instance.
    #[classmethod]
    fn connect<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        url: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let broker = NatsBroker::connect(url.as_str())
                .await
                .map_err(|err| to_pyerr(&err))?;
            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let obj = Py::new(
                    py,
                    Self {
                        inner: Arc::new(broker),
                    },
                )?;
                Ok(obj.into_any())
            })
        })
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

    /// Publishes every payload in `payloads` to `topic`, then flushes once.
    ///
    /// Enqueues all messages on the shared client and crosses the Python/async boundary a
    /// single time, instead of one awaitable per message. The trailing flush makes the call
    /// resolve only after the client has handed the whole batch to the connection, preserving
    /// producer flow control. Ordering is retained; this is not an atomic transaction.
    fn publish_batch<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        payloads: Vec<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let publisher = inner.publisher();
            for payload in &payloads {
                publisher
                    .publish(OutgoingMessage::new(topic.as_str(), payload.as_slice()))
                    .await
                    .map_err(|err| to_pyerr(&err))?;
            }
            inner.client().flush().await.map_err(|err| to_pyerr(&err))
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
            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let obj = Py::new(py, PySubscriber::new(rx, cancel))?;
                Ok(obj.into_any())
            })
        })
    }

    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.shutdown_client().await;
            Ok::<_, PyErr>(())
        })
    }

    fn __repr__(&self) -> String {
        format!("NatsBroker(id=0x{:x})", Arc::as_ptr(&self.inner) as usize)
    }
}
