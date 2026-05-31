//! `PyMessage`: a single NATS delivery exposed to Python.

use std::{collections::HashMap, sync::Mutex};

use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyBytes, PyDict},
};
use ruststream_pyo3::{PyIncomingMessage, to_pyerr};

#[pyclass(name = "Message", frozen)]
pub struct PyMessage {
    inner: Mutex<Option<Box<dyn PyIncomingMessage>>>,
    payload_snapshot: Vec<u8>,
    headers_snapshot: HashMap<String, Vec<u8>>,
}

impl std::fmt::Debug for PyMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyMessage")
            .field("payload_len", &self.payload_snapshot.len())
            .field("headers", &self.headers_snapshot.len())
            .finish_non_exhaustive()
    }
}

impl PyMessage {
    pub(crate) fn new(msg: Box<dyn PyIncomingMessage>) -> Self {
        let payload_snapshot = msg.payload().to_vec();
        let headers_snapshot = msg
            .headers()
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        Self {
            inner: Mutex::new(Some(msg)),
            payload_snapshot,
            headers_snapshot,
        }
    }

    fn take_inner(&self) -> PyResult<Box<dyn PyIncomingMessage>> {
        self.inner
            .lock()
            .expect("PyMessage mutex poisoned")
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("message already acknowledged"))
    }
}

#[pymethods]
impl PyMessage {
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.payload_snapshot)
    }

    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (name, value) in &self.headers_snapshot {
            dict.set_item(name.as_str(), PyBytes::new(py, value))?;
        }
        Ok(dict)
    }

    fn ack<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.take_inner()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.ack().await.map_err(|err| to_pyerr(err.as_ref()))
        })
    }

    #[pyo3(signature = (requeue=false))]
    fn nack<'py>(&self, py: Python<'py>, requeue: bool) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.take_inner()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .nack(requeue)
                .await
                .map_err(|err| to_pyerr(err.as_ref()))
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Message(payload_len={}, headers={})",
            self.payload_snapshot.len(),
            self.headers_snapshot.len(),
        )
    }
}
