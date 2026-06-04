//! `PySubscriber`: async iterator over [`PyMessage`].

use std::sync::Arc;

use pyo3::{exceptions::PyStopAsyncIteration, prelude::*};
use ruststream_pyo3::DeliveryRx;
use tokio_util::sync::CancellationToken;

use crate::message::PyMessage;

#[pyclass(name = "Subscriber")]
pub struct PySubscriber {
    rx: DeliveryRx,
    cancel: CancellationToken,
}

impl std::fmt::Debug for PySubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PySubscriber").finish_non_exhaustive()
    }
}

impl PySubscriber {
    pub(crate) fn new(rx: DeliveryRx, cancel: CancellationToken) -> Self {
        Self { rx, cancel }
    }
}

#[pymethods]
impl PySubscriber {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = Arc::clone(&self.rx);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let msg = rx
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| PyStopAsyncIteration::new_err("subscription closed"))?;
            Python::attach(|py| -> PyResult<Py<PyAny>> {
                let obj = Py::new(py, PyMessage::new(msg))?;
                Ok(obj.into_any())
            })
        })
    }

    fn close(&self) {
        self.cancel.cancel();
    }
}

impl Drop for PySubscriber {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
