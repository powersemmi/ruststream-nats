//! Native extension for the `ruststream_nats` Python package.
//!
//! Exposes `NatsBroker` (real `async-nats` client) and `NatsTestBroker` (handler-stub
//! dispatcher) plus the per-wheel `Message` / `Subscriber` pyclasses.

#![allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]

use pyo3::prelude::*;

mod broker;
mod message;
mod options;
mod subscriber;
mod testing;

#[pymodule]
fn _native(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    ruststream_pyo3::install_runtime();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<broker::PyNatsBroker>()?;
    m.add_class::<testing::PyNatsTestBroker>()?;
    m.add_class::<message::PyMessage>()?;
    m.add_class::<subscriber::PySubscriber>()?;
    Ok(())
}
