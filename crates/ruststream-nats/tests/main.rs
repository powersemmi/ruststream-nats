//! The integration suite, one test binary: each file under `tests/` is a module of it.

mod live;

mod asyncapi_nats;
mod both_modes;
mod conformance_nats;
mod handlers;
mod in_process_nats;
mod integration_nats;
mod integration_nats_batch;
mod integration_nats_consumer;
mod integration_nats_retry;
mod prelude_names;
mod publish_allocations;
