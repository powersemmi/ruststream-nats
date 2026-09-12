//! The gate the live suites share: when a run is supposed to reach a server, a skip is a defect.
//!
//! A live test skips when `NATS_TEST_URL` is unset, which is what keeps the suites usable during
//! development: `cargo test` on a laptop with no server passes. The same skip in CI is a lie,
//! because the job stood a server up first, and a suite that returns before its first assertion
//! reports `ok` exactly like one that ran. The live job therefore sets `RUSTSTREAM_REQUIRE_LIVE`,
//! and under that flag every skip becomes a failure naming what it wanted.

use std::fmt::Display;

/// The variable a job sets to say it stood a server up, so skipping past it is a defect.
pub(crate) const REQUIRE_LIVE: &str = "RUSTSTREAM_REQUIRE_LIVE";

/// Whether this run is required to reach a live server.
fn required() -> bool {
    std::env::var(REQUIRE_LIVE).is_ok_and(|value| !value.is_empty())
}

/// The server URL from `name`, or `None` to skip the test.
///
/// # Panics
///
/// Panics when [`REQUIRE_LIVE`] is set and `name` is not: a job that stood a server up and then
/// lost its address is a broken job, and the tests behind it would have passed without running.
#[allow(dead_code)] // Each test target uses the half of this module its own gate needs.
pub(crate) fn url(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Some(value),
        _ => {
            assert!(
                !required(),
                "{REQUIRE_LIVE} is set, so this suite must run, but {name} is unset or empty",
            );
            None
        }
    }
}

/// Reports a server that could not be reached, and returns so the caller can skip.
///
/// # Panics
///
/// Panics when [`REQUIRE_LIVE`] is set: a server the job started and the suite cannot reach is a
/// failure, not a reason to pass quietly.
#[allow(dead_code)] // See `url`.
pub(crate) fn unreachable(url: &str, err: &dyn Display) {
    assert!(
        !required(),
        "{REQUIRE_LIVE} is set, so this suite must run, but NATS at {url} could not be reached: \
         {err}",
    );
    eprintln!("could not reach NATS at {url}: {err}; skipping");
}
