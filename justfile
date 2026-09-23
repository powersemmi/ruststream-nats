set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    # The benchmark package is left out of the all-features legs on purpose: it is built with the
    # feature set a service ships, and the framework's harness feature is a compile error in it.
    # Its own leg follows.
    cargo clippy --workspace --exclude ruststream-nats-bench --all-targets --all-features -- -D warnings
    cargo clippy -p ruststream-nats-bench --all-targets -- -D warnings
    cargo check --workspace --exclude ruststream-nats-bench --all-targets --all-features
    cargo check --workspace --no-default-features

test:
    cargo test --workspace --all-features

brokers-up:
    docker compose -f docker-compose.test.yml up -d --wait

brokers-down:
    docker compose -f docker-compose.test.yml down -v

test-brokers: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    # This recipe starts the stand, so a gated test that skips itself here is a fault, not a
    # developer without a broker.
    NATS_TEST_URL=nats://127.0.0.1:4222 \
    RUSTSTREAM_REQUIRE_LIVE=1 \
        cargo test --workspace --all-features -- --test-threads=1

# What this crate costs over the async-nats client it wraps: one scenario run as a RustStream
# service and as a hand-written loop, against the stand the tests use. On demand only - it takes
# minutes and it wants the machine to itself. The page it feeds is docs/benchmarks.md.
bench *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    mkdir -p target
    # RUSTFLAGS is cleared so the numbers are not tied to this machine's CPU: a binary built with
    # `-C target-cpu=native` cannot be reproduced anywhere else.
    RUSTFLAGS="" NATS_TEST_URL=nats://127.0.0.1:4222 \
    RUSTSTREAM_BENCH_OUT="$PWD/target/bench-paired.json" \
        cargo bench -p ruststream-nats-bench --bench paired {{ ARGS }}
    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json

# What a message costs in the framework and the crate's in-process transport, counted under
# valgrind: instructions through callgrind and allocations through DHAT, each scenario a service
# on that transport.
# It takes seconds and the counts repeat within a tenth of a percent, so it needs no stand and no
# quiet machine. The page it feeds is the code table of docs/benchmarks.md. RUSTFLAGS is cleared
# because valgrind aborts on the instructions a recent CPU advertises. Needs valgrind and the
# runner the benches pin: cargo install --locked gungraun-runner --version =0.19.4
# Extra arguments reach the runner: `just bench-code --save-baseline=main` records a baseline,
# `just bench-code --baseline=main` compares against it.
bench-code *ARGS:
    mkdir -p target
    RUSTFLAGS="" cargo bench -p ruststream-nats --features testing \
        --bench consume --bench reply --bench batch \
        -- --output-format=json {{ ARGS }} > target/bench-code.json
    python3 scripts/bench_results.py --code target/bench-code.json docs/benchmarks/results.json

fmt:
    cargo fmt --all

build:
    cargo build --workspace --release

security: deny zizmor

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

zizmor:
    uvx zizmor .github/workflows

typo:
    uvx codespell

clean:
    cargo clean
    rm -rf dist wheels

ci: check test typo security
