# Contributing to ruststream-nats

`ruststream-nats` is the NATS broker crate of RustStream. The framework's core crate lives in
[`powersemmi/ruststream`](https://github.com/powersemmi/ruststream), and each of the other broker
crates in a repository of its own. This page covers the environment, the checks a change passes
before review, and how a change is tested against a local checkout of the core.

## Repositories

This crate depends on the core through crates.io and builds on its own. To work on both, clone
them side by side in one directory:

```text
RustStream/
  ruststream/
  ruststream-nats/
  ...           the other broker crates, when a change reaches them too
```

```bash
mkdir RustStream && cd RustStream
git clone https://github.com/powersemmi/ruststream.git
git clone https://github.com/powersemmi/ruststream-nats.git
```

## Environment

- **Rust** through rustup. `rust-toolchain.toml` selects stable with rustfmt and clippy. The
  minimum supported version is 1.88, the `rust-version` in `Cargo.toml`:
  `rustup toolchain install 1.88` builds against it with
  `cargo +1.88 check --workspace --all-features`.
- **just**, which runs every recipe below.
- **Docker** with Compose, for the NATS server with JetStream that the live suite and the benchmarks run against.
- Per task:

| Task | Tool | Install |
| --- | --- | --- |
| `just deny` | cargo-deny | `cargo install cargo-deny --locked` |
| `just typo`, `just zizmor` | uv | the uv documentation |
| rendering a scaffold under `templates/` | cargo-generate | `cargo install cargo-generate --locked` |
| `just bench` | Python 3 | the system package manager |
| `just bench-code` | valgrind and the benchmark runner | the system package manager, then `cargo install --locked gungraun-runner --version =0.19.4` |
| the documentation site | Python 3.12 | `pip install -r docs/requirements.txt`, then `properdocs serve` |

## Checking a change

```bash
just check          # rustfmt, clippy, cargo check with all features and with none
just test           # the suite in process, NatsBroker with no server
just test-brokers   # the live suite against NATS with JetStream
just ci             # check and test, plus codespell, cargo deny and zizmor
```

`just test` runs the suite in process; the live tests skip themselves there. `just test-brokers`
starts the Compose stand, runs the whole suite against it with the live tests required, and stops
the stand.

The `nats` and `nats-js` scaffolds under `templates/` are rendered and compiled against this
branch by CI's lint job whenever they or the crate change. `cargo generate --path templates/nats
--name smoke` renders one locally.

`just bench` measures what this crate and the framework's runtime cost over the raw `async-nats`
client on the same stand and rewrites `docs/benchmarks/results.json`. It takes minutes and wants
the machine to itself. `just bench-code` counts what a message costs on the service's thread (the
framework, this crate and the client) in instructions and allocations, on the same stand, and
rewrites the code table of the same document; it takes under a minute.

## Testing against a local core

A change in the core is tested here before it is released, with the `ruststream` dependency
patched to the checkout next to this one:

```bash
cargo test --workspace --all-features --config "patch.crates-io.ruststream.path='../ruststream'"
```

From the core, `just brokers nats` runs the same command against the core's working tree.

For the live suite against the local core, start the stand, run the command `test-brokers` runs
with the core patched in, and stop the stand:

```bash
just brokers-up
NATS_TEST_URL=nats://127.0.0.1:4222 RUSTSTREAM_REQUIRE_LIVE=1 \
    cargo test --workspace --all-features \
    --config "patch.crates-io.ruststream.path='../ruststream'" -- --test-threads=1
just brokers-down
```

## Pull requests

- One logical change per pull request.
- Open it as a draft. CI runs when it is marked ready for review.
- A pull request merges as one squashed commit, after the `CI result` check and one approving
  review. Commits are signed.
- Documentation changes with the code. An item's rustdoc says what it is and does, and the crate's
  module overviews are its guides. The site holds the entry pages in English, Russian and Chinese,
  and an edit reaches all three.
