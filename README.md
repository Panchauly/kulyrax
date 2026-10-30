# Kulyrax

[![CI](https://github.com/Kulyrax/kulyrax/actions/workflows/ci.yml/badge.svg)](https://github.com/Kulyrax/kulyrax/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#status)

**Kulyrax** deterministically projects events into SQL, document, key-value, and graph
stores using explicit, schema-driven mappings. It is **projection-only**: it consumes an
event log it does not own and never produces, buffers, or stores events itself (see
[`architecture.md` §1.1](architecture.md)).

Every storage kind has a real database backend, not just a file-backed default — SQL on
SQLite, Postgres, or MySQL; key-value on Redis; document on MongoDB or Elasticsearch;
graph on Neo4j — all behind the exact same sequence-gated idempotency model, alongside
file-backed document, key-value and graph adapters. Adding another is a public
extension point, not a fork: see
[`docs/writing-a-backend.md`](docs/writing-a-backend.md).

## Install

Download the archive for your platform from the
[latest release](https://github.com/Kulyrax/kulyrax/releases/latest), unpack it, and
put `kulyrax` on your `PATH`. One binary, every backend included.

| Platform | Archive |
|---|---|
| Linux x86_64 | `kulyrax-<version>-x86_64-unknown-linux-gnu.tar.gz` |
| macOS, Apple Silicon | `kulyrax-<version>-aarch64-apple-darwin.tar.gz` |
| macOS, Intel | `kulyrax-<version>-x86_64-apple-darwin.tar.gz` |
| Windows x86_64 | `kulyrax-<version>-x86_64-pc-windows-msvc.zip` |

Each archive has a `.sha256` file next to it. The binaries are not code-signed yet, so
macOS and Windows will ask you to confirm the first run.

**Or build from source** with Rust 1.89 or newer (no `protoc` install needed):

```sh
cargo install --git https://github.com/Kulyrax/kulyrax kulyrax-cli
```

## Start your own project

```sh
kulyrax init my-project
cd my-project
kulyrax run --config config.yaml --mappings mappings --once
```

`init` writes a config, a routing file, one mapping and two sample events, all
commented. It projects into JSON files under `./out` so it runs with no database;
the config shows how to add SQLite. It refuses to overwrite existing files.

## Quickstart

```sh
git clone https://github.com/Kulyrax/kulyrax.git
cd kulyrax
bash examples/sqlite-quickstart/run.sh
```

That's it — three events go in, a projected SQLite table comes out:

```
OK — app.db.users matches expected/users.txt:
u1|Ada|ada.lovelace@example.com
u2|Grace|grace@example.com
```

What just ran, unpacked (all under [`examples/sqlite-quickstart/`](examples/sqlite-quickstart/)):

```sh
kulyrax run --config config.yaml --mappings mappings --once
```

- **`config.yaml`** points at a `sqlite` adapter and a `directory` event source.
- **`mappings/sql/user_registered.yaml`** and **`user_email_changed.yaml`** map
  `event_type` → SQL columns, one file per event type.
- **`routing.yaml`** says which adapters each event type reaches.
- **`events/events.ndjson`** is the input — three NDJSON events.

**You create the tables, Kulyrax fills them.** Kulyrax never creates or migrates your
read-model schema; it only creates its own bookkeeping table (`conduit_projection_state`).
That's why `run.sh` calls `dbtool init` to create `users` before the run. Without it,
every event fails because the table doesn't exist.

Same event, three other storage kinds, one call each: `examples/postgres/` (a real
transactional Postgres backend), `examples/embedded/` (call `kulyrax-core` as a library,
no CLI), `examples/grpc-producer/` (stream events into a long-lived `kulyrax ingest`
service instead of reading a file).

## Changing a running project

Config, routing and mappings are read when a process starts. A long-running
`kulyrax run` (without `--once`) or `kulyrax ingest` picks up edits when you ask:

```sh
kulyrax reload --config config.yaml
```

The process re-reads and validates everything between batches, builds the new
adapters, and switches over. If anything is invalid it keeps running on the previous
configuration and `reload` prints why (exit 1). Changing `sources:` needs a restart.
Pass the same config file the process was started with.

## Why Kulyrax exists

A single event often needs to reach a SQL table (transactions), a document store (read
models), a key-value store (fast lookups), and a graph (relationships) — and that
fan-out logic tends to get duplicated across services, implemented inconsistently, and
be hard to reason about when something fails. Kulyrax centralizes it with deterministic
routing, explicit mappings, and a startup-time validation pass, so a redelivered or
out-of-order event is a clean, explainable skip rather than a duplicate write.

## Core principles

- **Event-first** — routing is by `event_type`, never content or metadata heuristics.
- **Deterministic** — no implicit behavior; the same event log always produces the same
  projected state.
- **Schema-explicit** — mappings are declared YAML, not inferred from payloads.
- **Fail fast** — bad config, unroutable events, and missing adapter capabilities are
  caught at startup, not mid-run.
- **Storage-agnostic** — adapters define *how* to write; mappings define *what*.

## How it compares

| Tool | What it does | Use it instead of Kulyrax when… |
|---|---|---|
| **Debezium** | Captures row changes from a database's log and publishes them as events. | You need to *produce* events from an existing database. It sits upstream of Kulyrax, not in its place. |
| **Kafka Connect** | Moves records between Kafka and other systems through per-connector sinks. | You are already on Kafka and an off-the-shelf sink does what you need. Kulyrax has no Kafka source yet ([roadmap](ROADMAP.md)). |
| **Materialize** | A streaming SQL database that keeps views over streams up to date inside itself. | You need joins or aggregates across streams, queried from Materialize. |
| **Kulyrax** | Projects each event into read models in *your* stores (SQL, document, key-value, graph) with one mapping format, one per-entity sequence guard against duplicates and reordering, and dependency order across targets. | You own the event log and want idempotent read models in several databases without writing projection code. |

## What Kulyrax is not

Not a streaming platform (no Kafka-alternative ambitions), not an ETL/ELT tool, not a
workflow engine, not a database, not a schema-inference system. It prefers explicitness
over convenience throughout.

## Learn more

- [`docs/concepts.md`](docs/concepts.md) — the mental model: the event envelope,
  `sequence` vs `position`, the guard, facets, `decide()`.
- [`docs/mapping-reference.md`](docs/mapping-reference.md) — every mapping field, per
  storage kind, with examples.
- [`architecture.md`](architecture.md) — the three-part structure (engine / sources /
  adapters), dependency rules, and open-source vs. product scope.
- [`docs/producer-contract.md`](docs/producer-contract.md) — the ordering/delivery
  assumptions a producer must satisfy.
- [`proto/README.md`](proto/README.md) — write a non-Rust producer against the gRPC
  ingestion contract.
- [`docs/writing-a-backend.md`](docs/writing-a-backend.md) — add a real database
  backend for a storage kind Kulyrax doesn't ship, without forking the crate.
- [`ROADMAP.md`](ROADMAP.md) — where things stand and what's still open.

## Contributing

Bug reports, mapping-reference corrections, and new backends are all welcome — see
[`CONTRIBUTING.md`](CONTRIBUTING.md) for dev setup and the PR process, and
[`docs/writing-a-backend.md`](docs/writing-a-backend.md) specifically for adding a
database backend.

## Status

Latest release notes: **[`RELEASES/v0.10.0.md`](RELEASES/v0.10.0.md)**.

Kulyrax is in an **alpha** phase — core architecture and execution semantics are stable,
but public APIs may still evolve. Intended for early feedback, not production
deployment.

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your
option.
