# mapreduce

MapReduce over gRPC prototype (Rust + `tonic`) with two run modes:

- `server` (master) — accepts worker connections via a bi-di stream and can send tasks.
- `client` (worker) — connects to the master, registers, and sends heartbeats.

## Requirements

- Rust toolchain
- `protoc` (protobuf compiler) in PATH (required to generate code from `proto/mapreduce.proto` during build)

## Build

```bash
cargo build
```

## Logging

Uses `tracing` + `tracing-subscriber`.

- Default log level: `info`
- Controlled via `RUST_LOG`

Examples:

```bash
RUST_LOG=info cargo run -- server
RUST_LOG=debug cargo run -- client
```

## Run server (master)

```bash
cargo run -- server --addr 127.0.0.1:50051
```

(`--addr` is optional, default: `127.0.0.1:50051`.)

## Run client (worker)

In another terminal:

```bash
cargo run -- client --addr 127.0.0.1:50051
```

## Example workflow

Terminal 1:

```bash
RUST_LOG=info cargo run -- server --addr 127.0.0.1:50051
```

Terminal 2:

```bash
RUST_LOG=info cargo run -- client --addr 127.0.0.1:50051
```

Expected behavior:

- the server logs the worker connection and inbound messages (`Register`, `Heartbeat`, `Result`)
- the client logs the connection and inbound messages from the master (`Task`, `Cancel`)
