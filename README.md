![](.github/header.png)

# Runlet

Runlet is a small Rust service for running background jobs (initially shell commands) and streaming structured events to clients over Server‑Sent Events (SSE). Clients can start jobs, watch live output/progress, and cancel jobs.

This README describes the v1 behavior implemented in this repo.

## Features

- Start a job (command + args) and receive a `job_id`.
- Subscribe to events for that job via SSE.
- Fetch event history for reconnects and debugging.
- Cancel running or queued jobs.
- Monotonic per‑job event sequencing (`seq`).
- In‑memory storage with bounded event history.

Non‑goals (v1): auth, clustering, durable persistence, workflow graphs.

## Quick start

Requirements:
- Rust (stable)
- macOS/Linux (Windows should work but is untested here)

Run the server:

```bash
cargo run -p runlet-server -- serve --addr 127.0.0.1:8787
```

Create a job:

```bash
curl -s -X POST http://127.0.0.1:8787/jobs \
  -H 'content-type: application/json' \
  -d '{"cmd":"bash","args":["-lc","echo hello && sleep 1 && echo world"]}'
```

Stream events:

```bash
curl -N http://127.0.0.1:8787/jobs/<JOB_ID>/events
```

Cancel a job:

```bash
curl -s -X POST http://127.0.0.1:8787/jobs/<JOB_ID>/cancel
```

Fetch history:

```bash
curl -s "http://127.0.0.1:8787/jobs/<JOB_ID>/events/history?after_seq=0&limit=500"
```

## API

Base URL: `http://127.0.0.1:8787`

### Create job

`POST /jobs`

Request:

```json
{
  "cmd": "bash",
  "args": ["-lc", "echo hello && sleep 1 && echo world"],
  "cwd": null,
  "env": null
}
```

Response:

```json
{ "job_id": "01HXYZ...", "status": "queued" }
```

### Get job status

`GET /jobs/{job_id}`

Response:

```json
{
  "job_id": "...",
  "status": "running",
  "created_at": "2026-01-26T23:00:00Z",
  "started_at": "2026-01-26T23:00:01Z",
  "ended_at": null,
  "exit_code": null
}
```

### Stream events (SSE)

`GET /jobs/{job_id}/events`

- Response `content-type: text/event-stream`
- Supports `Last-Event-ID` header for reconnects
- Optional query `last_event_id` also supported

SSE format:

```
id: <seq>
event: <kind>
data: <json>
```

Example:

```
id: 1
event: job.started
data: {"job_id":"...","ts":"..."}
```

### Fetch event history

`GET /jobs/{job_id}/events/history?after_seq=0&limit=500`

Response:

```json
{
  "job_id": "...",
  "events": [
    { "seq": 1, "ts": "...", "kind": "job.started", "payload": {} }
  ]
}
```

### Cancel job

`POST /jobs/{job_id}/cancel`

Response:

```json
{ "job_id": "...", "status": "canceling" }
```

## Event model

All events share this envelope in storage and SSE:

```rust
struct EventRecord {
  seq: u64,
  ts: DateTime<Utc>,
  kind: EventKind,
  payload: serde_json::Value
}
```

Event kinds (v1):

- `job.queued`
- `job.started`
- `stdout` (payload: `{ "line": "..." }`)
- `stderr` (payload: `{ "line": "..." }`)
- `job.completed` (payload: `{ "exit_code": i32 }`)
- `job.failed` (payload: `{ "message": "..." }`)
- `job.canceled`
- `job.cancel_requested`

Rules:
- `job.queued` is emitted immediately on creation with `seq=1`.
- `job.started` when process spawns.
- Terminal event is one of: `job.completed`, `job.failed`, `job.canceled`.
- After terminal, the SSE stream completes shortly after.

## Execution model

- Jobs run via `tokio::process::Command`.
- Stdout/stderr are read asynchronously and split on `\n`.
- A per‑job sequencer task assigns monotonic `seq` values and appends to a bounded history buffer.
- Events are fanned out to subscribers via `broadcast` channels.

## Storage

In‑memory (v1):

- `jobs: HashMap<JobId, JobState>`
- `events: VecDeque<EventRecord>` (bounded per job)

Defaults:

- `MAX_EVENTS_PER_JOB = 10_000`
- `MAX_LINE_BYTES = 16_384` (lines are truncated beyond this)

## Cancellation semantics

- `POST /jobs/{id}/cancel` emits `job.cancel_requested` immediately.
- If queued, executor emits `job.canceled` without spawning.
- If running:
  - Unix: SIGTERM, wait 1500ms, then SIGKILL if still alive.
  - Windows: best‑effort `kill()`.

## CLI

Binary: `runlet-server`

Flags:

- `serve` (optional subcommand)
- `--addr 127.0.0.1:8787`
- `--max-events-per-job 10000`
- `--max-line-bytes 16384`
- `--log-level info`

Examples:

```bash
cargo run -p runlet-server -- serve --addr 127.0.0.1:8787
cargo run -p runlet-server -- --addr 0.0.0.0:8787 --log-level debug
```

## Project structure

```
runlet/
  Cargo.toml
  crates/
    runlet-core/
      src/
        event.rs
        executor.rs
        job.rs
        store.rs
    runlet-server/
      src/
        main.rs
        http.rs
        routes.rs
        sse.rs
        state.rs
```

## Tests

Run all tests:

```bash
cargo test
```

Core tests include:

- Event sequencing monotonicity
- Job completion event flow
- Cancel flow emits `job.cancel_requested` and terminal `job.canceled`

## Notes / limitations

- No auth or multi‑tenant permissions in v1.
- In‑memory state only; restarts lose history.
- No command allowlist (runs arbitrary commands in local environment).

## License

MIT
