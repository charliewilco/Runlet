![](.github/header.png)

# Runlet

Runlet is a small Go service for running background jobs and streaming structured events over Server-Sent Events (SSE). It can be used as a library or run as a standalone binary.

This README documents the v1 behavior implemented in this repository.

## Features

- Start a job with a command, arguments, working directory, and environment.
- Subscribe to job events over SSE.
- Fetch event history for reconnects and debugging.
- Cancel running or queued jobs.
- Monotonic per-job event sequencing.
- In-memory storage with bounded history.

Non-goals for v1: auth, clustering, durable persistence, workflow graphs.

## Quick Start

Requirements:

- Go 1.22+

Run the server:

```bash
go run ./cmd/runlet serve --addr 127.0.0.1:8787
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

## Library

Runlet is importable as `github.com/charliewilco/runlet`.

```go
r := runlet.New(runlet.Config{
	MaxEventsPerJob: 10000,
	MaxLineBytes:    16384,
})

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

go r.Start(ctx)

jobID, err := r.CreateJob(ctx, runlet.JobRequest{
	Cmd:  "bash",
	Args: []string{"-lc", "echo hello"},
})

events, err := r.EventHistory(ctx, jobID, 0, 500)
err = r.CancelJob(ctx, jobID)
```

`Start` blocks until its context is canceled and all job goroutines exit, so callers should run it in a goroutine when using the library directly.

## HTTP API

Base URL: `http://127.0.0.1:8787`

### Create Job

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

### Get Job Status

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

### Stream Events

`GET /jobs/{job_id}/events`

- Response content type: `text/event-stream`
- Supports `Last-Event-ID` header
- Supports `last_event_id` query parameter

SSE format:

```text
id: <seq>
event: <kind>
data: <json>

```

### Fetch Event History

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

### Cancel Job

`POST /jobs/{job_id}/cancel`

Response:

```json
{ "job_id": "...", "status": "canceling" }
```

## Event Model

Events are stored as:

```go
type EventRecord struct {
	Seq     uint64
	TS      time.Time
	Kind    EventKind
	Payload map[string]any
}
```

Event kinds:

- `job.queued`
- `job.started`
- `stdout`
- `stderr`
- `job.completed`
- `job.failed`
- `job.canceled`
- `job.cancel_requested`

Rules:

- `job.queued` is emitted immediately on creation with `seq = 1`.
- `job.started` is emitted when the process spawns.
- The terminal event is one of `job.completed`, `job.failed`, or `job.canceled`.
- The SSE stream closes shortly after the terminal event.

## Execution Model

- Jobs run via `os/exec`.
- Stdout and stderr are read asynchronously in goroutines and split on `\n`.
- Output lines are truncated at `MaxLineBytes`.
- A per-job sequencer goroutine assigns monotonic `seq` values for events after creation.
- Events are fanned out to SSE subscribers with per-job channels protected by a mutex.

## Project Structure

```text
runlet/
  job.go
  event.go
  executor.go
  store.go
  sequencer.go
  sse.go
  server.go
  runlet.go
  cmd/
    runlet/
      main.go
  go.mod
  go.sum
  README.md
```

## Storage

In-memory only.

- Jobs are held in a `map[JobID]*jobState`.
- Event history is bounded per job by `MaxEventsPerJob`.
- Oldest events are dropped when the limit is exceeded.

Defaults:

- `MaxEventsPerJob = 10_000`
- `MaxLineBytes = 16_384`

## Cancellation Semantics

- `POST /jobs/{id}/cancel` emits `job.cancel_requested` immediately.
- If the job is still queued, it becomes `job.canceled` without spawning a process.
- If the job is running, Runlet sends `os.Interrupt`, waits 1500 ms, then sends `os.Kill`.

## CLI

Binary: `runlet`

```bash
runlet serve --addr 127.0.0.1:8787 --max-events-per-job 10000 --max-line-bytes 16384 --log-level info
```

Flags:

- `serve` subcommand
- `--addr 127.0.0.1:8787`
- `--max-events-per-job 10000`
- `--max-line-bytes 16384`
- `--log-level info`

## Tests

Run:

```bash
go test ./...
go vet ./...
```

Core tests cover:

- Job creation returns a valid job ID and `queued` status
- Monotonic sequencing across concurrent jobs
- Completion emits `job.completed` with the correct exit code
- Canceling a queued job emits `job.canceled` without spawning
- Canceling a running job emits `job.cancel_requested` then `job.canceled`
- SSE reconnects replay only events after `Last-Event-ID`
- Long output lines are truncated, not dropped
- `Start` exits cleanly when its context is canceled

## License

MIT
