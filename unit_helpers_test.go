package runlet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestLastEventIDHeaderQueryAndFallback(t *testing.T) {
	t.Run("uses header value when valid", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/jobs/abc/events?last_event_id=9", nil)
		req.Header.Set("Last-Event-ID", "7")
		if got := lastEventID(req); got != 7 {
			t.Fatalf("lastEventID() = %d, want 7", got)
		}
	})

	t.Run("falls back to query parameter", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/jobs/abc/events?last_event_id=11", nil)
		req.Header.Set("Last-Event-ID", "bad")
		if got := lastEventID(req); got != 11 {
			t.Fatalf("lastEventID() = %d, want 11", got)
		}
	})

	t.Run("returns zero for invalid values", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/jobs/abc/events?last_event_id=nope", nil)
		if got := lastEventID(req); got != 0 {
			t.Fatalf("lastEventID() = %d, want 0", got)
		}
	})
}

func TestWriteSSEFormatsAndSerializesPayload(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	event := EventRecord{
		Seq:  42,
		Kind: EventKindStdout,
		Payload: map[string]any{
			"line": "hello",
			"n":    3,
		},
	}

	if err := writeSSE(buffer, event); err != nil {
		t.Fatalf("writeSSE returned error: %v", err)
	}

	wantPrefix := "id: 42\nevent: stdout\ndata: "
	if got := buffer.String(); len(got) < len(wantPrefix) || got[:len(wantPrefix)] != wantPrefix {
		t.Fatalf("writeSSE output prefix mismatch: %q", got)
	}

	parts := bytes.SplitN(bytes.TrimSuffix(buffer.Bytes(), []byte("\n\n")), []byte("data: "), 2)
	if len(parts) != 2 {
		t.Fatalf("expected data field in SSE payload: %q", buffer.String())
	}

	decoded := map[string]any{}
	if err := json.Unmarshal(parts[1], &decoded); err != nil {
		t.Fatalf("failed to decode SSE data json: %v", err)
	}
	if decoded["line"] != "hello" {
		t.Fatalf("decoded payload line = %#v, want hello", decoded["line"])
	}
}

func TestStoreHistoryRespectsLimitAndClonesPayload(t *testing.T) {
	s := newStore()
	jobID := JobID("job-1")
	s.createJob(Job{JobID: jobID, Status: JobStatusQueued, CreatedAt: time.Now().UTC()})

	for seq := uint64(1); seq <= 3; seq++ {
		event := EventRecord{Seq: seq, TS: time.Now().UTC(), Kind: EventKindStdout, Payload: map[string]any{"line": "x"}}
		if _, err := s.appendEvent(jobID, event, 0); err != nil {
			t.Fatalf("appendEvent returned error: %v", err)
		}
	}

	events, err := s.eventHistory(jobID, 1, 1)
	if err != nil {
		t.Fatalf("eventHistory returned error: %v", err)
	}
	if len(events) != 1 || events[0].Seq != 2 {
		t.Fatalf("eventHistory got %+v, want only seq=2", events)
	}

	events[0].Payload["line"] = "mutated"
	again, err := s.eventHistory(jobID, 1, 1)
	if err != nil {
		t.Fatalf("eventHistory second call returned error: %v", err)
	}
	if again[0].Payload["line"] != "x" {
		t.Fatalf("payload was not cloned, got %#v", again[0].Payload["line"])
	}
}

func TestTruncateLinePreservesUTF8Boundary(t *testing.T) {
	input := []byte("😀😀")
	got := truncateLine(input, 5)
	if got != "😀" {
		t.Fatalf("truncateLine returned %q, want single emoji", got)
	}
}

func TestWaitForEventMatchAndErrors(t *testing.T) {
	t.Run("returns matching event", func(t *testing.T) {
		events := make(chan EventRecord, 2)
		events <- EventRecord{Kind: EventKindStdout}
		events <- EventRecord{Kind: EventKindJobCompleted}
		close(events)

		event, err := waitForEvent(context.Background(), events, func(e EventRecord) bool {
			return e.Kind == EventKindJobCompleted
		})
		if err != nil {
			t.Fatalf("waitForEvent returned error: %v", err)
		}
		if event.Kind != EventKindJobCompleted {
			t.Fatalf("waitForEvent returned %s, want job.completed", event.Kind)
		}
	})

	t.Run("returns context cancellation error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := waitForEvent(ctx, make(chan EventRecord), func(EventRecord) bool { return true })
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled error, got %v", err)
		}
	})

	t.Run("returns stream closed error", func(t *testing.T) {
		events := make(chan EventRecord)
		close(events)
		_, err := waitForEvent(context.Background(), events, func(EventRecord) bool { return true })
		if err == nil || err.Error() != "runlet: stream closed" {
			t.Fatalf("expected stream closed error, got %v", err)
		}
	})
}

func TestUpdateJobForEventSetsTerminalFields(t *testing.T) {
	job := Job{Status: JobStatusQueued}
	now := time.Now()

	updateJobForEvent(&job, EventRecord{TS: now, Kind: EventKindJobStarted, Payload: map[string]any{}})
	if job.Status != JobStatusRunning || job.StartedAt == nil {
		t.Fatalf("expected running state with started_at, got %+v", job)
	}

	updateJobForEvent(&job, EventRecord{TS: now.Add(time.Second), Kind: EventKindJobCompleted, Payload: map[string]any{"exit_code": float64(9)}})
	if job.Status != JobStatusCompleted || job.EndedAt == nil || job.ExitCode == nil || *job.ExitCode != 9 {
		t.Fatalf("expected completed state with exit code, got %+v", job)
	}
}

func TestBytesTrimSuffixAndTrimLineEnding(t *testing.T) {
	if got := string(bytesTrimSuffix([]byte("abc\n"), '\n')); got != "abc" {
		t.Fatalf("bytesTrimSuffix returned %q", got)
	}
	if got := string(trimLineEnding([]byte("abc\r\n"))); got != "abc" {
		t.Fatalf("trimLineEnding returned %q", got)
	}
}

func TestLastEventIDHandlesEscapedQuery(t *testing.T) {
	req := httptest.NewRequest("GET", "/jobs/abc/events", nil)
	req.URL.RawQuery = url.Values{"last_event_id": []string{"15"}}.Encode()
	if got := lastEventID(req); got != 15 {
		t.Fatalf("lastEventID() = %d, want 15", got)
	}
}
