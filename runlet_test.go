package runlet

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

func TestCreateJobReturnsValidIDAndQueuedStatus(t *testing.T) {
	r := New(Config{})

	jobID, err := r.CreateJob(context.Background(), JobRequest{
		Cmd:  "bash",
		Args: []string{"-lc", "echo hello"},
	})
	if err != nil {
		t.Fatalf("CreateJob returned error: %v", err)
	}
	if _, err := ulid.Parse(string(jobID)); err != nil {
		t.Fatalf("job ID is not a valid ULID: %v", err)
	}

	job, err := r.Job(context.Background(), jobID)
	if err != nil {
		t.Fatalf("Job returned error: %v", err)
	}
	if job.Status != JobStatusQueued {
		t.Fatalf("expected queued status, got %s", job.Status)
	}

	events := waitForHistoryLength(t, r, jobID, 1, 2*time.Second)
	if events[0].Kind != EventKindJobQueued || events[0].Seq != 1 {
		t.Fatalf("unexpected queued event: %+v", events[0])
	}
}

func TestEventSequenceStrictlyMonotonicAcrossConcurrentJobs(t *testing.T) {
	r, cancel, _ := startRunlet(t)
	defer cancel()

	jobIDs := make([]JobID, 0, 4)
	for range 4 {
		jobID, err := r.CreateJob(context.Background(), JobRequest{
			Cmd:  "bash",
			Args: []string{"-lc", "echo one; echo two; echo three"},
		})
		if err != nil {
			t.Fatalf("CreateJob returned error: %v", err)
		}
		jobIDs = append(jobIDs, jobID)
	}

	for _, jobID := range jobIDs {
		waitForTerminalState(t, r, jobID, 5*time.Second)
		events, err := r.EventHistory(context.Background(), jobID, 0, 100)
		if err != nil {
			t.Fatalf("EventHistory returned error: %v", err)
		}
		for idx := 1; idx < len(events); idx++ {
			if events[idx-1].Seq >= events[idx].Seq {
				t.Fatalf("event sequence is not monotonic for %s: %+v", jobID, events)
			}
		}
	}
}

func TestJobCompletionEmitsCompletedTerminalEventWithExitCode(t *testing.T) {
	r, cancel, _ := startRunlet(t)
	defer cancel()

	jobID, err := r.CreateJob(context.Background(), JobRequest{
		Cmd:  "bash",
		Args: []string{"-lc", "exit 7"},
	})
	if err != nil {
		t.Fatalf("CreateJob returned error: %v", err)
	}

	terminal := waitForTerminalEvent(t, r, jobID, 5*time.Second)
	if terminal.Kind != EventKindJobCompleted {
		t.Fatalf("expected terminal event job.completed, got %s", terminal.Kind)
	}

	exitCode, ok := payloadInt(terminal.Payload, "exit_code")
	if !ok || exitCode != 7 {
		t.Fatalf("expected exit code 7, got %#v", terminal.Payload)
	}
}

func TestCancelQueuedJobEmitsCanceledWithoutSpawning(t *testing.T) {
	r := New(Config{})

	jobID, err := r.CreateJob(context.Background(), JobRequest{
		Cmd:  "bash",
		Args: []string{"-lc", "echo should-not-run"},
	})
	if err != nil {
		t.Fatalf("CreateJob returned error: %v", err)
	}

	if err := r.CancelJob(context.Background(), jobID); err != nil {
		t.Fatalf("CancelJob returned error: %v", err)
	}

	events := waitForTerminalHistory(t, r, jobID, 2*time.Second)
	if containsKind(events, EventKindJobStarted) {
		t.Fatalf("queued cancel should not start the process: %+v", events)
	}
	if events[len(events)-1].Kind != EventKindJobCanceled {
		t.Fatalf("expected terminal event job.canceled, got %s", events[len(events)-1].Kind)
	}
	assertKindsInOrder(t, events, []EventKind{
		EventKindJobQueued,
		EventKindJobCancelRequested,
		EventKindJobCanceled,
	})
}

func TestCancelRunningJobEmitsCancelRequestedThenCanceled(t *testing.T) {
	r, cancel, _ := startRunlet(t)
	defer cancel()

	jobID, err := r.CreateJob(context.Background(), JobRequest{
		Cmd:  "bash",
		Args: []string{"-lc", "sleep 10"},
	})
	if err != nil {
		t.Fatalf("CreateJob returned error: %v", err)
	}

	waitForEventKind(t, r, jobID, EventKindJobStarted, 5*time.Second)
	if err := r.CancelJob(context.Background(), jobID); err != nil {
		t.Fatalf("CancelJob returned error: %v", err)
	}

	events := waitForTerminalHistory(t, r, jobID, 5*time.Second)
	if events[len(events)-1].Kind != EventKindJobCanceled {
		t.Fatalf("expected terminal event job.canceled, got %s", events[len(events)-1].Kind)
	}
	assertKindsInOrder(t, events, []EventKind{
		EventKindJobCancelRequested,
		EventKindJobCanceled,
	})
}

func TestSSEReconnectWithLastEventIDReplaysOnlyLaterEvents(t *testing.T) {
	r, cancel, _ := startRunlet(t)
	defer cancel()

	server := httptest.NewServer(NewServer(r).Handler())
	defer server.Close()

	jobID, err := r.CreateJob(context.Background(), JobRequest{
		Cmd:  "bash",
		Args: []string{"-lc", "echo one; echo two; echo three"},
	})
	if err != nil {
		t.Fatalf("CreateJob returned error: %v", err)
	}

	waitForTerminalState(t, r, jobID, 5*time.Second)
	history, err := r.EventHistory(context.Background(), jobID, 0, 100)
	if err != nil {
		t.Fatalf("EventHistory returned error: %v", err)
	}
	if len(history) < 3 {
		t.Fatalf("expected at least three events, got %+v", history)
	}

	request, err := http.NewRequest(http.MethodGet, server.URL+"/jobs/"+string(jobID)+"/events", nil)
	if err != nil {
		t.Fatalf("NewRequest returned error: %v", err)
	}
	request.Header.Set("Last-Event-ID", "2")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("Do returned error: %v", err)
	}
	defer response.Body.Close()

	events := readAllSSEEvents(t, response.Body)
	for _, event := range events {
		if event.Seq <= 2 {
			t.Fatalf("received event at or before Last-Event-ID: %+v", events)
		}
	}
	if len(events) != len(history)-2 {
		t.Fatalf("unexpected replay count: got %d want %d", len(events), len(history)-2)
	}
}

func TestLongLinesAreTruncatedNotDropped(t *testing.T) {
	r, cancel, _ := startRunlet(t, Config{MaxLineBytes: 32})
	defer cancel()

	jobID, err := r.CreateJob(context.Background(), JobRequest{
		Cmd:  "bash",
		Args: []string{"-lc", "printf '%*s\\n' 128 '' | tr ' ' a"},
	})
	if err != nil {
		t.Fatalf("CreateJob returned error: %v", err)
	}

	stdout := waitForEventKind(t, r, jobID, EventKindStdout, 5*time.Second)
	line, ok := stdout.Payload["line"].(string)
	if !ok {
		t.Fatalf("stdout payload missing line: %+v", stdout.Payload)
	}
	if len(line) != 32 {
		t.Fatalf("expected truncated line length 32, got %d", len(line))
	}
}

func TestStartExitsCleanlyWhenContextIsCancelled(t *testing.T) {
	r := New(Config{})
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Start(ctx)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after context cancellation")
	}
}

func startRunlet(t *testing.T, configs ...Config) (*Runlet, context.CancelFunc, <-chan struct{}) {
	t.Helper()

	config := Config{}
	if len(configs) > 0 {
		config = configs[0]
	}

	r := New(config)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Start(ctx)
	}()
	return r, cancel, done
}

func waitForHistoryLength(t *testing.T, r *Runlet, jobID JobID, length int, timeout time.Duration) []EventRecord {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events, err := r.EventHistory(context.Background(), jobID, 0, 1000)
		if err == nil && len(events) >= length {
			return events
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for history length %d for job %s", length, jobID)
	return nil
}

func waitForTerminalState(t *testing.T, r *Runlet, jobID JobID, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := r.Job(context.Background(), jobID)
		if err == nil && job.terminal() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach terminal state", jobID)
}

func waitForTerminalHistory(t *testing.T, r *Runlet, jobID JobID, timeout time.Duration) []EventRecord {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events, err := r.EventHistory(context.Background(), jobID, 0, 1000)
		if err == nil && len(events) > 0 && events[len(events)-1].Kind.terminal() {
			return events
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for terminal history for job %s", jobID)
	return nil
}

func waitForTerminalEvent(t *testing.T, r *Runlet, jobID JobID, timeout time.Duration) EventRecord {
	t.Helper()
	return waitForHistoryEvent(t, r, jobID, timeout, func(event EventRecord) bool {
		return event.Kind.terminal()
	})
}

func waitForEventKind(t *testing.T, r *Runlet, jobID JobID, kind EventKind, timeout time.Duration) EventRecord {
	t.Helper()
	return waitForHistoryEvent(t, r, jobID, timeout, func(event EventRecord) bool {
		return event.Kind == kind
	})
}

func waitForHistoryEvent(t *testing.T, r *Runlet, jobID JobID, timeout time.Duration, matcher func(EventRecord) bool) EventRecord {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events, err := r.EventHistory(context.Background(), jobID, 0, 1000)
		if err == nil {
			for _, event := range events {
				if matcher(event) {
					return event
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for matching history event for job %s", jobID)
	return EventRecord{}
}

func containsKind(events []EventRecord, kind EventKind) bool {
	for _, event := range events {
		if event.Kind == kind {
			return true
		}
	}
	return false
}

func assertKindsInOrder(t *testing.T, events []EventRecord, expected []EventKind) {
	t.Helper()

	index := 0
	for _, event := range events {
		if index < len(expected) && event.Kind == expected[index] {
			index++
		}
	}
	if index != len(expected) {
		t.Fatalf("expected event kinds in order %v, got %+v", expected, events)
	}
}

type sseEvent struct {
	Seq   uint64
	Event string
	Data  json.RawMessage
}

func readAllSSEEvents(t *testing.T, body io.Reader) []sseEvent {
	t.Helper()

	reader := bufio.NewReader(body)
	events := make([]sseEvent, 0)
	for {
		event, err := readSSEEvent(reader)
		if err == io.EOF {
			return events
		}
		if err != nil {
			t.Fatalf("readSSEEvent returned error: %v", err)
		}
		events = append(events, event)
	}
}

func readSSEEvent(reader *bufio.Reader) (sseEvent, error) {
	var event sseEvent
	var sawField bool

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return sseEvent{}, err
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if sawField {
				return event, nil
			}
			if err == io.EOF {
				return sseEvent{}, io.EOF
			}
			continue
		}

		sawField = true
		switch {
		case strings.HasPrefix(line, "id: "):
			parsed, parseErr := strconv.ParseUint(strings.TrimPrefix(line, "id: "), 10, 64)
			if parseErr != nil {
				return sseEvent{}, parseErr
			}
			event.Seq = parsed
		case strings.HasPrefix(line, "event: "):
			event.Event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			event.Data = append(event.Data[:0], strings.TrimPrefix(line, "data: ")...)
		}

		if err == io.EOF {
			if sawField {
				return event, nil
			}
			return sseEvent{}, io.EOF
		}
	}
}

func mustDecodeJSON(t *testing.T, body io.Reader, target any) {
	t.Helper()
	if err := json.NewDecoder(body).Decode(target); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
}

func mustPostJSON(t *testing.T, url, payload string) *http.Response {
	t.Helper()

	response, err := http.Post(url, "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST %s failed: %v", url, err)
	}
	return response
}
