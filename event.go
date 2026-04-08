package runlet

import "time"

type EventKind string

const (
	EventKindJobQueued          EventKind = "job.queued"
	EventKindJobStarted         EventKind = "job.started"
	EventKindStdout             EventKind = "stdout"
	EventKindStderr             EventKind = "stderr"
	EventKindJobCompleted       EventKind = "job.completed"
	EventKindJobFailed          EventKind = "job.failed"
	EventKindJobCanceled        EventKind = "job.canceled"
	EventKindJobCancelRequested EventKind = "job.cancel_requested"
)

type EventRecord struct {
	Seq     uint64         `json:"seq"`
	TS      time.Time      `json:"ts"`
	Kind    EventKind      `json:"kind"`
	Payload map[string]any `json:"payload"`
}

func (k EventKind) terminal() bool {
	switch k {
	case EventKindJobCompleted, EventKindJobFailed, EventKindJobCanceled:
		return true
	default:
		return false
	}
}

type eventInput struct {
	ts      time.Time
	kind    EventKind
	payload map[string]any
	ack     chan struct{}
}
