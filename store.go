package runlet

import (
	"errors"
	"sync"
)

var errJobNotFound = errors.New("job not found")

type store struct {
	mu   sync.RWMutex
	jobs map[JobID]*jobState
}

type jobState struct {
	job    Job
	events []EventRecord
	subs   []chan EventRecord
}

func newStore() *store {
	return &store{
		jobs: make(map[JobID]*jobState),
	}
}

func (s *store) createJob(job Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[job.JobID] = &jobState{
		job:    cloneJob(job),
		events: make([]EventRecord, 0),
		subs:   make([]chan EventRecord, 0),
	}
}

func (s *store) getJob(jobID JobID) (Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state, ok := s.jobs[jobID]
	if !ok {
		return Job{}, false
	}

	return cloneJob(state.job), true
}

func (s *store) appendEvent(jobID JobID, event EventRecord, maxEvents int) ([]chan EventRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.jobs[jobID]
	if !ok {
		return nil, errJobNotFound
	}

	updateJobForEvent(&state.job, event)

	state.events = append(state.events, cloneEventRecord(event))
	if maxEvents > 0 && len(state.events) > maxEvents {
		trimmed := make([]EventRecord, maxEvents)
		copy(trimmed, state.events[len(state.events)-maxEvents:])
		state.events = trimmed
	}

	subs := make([]chan EventRecord, len(state.subs))
	copy(subs, state.subs)
	return subs, nil
}

func (s *store) eventHistory(jobID JobID, afterSeq uint64, limit int) ([]EventRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state, ok := s.jobs[jobID]
	if !ok {
		return nil, errJobNotFound
	}

	if limit <= 0 {
		limit = 500
	}

	events := make([]EventRecord, 0, min(limit, len(state.events)))
	for _, event := range state.events {
		if event.Seq <= afterSeq {
			continue
		}
		events = append(events, cloneEventRecord(event))
		if len(events) == limit {
			break
		}
	}

	return events, nil
}

func (s *store) subscribe(jobID JobID, buffer int) (chan EventRecord, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.jobs[jobID]
	if !ok {
		return nil, nil, errJobNotFound
	}

	ch := make(chan EventRecord, buffer)
	state.subs = append(state.subs, ch)

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()

			current, ok := s.jobs[jobID]
			if !ok {
				return
			}

			found := false
			for idx, sub := range current.subs {
				if sub != ch {
					continue
				}
				current.subs = append(current.subs[:idx], current.subs[idx+1:]...)
				found = true
				break
			}
			if found {
				close(ch)
			}
		})
	}

	return ch, unsubscribe, nil
}

func (s *store) closeSubscribers(jobID JobID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.jobs[jobID]
	if !ok {
		return
	}

	for _, sub := range state.subs {
		close(sub)
	}
	state.subs = nil
}

func updateJobForEvent(job *Job, event EventRecord) {
	switch event.Kind {
	case EventKindJobQueued:
		job.Status = JobStatusQueued
	case EventKindJobStarted:
		job.Status = JobStatusRunning
		ts := event.TS.UTC()
		job.StartedAt = &ts
	case EventKindJobCompleted:
		job.Status = JobStatusCompleted
		ts := event.TS.UTC()
		job.EndedAt = &ts
		if exitCode, ok := payloadInt(event.Payload, "exit_code"); ok {
			job.ExitCode = &exitCode
		}
	case EventKindJobFailed:
		job.Status = JobStatusFailed
		ts := event.TS.UTC()
		job.EndedAt = &ts
	case EventKindJobCanceled:
		job.Status = JobStatusCanceled
		ts := event.TS.UTC()
		job.EndedAt = &ts
	}
}

func cloneJob(job Job) Job {
	cloned := job
	if job.StartedAt != nil {
		started := job.StartedAt.UTC()
		cloned.StartedAt = &started
	}
	if job.EndedAt != nil {
		ended := job.EndedAt.UTC()
		cloned.EndedAt = &ended
	}
	if job.ExitCode != nil {
		code := *job.ExitCode
		cloned.ExitCode = &code
	}
	return cloned
}

func cloneEventRecord(event EventRecord) EventRecord {
	return EventRecord{
		Seq:     event.Seq,
		TS:      event.TS.UTC(),
		Kind:    event.Kind,
		Payload: clonePayload(event.Payload),
	}
}

func clonePayload(payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return map[string]any{}
	}

	cloned := make(map[string]any, len(payload))
	for key, value := range payload {
		cloned[key] = value
	}
	return cloned
}

func payloadInt(payload map[string]any, key string) (int, bool) {
	value, ok := payload[key]
	if !ok {
		return 0, false
	}

	switch typed := value.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
