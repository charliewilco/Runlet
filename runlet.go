package runlet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

const (
	defaultMaxEventsPerJob = 10_000
	defaultMaxLineBytes    = 16_384
)

type Config struct {
	MaxEventsPerJob int
	MaxLineBytes    int
}

type JobRequest struct {
	Cmd  string            `json:"cmd"`
	Args []string          `json:"args"`
	Cwd  *string           `json:"cwd"`
	Env  map[string]string `json:"env"`
}

type Runlet struct {
	config Config
	store  *store

	ctx    context.Context
	cancel context.CancelFunc

	started   chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once

	mu         sync.Mutex
	sequencers map[JobID]*sequencer
	controls   map[JobID]*jobControl

	wg sync.WaitGroup
}

type jobControl struct {
	mu       sync.Mutex
	started  bool
	terminal bool
	command  any

	cancelCh   chan struct{}
	cancelOnce sync.Once
}

func New(config Config) *Runlet {
	if config.MaxEventsPerJob <= 0 {
		config.MaxEventsPerJob = defaultMaxEventsPerJob
	}
	if config.MaxLineBytes <= 0 {
		config.MaxLineBytes = defaultMaxLineBytes
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Runlet{
		config:     config,
		store:      newStore(),
		ctx:        ctx,
		cancel:     cancel,
		started:    make(chan struct{}),
		sequencers: make(map[JobID]*sequencer),
		controls:   make(map[JobID]*jobControl),
	}
}

func (r *Runlet) Start(ctx context.Context) {
	r.startOnce.Do(func() {
		close(r.started)
	})

	<-ctx.Done()

	r.stopOnce.Do(func() {
		r.cancel()
	})
	r.wg.Wait()
}

func (r *Runlet) CreateJob(ctx context.Context, request JobRequest) (JobID, error) {
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("runlet: %w", err)
	}
	if err := r.ctx.Err(); err != nil {
		return "", fmt.Errorf("runlet: %w", err)
	}

	jobID := JobID(ulid.Make().String())
	now := time.Now().UTC()
	job := Job{
		JobID:     jobID,
		Status:    JobStatusQueued,
		CreatedAt: now,
	}
	r.store.createJob(job)

	seq := &sequencer{
		in:   make(chan eventInput, 256),
		done: make(chan struct{}),
	}
	control := &jobControl{
		cancelCh: make(chan struct{}),
	}

	r.mu.Lock()
	r.sequencers[jobID] = seq
	r.controls[jobID] = control
	r.mu.Unlock()

	r.wg.Add(2)
	go r.runSequencer(jobID, seq)
	go r.runJob(jobID, request, control)

	if err := r.enqueueEventSync(jobID, EventKindJobQueued, map[string]any{}); err != nil {
		return "", err
	}

	return jobID, nil
}

func (r *Runlet) Job(ctx context.Context, jobID JobID) (Job, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, fmt.Errorf("runlet: %w", err)
	}

	job, ok := r.store.getJob(jobID)
	if !ok {
		return Job{}, fmt.Errorf("runlet: %w", errJobNotFound)
	}
	return job, nil
}

func (r *Runlet) EventHistory(ctx context.Context, jobID JobID, afterSeq uint64, limit int) ([]EventRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("runlet: %w", err)
	}

	events, err := r.store.eventHistory(jobID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("runlet: %w", err)
	}
	return events, nil
}

func (r *Runlet) CancelJob(ctx context.Context, jobID JobID) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("runlet: %w", err)
	}

	control, ok := r.getControl(jobID)
	if !ok {
		if _, exists := r.store.getJob(jobID); exists {
			return nil
		}
		return fmt.Errorf("runlet: %w", errJobNotFound)
	}

	if err := r.enqueueEvent(jobID, EventKindJobCancelRequested, map[string]any{}); err != nil && !errors.Is(err, errJobNotFound) {
		return err
	}

	if control.isTerminal() {
		return nil
	}

	if !control.isStarted() {
		if control.markTerminal() {
			if err := r.enqueueEvent(jobID, EventKindJobCanceled, map[string]any{}); err != nil && !errors.Is(err, errJobNotFound) {
				return err
			}
		}
		control.requestCancel()
		return nil
	}

	control.requestCancel()
	return nil
}

func (r *Runlet) subscribe(jobID JobID) (chan EventRecord, func(), error) {
	events, unsubscribe, err := r.store.subscribe(jobID, 256)
	if err != nil {
		return nil, nil, fmt.Errorf("runlet: %w", err)
	}
	return events, unsubscribe, nil
}

func (r *Runlet) enqueueEvent(jobID JobID, kind EventKind, payload map[string]any) error {
	return r.enqueue(jobID, kind, payload, false)
}

func (r *Runlet) enqueueEventSync(jobID JobID, kind EventKind, payload map[string]any) error {
	return r.enqueue(jobID, kind, payload, true)
}

func (r *Runlet) enqueue(jobID JobID, kind EventKind, payload map[string]any, wait bool) error {
	seq, ok := r.getSequencer(jobID)
	if !ok {
		return fmt.Errorf("runlet: %w", errJobNotFound)
	}

	var ack chan struct{}
	if wait {
		ack = make(chan struct{})
	}

	input := eventInput{
		ts:      time.Now().UTC(),
		kind:    kind,
		payload: clonePayload(payload),
		ack:     ack,
	}

	select {
	case <-r.ctx.Done():
		return fmt.Errorf("runlet: %w", r.ctx.Err())
	case <-seq.done:
		return nil
	case seq.in <- input:
	}

	if ack == nil {
		return nil
	}

	select {
	case <-r.ctx.Done():
		return fmt.Errorf("runlet: %w", r.ctx.Err())
	case <-seq.done:
		return nil
	case <-ack:
		return nil
	}
}

func (r *Runlet) getSequencer(jobID JobID) (*sequencer, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	seq, ok := r.sequencers[jobID]
	return seq, ok
}

func (r *Runlet) removeSequencer(jobID JobID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sequencers, jobID)
}

func (r *Runlet) getControl(jobID JobID) (*jobControl, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	control, ok := r.controls[jobID]
	return control, ok
}

func (r *Runlet) removeControl(jobID JobID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.controls, jobID)
}

func (c *jobControl) setStarted(command any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.started = true
	c.command = command
}

func (c *jobControl) isStarted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}

func (c *jobControl) markTerminal() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal {
		return false
	}
	c.terminal = true
	return true
}

func (c *jobControl) isTerminal() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.terminal
}

func (c *jobControl) requestCancel() {
	c.cancelOnce.Do(func() {
		close(c.cancelCh)
	})
}
