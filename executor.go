package runlet

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
	"unicode/utf8"
)

func (r *Runlet) runJob(jobID JobID, request JobRequest, control *jobControl) {
	defer r.wg.Done()
	defer r.removeControl(jobID)

	select {
	case <-r.ctx.Done():
		return
	case <-control.cancelCh:
		return
	case <-r.started:
	}

	if control.isTerminal() {
		return
	}

	cmd := exec.Command(request.Cmd, request.Args...)
	if request.Cwd != nil {
		cmd.Dir = *request.Cwd
	}
	if len(request.Env) > 0 {
		cmd.Env = os.Environ()
		for key, value := range request.Env {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", key, value))
		}
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		r.failJob(jobID, control, err)
		return
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		r.failJob(jobID, control, err)
		return
	}

	if control.isTerminal() {
		return
	}

	if err := cmd.Start(); err != nil {
		r.failJob(jobID, control, err)
		return
	}

	control.setStarted(cmd)
	_ = r.enqueueEvent(jobID, EventKindJobStarted, map[string]any{})

	var pipeWG sync.WaitGroup
	pipeWG.Add(2)
	r.wg.Add(2)
	go r.streamOutput(jobID, EventKindStdout, stdout, &pipeWG)
	go r.streamOutput(jobID, EventKindStderr, stderr, &pipeWG)

	waitDone := make(chan error, 1)
	processDone := make(chan struct{})
	go func() {
		waitDone <- cmd.Wait()
		close(processDone)
	}()

	canceling := false
	for {
		select {
		case err := <-waitDone:
			pipeWG.Wait()
			if canceling {
				if control.markTerminal() {
					_ = r.enqueueEvent(jobID, EventKindJobCanceled, map[string]any{})
				}
				return
			}

			if exitErr, ok := err.(*exec.ExitError); ok {
				if control.markTerminal() {
					_ = r.enqueueEvent(jobID, EventKindJobCompleted, map[string]any{
						"exit_code": exitErr.ExitCode(),
					})
				}
				return
			}
			if err != nil {
				r.failJob(jobID, control, err)
				return
			}
			if control.markTerminal() {
				_ = r.enqueueEvent(jobID, EventKindJobCompleted, map[string]any{
					"exit_code": 0,
				})
			}
			return
		case <-control.cancelCh:
			if canceling {
				continue
			}
			canceling = true
			r.wg.Add(1)
			go r.requestCancel(cmd.Process, processDone)
		case <-r.ctx.Done():
			if canceling {
				continue
			}
			canceling = true
			r.wg.Add(1)
			go r.requestCancel(cmd.Process, processDone)
		}
	}
}

func (r *Runlet) streamOutput(jobID JobID, kind EventKind, reader io.ReadCloser, pipeWG *sync.WaitGroup) {
	defer r.wg.Done()
	defer pipeWG.Done()
	defer reader.Close()

	buffered := bufio.NewReader(reader)
	for {
		line, err := buffered.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := trimLineEnding(line)
			_ = r.enqueueEvent(jobID, kind, map[string]any{
				"line": truncateLine(trimmed, r.config.MaxLineBytes),
			})
		}

		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			return
		}
	}
}

func (r *Runlet) requestCancel(process *os.Process, done <-chan struct{}) {
	defer r.wg.Done()

	if process == nil {
		return
	}

	_ = process.Signal(os.Interrupt)

	timer := time.NewTimer(1500 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-done:
		return
	case <-r.ctx.Done():
	case <-timer.C:
	}

	select {
	case <-done:
		return
	default:
		_ = process.Kill()
	}
}

func (r *Runlet) failJob(jobID JobID, control *jobControl, err error) {
	if !control.markTerminal() {
		return
	}
	_ = r.enqueueEvent(jobID, EventKindJobFailed, map[string]any{
		"message": err.Error(),
	})
}

func trimLineEnding(line []byte) []byte {
	line = bytesTrimSuffix(line, '\n')
	line = bytesTrimSuffix(line, '\r')
	return line
}

func bytesTrimSuffix(input []byte, suffix byte) []byte {
	if len(input) == 0 {
		return input
	}
	if input[len(input)-1] == suffix {
		return input[:len(input)-1]
	}
	return input
}

func truncateLine(line []byte, maxLineBytes int) string {
	if maxLineBytes <= 0 || len(line) <= maxLineBytes {
		return string(line)
	}

	truncated := line[:maxLineBytes]
	for len(truncated) > 0 && !utf8.Valid(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return string(truncated)
}

func waitForEvent(ctx context.Context, events <-chan EventRecord, matcher func(EventRecord) bool) (EventRecord, error) {
	for {
		select {
		case <-ctx.Done():
			return EventRecord{}, fmt.Errorf("runlet: %w", ctx.Err())
		case event, ok := <-events:
			if !ok {
				return EventRecord{}, fmt.Errorf("runlet: stream closed")
			}
			if matcher(event) {
				return event, nil
			}
		}
	}
}
