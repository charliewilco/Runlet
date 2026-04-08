package runlet

type sequencer struct {
	in   chan eventInput
	done chan struct{}
}

func (r *Runlet) runSequencer(jobID JobID, seq *sequencer) {
	defer r.wg.Done()
	defer close(seq.done)
	defer r.removeSequencer(jobID)

	var nextSeq uint64
	for {
		select {
		case <-r.ctx.Done():
			r.store.closeSubscribers(jobID)
			return
		case input, ok := <-seq.in:
			if !ok {
				r.store.closeSubscribers(jobID)
				return
			}

			nextSeq++
			record := EventRecord{
				Seq:     nextSeq,
				TS:      input.ts.UTC(),
				Kind:    input.kind,
				Payload: clonePayload(input.payload),
			}

			subs, err := r.store.appendEvent(jobID, record, r.config.MaxEventsPerJob)
			if err != nil {
				r.store.closeSubscribers(jobID)
				return
			}

			broadcast(subs, record)
			if input.ack != nil {
				close(input.ack)
			}
			if record.Kind.terminal() {
				r.store.closeSubscribers(jobID)
				return
			}
		}
	}
}
