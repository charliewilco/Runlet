package runlet

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

func broadcast(subs []chan EventRecord, event EventRecord) {
	for _, sub := range subs {
		select {
		case sub <- cloneEventRecord(event):
		default:
		}
	}
}

func writeSSE(w io.Writer, event EventRecord) error {
	payload, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("runlet: %w", err)
	}

	if _, err := fmt.Fprintf(
		w,
		"id: %d\nevent: %s\ndata: %s\n\n",
		event.Seq,
		event.Kind,
		payload,
	); err != nil {
		return fmt.Errorf("runlet: %w", err)
	}

	return nil
}

func lastEventID(request *http.Request) uint64 {
	if headerValue := request.Header.Get("Last-Event-ID"); headerValue != "" {
		if parsed, err := strconv.ParseUint(headerValue, 10, 64); err == nil {
			return parsed
		}
	}

	if queryValue := request.URL.Query().Get("last_event_id"); queryValue != "" {
		if parsed, err := strconv.ParseUint(queryValue, 10, 64); err == nil {
			return parsed
		}
	}

	return 0
}
