package aiprovider

import (
	"bufio"
	"io"
	"strings"
)

// maxSSEEventBytes bounds one event. It is generous because the limit exists to
// stop an endpoint from growing this process without end, not to police a size
// any real answer reaches.
const maxSSEEventBytes = 8 << 20

// sseEvent is one server-sent event, reduced to what both APIs use.
//
// The specification allows several fields; these two are what a streamed
// completion carries. An event with no data is a keep-alive and is skipped
// rather than delivered, because a consumer that had to recognize those would
// be reimplementing this reader.
type sseEvent struct {
	// Name is the `event:` field, empty when the stream does not send one.
	// OpenAI-compatible endpoints omit it; Anthropic sends one per event.
	Name string
	// Data is the `data:` payload, with continuation lines joined.
	Data string
}

// readSSE calls consume for each event, stopping at the end of the stream.
//
// Buffered per line rather than per read, because an event's payload is a
// complete JSON document and half of one cannot be parsed. The scanner buffer
// is raised past the default: a tool call's arguments arrive in one `data:`
// line on some endpoints, and a schema-shaped argument passes 64 KiB easily.
func readSSE(body io.Reader, consume func(sseEvent) error) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSSEEventBytes)

	event := sseEvent{}
	data := strings.Builder{}
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			// A blank line ends an event. One carrying no data is a comment or
			// a keep-alive, and there is nothing to deliver.
			if data.Len() > 0 {
				event.Data = data.String()
				if err := consume(event); err != nil {
					return err
				}
			}
			event = sseEvent{}
			data.Reset()
			continue
		}
		if strings.HasPrefix(line, ":") {
			// A comment. Some endpoints send these as keep-alives.
			continue
		}
		name, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		switch name {
		case "event":
			event.Name = value
		case "data":
			if data.Len() > 0 {
				// The specification joins several data lines with a newline.
				data.WriteByte('\n')
			}
			data.WriteString(value)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	// A stream that ended without a trailing blank line still has an event.
	if data.Len() > 0 {
		event.Data = data.String()
		return consume(event)
	}
	return nil
}
