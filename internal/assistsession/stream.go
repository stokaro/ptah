package assistsession

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// stamped is the half of a recorder that writes an already-timestamped record.
//
// A [Tee] decides the moment once and hands the same record to every sink. The
// interface is unexported because deciding a record's time is a privilege of
// the recording machinery: a caller that could set it could write a record that
// appears to belong to a different moment.
type stamped interface {
	appendStamped(record Record) error
}

// Stream writes records to an io.Writer as JSON lines.
//
// This is what `--format jsonl` prints: the same records the session file
// holds, in the same schema, carrying the same [SchemaVersion]. A consumer
// reading Ptah's stdout and a consumer reading a saved session are reading one
// format, so there is one thing to version and one thing to learn.
//
// It reports no identifier and no path because it is not a saved session. A
// [Tee] answers those from the sink that is one.
type Stream struct {
	out   io.Writer
	clock func() time.Time
}

// NewStream returns a recorder that writes JSON lines to out.
func NewStream(out io.Writer, clock func() time.Time) *Stream {
	if clock == nil {
		clock = time.Now
	}
	return &Stream{out: out, clock: clock}
}

// Append writes one record, stamping it with the stream's clock.
func (s *Stream) Append(record Record) error {
	record.At = s.clock().UTC()
	return s.appendStamped(record)
}

func (s *Stream) appendStamped(record Record) error {
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode the session record: %w", err)
	}
	if _, err := s.out.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write the session record: %w", err)
	}
	return nil
}

// ID reports nothing: a stream is not a session anyone can resume.
func (s *Stream) ID() string { return "" }

// Path reports nothing: a stream is not a file anyone can re-read.
func (s *Stream) Path() string { return "" }

// Close does nothing. The writer belongs to whoever supplied it -- closing
// stdout underneath a command that still has a summary to print would lose it.
func (s *Stream) Close() error { return nil }

// Tee records to several sinks at once.
//
// The record is stamped once and every sink writes that same record, so the
// copies are identical rather than merely equivalent. That is the property that
// makes `--format jsonl` worth having: what a person reads on stdout is what
// they will read back out of the session file, byte for byte.
type Tee struct {
	clock     func() time.Time
	recorders []Recorder
}

// NewTee returns a recorder that fans each record out to all of them.
//
// Identity and path come from the first sink that has one, which is the saved
// session rather than the stream.
func NewTee(clock func() time.Time, recorders ...Recorder) *Tee {
	if clock == nil {
		clock = time.Now
	}
	return &Tee{clock: clock, recorders: recorders}
}

// Append stamps the record once and writes it to every sink.
//
// Every sink is attempted even after one fails, and the errors are joined: a
// stdout that has gone away is not a reason to stop saving the conversation,
// and a full disk is not a reason to stop printing it.
func (t *Tee) Append(record Record) error {
	record.At = t.clock().UTC()
	errs := make([]error, 0, len(t.recorders))
	for _, recorder := range t.recorders {
		if sink, ok := recorder.(stamped); ok {
			errs = append(errs, sink.appendStamped(record))
			continue
		}
		errs = append(errs, recorder.Append(record))
	}
	return errors.Join(errs...)
}

// ID reports the first identifier any sink has.
func (t *Tee) ID() string {
	for _, recorder := range t.recorders {
		if id := recorder.ID(); id != "" {
			return id
		}
	}
	return ""
}

// Path reports the first path any sink has.
func (t *Tee) Path() string {
	for _, recorder := range t.recorders {
		if path := recorder.Path(); path != "" {
			return path
		}
	}
	return ""
}

// Close closes every sink, joining what they report.
func (t *Tee) Close() error {
	errs := make([]error, 0, len(t.recorders))
	for _, recorder := range t.recorders {
		errs = append(errs, recorder.Close())
	}
	return errors.Join(errs...)
}

// Begin writes a header to a recorder that has no session file behind it.
//
// [Store.Create] does this for a saved conversation. An ephemeral one still
// needs the header when it is being streamed, because a stream whose first line
// is a request cannot say which model answered or what schema it is in. It
// carries no identifier: there is nothing to resume.
func Begin(recorder Recorder, header Record) error {
	header.Kind = KindHeader
	header.SchemaVersion = SchemaVersion
	return recorder.Append(header)
}
