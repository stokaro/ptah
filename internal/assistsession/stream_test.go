package assistsession_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/assistsession"
)

func TestCreate_MirrorsEveryRecordByteForByte(t *testing.T) {
	// The property `--format jsonl` is worth having for: what a person reads on
	// stdout is what they will read back out of the session file. Two sinks
	// each stamping their own write would produce records that are equivalent
	// and not identical, and a consumer diffing the two would find a difference
	// that means nothing at all.
	c := qt.New(t)
	store, root := newStore(c)
	mirror := &bytes.Buffer{}

	recorder, err := store.Create("session-1",
		assistsession.Record{Model: "a-model", Provider: "local"},
		assistsession.NewStream(mirror, nil))
	c.Assert(err, qt.IsNil)
	c.Assert(recorder.Append(assistsession.Record{
		Kind: assistsession.KindRequest, Text: "what is here?",
	}), qt.IsNil)
	c.Assert(recorder.Append(assistsession.Record{
		Kind: assistsession.KindTool, Tool: "read_artifact", Result: "one pair",
	}), qt.IsNil)
	c.Assert(recorder.Close(), qt.IsNil)

	saved, err := os.ReadFile(filepath.Join(root, ".ptah", "sessions", "session-1.jsonl"))
	c.Assert(err, qt.IsNil)
	c.Assert(mirror.String(), qt.Equals, string(saved))
	c.Assert(strings.Count(mirror.String(), "\n"), qt.Equals, 3)
}

func TestCreate_TheMirrorCarriesTheHeader(t *testing.T) {
	// A stream whose first line is the request cannot say which model answered
	// or what schema version it is in, and a consumer would have to read the
	// session file to find out -- which is the thing the stream exists to avoid.
	c := qt.New(t)
	store, _ := newStore(c)
	mirror := &bytes.Buffer{}

	_, err := store.Create("session-1",
		assistsession.Record{Model: "a-model", Provider: "local"},
		assistsession.NewStream(mirror, nil))

	c.Assert(err, qt.IsNil)
	header := make(map[string]any)
	first, _, _ := strings.Cut(mirror.String(), "\n")
	c.Assert(json.Unmarshal([]byte(first), &header), qt.IsNil)
	c.Assert(header["type"], qt.Equals, "session")
	c.Assert(header["session_id"], qt.Equals, "session-1")
	c.Assert(header["model"], qt.Equals, "a-model")
	c.Assert(header["schema_version"], qt.Equals, float64(assistsession.SchemaVersion))
}

func TestCreate_IdentityComesFromTheSavedSessionNotTheStream(t *testing.T) {
	// `--resume` needs an identifier, and the stream has none. A tee that
	// answered from the wrong sink would print "continue it with --resume" and
	// no identifier to continue it by.
	c := qt.New(t)
	store, root := newStore(c)

	recorder, err := store.Create("session-1", assistsession.Record{Model: "a-model"},
		assistsession.NewStream(&bytes.Buffer{}, nil))

	c.Assert(err, qt.IsNil)
	c.Assert(recorder.ID(), qt.Equals, "session-1")
	c.Assert(recorder.Path(), qt.Equals,
		filepath.Join(root, ".ptah", "sessions", "session-1.jsonl"))
}

func TestStream_IsASessionNobodyCanResume(t *testing.T) {
	// The ephemeral shape: records on stdout and nothing on disk. It reports no
	// identifier because offering one would promise a `--resume` that has
	// nothing to read.
	c := qt.New(t)
	out := &bytes.Buffer{}
	stream := assistsession.NewStream(out, clock(time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)))

	c.Assert(stream.Append(assistsession.Record{
		Kind: assistsession.KindRequest, Text: "hello",
	}), qt.IsNil)
	c.Assert(stream.Close(), qt.IsNil)

	c.Assert(stream.ID(), qt.Equals, "")
	c.Assert(stream.Path(), qt.Equals, "")
	record := make(map[string]any)
	c.Assert(json.Unmarshal([]byte(strings.TrimSpace(out.String())), &record), qt.IsNil)
	c.Assert(record["type"], qt.Equals, "request")
	c.Assert(record["at"], qt.Equals, "2026-08-24T12:00:01Z")
}

func TestStream_CloseLeavesTheWriterOpen(t *testing.T) {
	// Closing stdout underneath a command that still has a summary to print
	// would lose the summary. The writer belongs to whoever supplied it.
	c := qt.New(t)
	out := &bytes.Buffer{}
	stream := assistsession.NewStream(out, nil)

	c.Assert(stream.Close(), qt.IsNil)
	c.Assert(stream.Append(assistsession.Record{Kind: assistsession.KindRequest, Text: "after"}),
		qt.IsNil)

	c.Assert(out.String(), qt.Contains, `"after"`)
}
