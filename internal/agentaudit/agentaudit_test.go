package agentaudit_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentaudit"
)

// fixedClock pins the timestamp so a record can be compared byte for byte.
func fixedClock() func() time.Time {
	stamp := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	return func() time.Time { return stamp }
}

// newWriter builds a writer over a buffer with the session fields set.
func newWriter(c *qt.C, out *strings.Builder) *agentaudit.Writer {
	c.Helper()
	writer, err := agentaudit.NewWriter(out, agentaudit.Options{
		SessionID:   "session-1",
		Surface:     agentaudit.SurfaceMCP,
		PtahVersion: "test",
		Clock:       fixedClock(),
	})
	c.Assert(err, qt.IsNil)
	return writer
}

// decode reads the events a writer emitted.
func decode(c *qt.C, raw string) []map[string]any {
	c.Helper()
	events := make([]map[string]any, 0)
	for line := range strings.SplitSeq(strings.TrimSuffix(raw, "\n"), "\n") {
		var event map[string]any
		c.Assert(json.Unmarshal([]byte(line), &event), qt.IsNil)
		events = append(events, event)
	}
	return events
}

func TestRecord_HappyPath(t *testing.T) {
	c := qt.New(t)
	out := &strings.Builder{}
	writer := newWriter(c, out)

	err := writer.Record(agentaudit.Event{
		Operation:    "apply_patch",
		Capability:   "artifact.write:migrations",
		Verdict:      "ask",
		DecidedBy:    "builtin",
		Approved:     true,
		Outcome:      agentaudit.OutcomeApproved,
		Artifact:     "migrations",
		Paths:        []string{"1700000100_add_status.up.sql"},
		PatchID:      "sha256:aa",
		BaseDigest:   "sha256:bb",
		ResultDigest: "sha256:cc",
		Gates:        []string{"migration-integrity", "migration-sql"},
	})

	c.Assert(err, qt.IsNil)
	events := decode(c, out.String())
	c.Assert(events, qt.HasLen, 1)
	c.Assert(events[0]["schema_version"], qt.Equals, float64(agentaudit.SchemaVersion))
	c.Assert(events[0]["session_id"], qt.Equals, "session-1")
	c.Assert(events[0]["surface"], qt.Equals, "mcp")
	c.Assert(events[0]["ptah_version"], qt.Equals, "test")
	c.Assert(events[0]["timestamp"], qt.Equals, "2026-08-23T12:00:00Z")
	c.Assert(events[0]["outcome"], qt.Equals, "approved")
	c.Assert(events[0]["capability"], qt.Equals, "artifact.write:migrations")
}

func TestRecord_KeepsTheSessionFieldsAwayFromTheCaller(t *testing.T) {
	// A caller that could set the session could produce a record attributing
	// its own decision to a different run.
	c := qt.New(t)
	out := &strings.Builder{}
	writer := newWriter(c, out)

	err := writer.Record(agentaudit.Event{
		Operation:   "read_artifact",
		Outcome:     agentaudit.OutcomePermitted,
		SessionID:   "somebody-elses-session",
		Surface:     agentaudit.SurfaceAssist,
		PtahVersion: "1.0.0-forged",
		Timestamp:   time.Date(1999, time.January, 1, 0, 0, 0, 0, time.UTC),
	})

	c.Assert(err, qt.IsNil)
	events := decode(c, out.String())
	c.Assert(events[0]["session_id"], qt.Equals, "session-1")
	c.Assert(events[0]["surface"], qt.Equals, "mcp")
	c.Assert(events[0]["ptah_version"], qt.Equals, "test")
	c.Assert(events[0]["timestamp"], qt.Equals, "2026-08-23T12:00:00Z")
}

func TestRecord_RefusalsAreRecordedToo(t *testing.T) {
	// The hostile-repository scenario asks for a record showing the denied
	// capability requests. A log written on the success path only would show a
	// clean session for exactly the run worth looking at.
	c := qt.New(t)
	out := &strings.Builder{}
	writer := newWriter(c, out)

	c.Assert(writer.Record(agentaudit.Event{
		Operation:  "apply_patch",
		Capability: "artifact.write:migrations",
		Verdict:    "deny",
		DecidedBy:  "invocation",
		Outcome:    agentaudit.OutcomeDenied,
		Reason:     `"artifact.write:migrations" denied by invocation policy`,
	}), qt.IsNil)
	c.Assert(writer.Record(agentaudit.Event{
		Operation:  "read_artifact",
		Capability: "filesystem.arbitrary_read",
		Verdict:    "deny",
		DecidedBy:  "builtin",
		Outcome:    agentaudit.OutcomeDenied,
	}), qt.IsNil)

	events := decode(c, out.String())
	c.Assert(events, qt.HasLen, 2)
	c.Assert(events[0]["outcome"], qt.Equals, "denied")
	c.Assert(events[1]["capability"], qt.Equals, "filesystem.arbitrary_read")
}

func TestRecord_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		event   agentaudit.Event
		wantErr string
	}{
		{
			name:    "no operation",
			event:   agentaudit.Event{Outcome: agentaudit.OutcomePermitted},
			wantErr: "audit event names no operation",
		},
		{
			name:    "no outcome",
			event:   agentaudit.Event{Operation: "apply_patch"},
			wantErr: "audit event states no outcome",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out := &strings.Builder{}
			writer := newWriter(c, out)

			err := writer.Record(test.event)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(out.String(), qt.Equals, "")
		})
	}
}

func TestNewWriter_RequiresASession(t *testing.T) {
	c := qt.New(t)

	writer, err := agentaudit.NewWriter(&strings.Builder{}, agentaudit.Options{})

	c.Assert(err, qt.ErrorMatches, "audit writer requires a session id")
	c.Assert(writer, qt.IsNil)
}

func TestOpenFile_AppendsAndProtectsTheLog(t *testing.T) {
	c := qt.New(t)
	path := agentaudit.DefaultPath(c.TempDir())

	first, err := agentaudit.OpenFile(path, agentaudit.Options{
		SessionID: "session-1", Surface: agentaudit.SurfaceAssist, Clock: fixedClock(),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(first.Record(agentaudit.Event{
		Operation: "read_artifact", Outcome: agentaudit.OutcomePermitted,
	}), qt.IsNil)
	c.Assert(first.Close(), qt.IsNil)
	c.Assert(first.Close(), qt.IsNil)

	second, err := agentaudit.OpenFile(path, agentaudit.Options{
		SessionID: "session-2", Surface: agentaudit.SurfaceAssist, Clock: fixedClock(),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(second.Record(agentaudit.Event{
		Operation: "apply_patch", Outcome: agentaudit.OutcomeFailed,
	}), qt.IsNil)
	c.Assert(second.Close(), qt.IsNil)

	raw, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	events := decode(c, string(raw))
	c.Assert(events, qt.HasLen, 2)
	c.Assert(events[0]["session_id"], qt.Equals, "session-1")
	c.Assert(events[1]["session_id"], qt.Equals, "session-2")

	info, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm()&0o077, qt.Equals, os.FileMode(0))

	dir, err := os.Stat(filepath.Dir(path))
	c.Assert(err, qt.IsNil)
	c.Assert(dir.Mode().Perm()&0o077, qt.Equals, os.FileMode(0))
}

func TestDiscard_KeepsNothing(t *testing.T) {
	c := qt.New(t)

	c.Assert(agentaudit.Discard{}.Record(agentaudit.Event{}), qt.IsNil)
}
