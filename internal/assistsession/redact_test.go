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

// sessionProbePassword is the secret the arguments below carry. It is a word
// that appears nowhere else in this tree, so a match is the password rather
// than a coincidence.
const sessionProbePassword = "hunter2correcthorse"

func TestAppend_ToolArgumentsDoNotCarryAPassword(t *testing.T) {
	// read_database takes a connection URL as a tool argument, and a tool
	// argument is written to the session file, which outlives the process and
	// which people are told to read later. The password must not be in it.
	//
	// The existing property in internal/agentapi covers the other channel: a
	// connection error must not echo the password into the conversation. This
	// covers the one that reaches the disk.
	c := qt.New(t)
	out := &bytes.Buffer{}
	stream := assistsession.NewStream(out, clock(time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)))

	c.Assert(stream.Append(assistsession.Record{
		Kind: assistsession.KindTool,
		Tool: "read_database",
		Arguments: json.RawMessage(
			`{"database_url":"postgres://ptah_user:` + sessionProbePassword +
				`@db.internal:5432/app?sslmode=require","schemas":["public"]}`),
	}), qt.IsNil)

	written := out.String()
	c.Assert(written, qt.Not(qt.Contains), sessionProbePassword,
		qt.Commentf("the session file carried the password: %s", written))
	c.Assert(written, qt.Contains, "ptah_user",
		qt.Commentf("the account is worth reading back; only the password goes"))
	c.Assert(written, qt.Contains, "db.internal:5432")
	c.Assert(written, qt.Contains, "public")
}

func TestAppend_RedactionReachesEverySink(t *testing.T) {
	// The mirror is what --format jsonl prints. A redaction that only reached
	// the file would put the password on the terminal and in whatever collected
	// that output instead.
	c := qt.New(t)
	store, root := newStore(c)
	mirror := &bytes.Buffer{}
	recorder, err := store.Create("session-1", assistsession.Record{Model: "a-model"},
		assistsession.NewStream(mirror, nil))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(recorder.Close(), qt.IsNil) })

	c.Assert(recorder.Append(assistsession.Record{
		Kind:      assistsession.KindTool,
		Tool:      "read_database",
		Arguments: json.RawMessage(`{"database_url":"mysql://root:` + sessionProbePassword + `@127.0.0.1:3306/app"}`),
	}), qt.IsNil)

	saved, readErr := os.ReadFile(filepath.Join(root, ".ptah", "sessions", "session-1.jsonl")) // #nosec G304 -- this test's own temporary directory
	c.Assert(readErr, qt.IsNil)
	c.Assert(mirror.String(), qt.Not(qt.Contains), sessionProbePassword)
	c.Assert(string(saved), qt.Not(qt.Contains), sessionProbePassword)
}

func TestAppend_LeavesArgumentsThatAreNotURLsAlone(t *testing.T) {
	// A record that quietly rewrote the model's arguments would misreport what
	// Ptah was asked to do, so only a URL carrying a password is touched.
	tests := []struct {
		name string
		args string
	}{
		{name: "prose with an at sign", args: `{"question":"who owns user@example.com?"}`},
		{name: "a URL with no credential", args: `{"database_url":"postgres://db.internal:5432/app"}`},
		{name: "a username and no password", args: `{"database_url":"postgres://ptah_user@db:5432/app"}`},
		{name: "an email address", args: `{"path":"docs/a@b.sql"}`},
		{name: "not an object", args: `"a bare string"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out := &bytes.Buffer{}
			stream := assistsession.NewStream(out, clock(time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)))

			c.Assert(stream.Append(assistsession.Record{
				Kind:      assistsession.KindTool,
				Tool:      "read_artifact",
				Arguments: json.RawMessage(test.args),
			}), qt.IsNil)

			record := make(map[string]any)
			c.Assert(json.Unmarshal([]byte(strings.TrimSpace(out.String())), &record), qt.IsNil)
			encoded, err := json.Marshal(record["arguments"])
			c.Assert(err, qt.IsNil)
			c.Assert(json.Compact(&bytes.Buffer{}, encoded), qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.args)
		})
	}
}
