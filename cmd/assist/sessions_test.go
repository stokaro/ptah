package assist_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// recordOne holds one conversation in a project, and returns its identifier.
func recordOne(c *qt.C, root, question, answer string) string {
	c.Helper()
	stub := &scripted{bodies: []string{
		toolAnswer("read_artifact", `{"artifact":"migrations"}`),
		textAnswer(answer),
	}}
	config := configHome(c, profileFor(endpoint(c, stub.handler())))

	_, _, err := converse(c, config, question+"\n/exit\n",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres")
	c.Assert(err, qt.IsNil)

	files := sessionFiles(c, root)
	c.Assert(len(files) > 0, qt.IsTrue)
	return strings.TrimSuffix(files[len(files)-1], ".jsonl")
}

func TestSessionsList_ShowsWhatWasAsked(t *testing.T) {
	c := qt.New(t)
	root := workspace(c)
	id := recordOne(c, root, "what migrations are there?", "one pair")

	out, err := execute(c, configHome(c, profileFor("https://example.invalid/v1")),
		"sessions", "list", "--workspace", root)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, id)
	c.Assert(out, qt.Contains, "what migrations are there?")
	c.Assert(out, qt.Contains, "1 request(s), 1 tool call(s) via local")
	c.Assert(out, qt.Contains, "ptah assist --resume")
}

func TestSessionsList_SaysWhenThereAreNone(t *testing.T) {
	c := qt.New(t)

	out, err := execute(c, configHome(c, profileFor("https://example.invalid/v1")),
		"sessions", "list", "--workspace", c.TempDir())

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "No saved conversations in")
}

func TestSessionsShow_PrintsTheEvidenceAndTheAnswer(t *testing.T) {
	// The reason a session is worth keeping: the tool records say what Ptah
	// actually did, beside what the model said about it.
	c := qt.New(t)
	root := workspace(c)
	id := recordOne(c, root, "what is here?", "one migration pair")

	out, err := execute(c, configHome(c, profileFor("https://example.invalid/v1")),
		"sessions", "show", id, "--workspace", root)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "session "+id)
	c.Assert(out, qt.Contains, "model    a-model via local")
	c.Assert(out, qt.Contains, "> what is here?")
	c.Assert(out, qt.Contains, "ok       read_artifact")
	c.Assert(out, qt.Contains, "one migration pair")
	c.Assert(out, qt.Contains, "Ptah tools answered")
}

func TestSessionsShow_JSONIsTheRecordsThemselves(t *testing.T) {
	c := qt.New(t)
	root := workspace(c)
	id := recordOne(c, root, "what is here?", "one pair")

	out, err := execute(c, configHome(c, profileFor("https://example.invalid/v1")),
		"sessions", "show", id, "--workspace", root, "--format", "json")

	c.Assert(err, qt.IsNil)
	records := make([]map[string]any, 0)
	c.Assert(json.Unmarshal([]byte(out), &records), qt.IsNil)
	c.Assert(records[0]["type"], qt.Equals, "session")
	c.Assert(records[len(records)-1]["type"], qt.Equals, "answer")
}

func TestSessionsDelete_RemovesOne(t *testing.T) {
	c := qt.New(t)
	root := workspace(c)
	id := recordOne(c, root, "a question", "an answer")

	out, err := execute(c, configHome(c, profileFor("https://example.invalid/v1")),
		"sessions", "delete", id, "--workspace", root)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Deleted")
	_, statErr := os.Stat(filepath.Join(root, ".ptah", "sessions", id+".jsonl"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestSessionsPrune_KeepsWhatIsRecent(t *testing.T) {
	// The control on the prune window: a session written moments ago must
	// survive the default retention, or the command is a delete-everything with
	// a friendlier name.
	c := qt.New(t)
	root := workspace(c)
	id := recordOne(c, root, "a question", "an answer")

	out, err := execute(c, configHome(c, profileFor("https://example.invalid/v1")),
		"sessions", "prune", "--workspace", root)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Nothing older than")
	c.Assert(sessionFiles(c, root), qt.HasLen, 1)
	c.Assert(sessionFiles(c, root)[0], qt.Equals, id+".jsonl")
}

func TestSessions_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "a session that does not exist",
			args:    []string{"sessions", "show", "nope"},
			wantErr: `no such session: "nope"`,
		},
		{
			name:    "deleting one that does not exist",
			args:    []string{"sessions", "delete", "nope"},
			wantErr: `no such session: "nope"`,
		},
		{
			name:    "a prune window of zero",
			args:    []string{"sessions", "prune", "--older-than", "0s"},
			wantErr: `--older-than must be positive.*`,
		},
		{
			name:    "an unknown output format",
			args:    []string{"sessions", "list", "--format", "yaml"},
			wantErr: `--format: unknown format "yaml"; want text or json`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := workspace(c)
			args := make([]string, 0, len(test.args)+2)
			args = append(args, test.args...)
			args = append(args, "--workspace", root)

			_, err := execute(c, configHome(c, profileFor("https://example.invalid/v1")), args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
