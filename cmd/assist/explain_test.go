package assist_test

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/migrateops"
	"go.5x5.cz/ptah/migration/migrator"
)

// scripted answers each chat request with the next body, so a test can drive a
// tool call followed by an answer.
type scripted struct {
	bodies []string
	calls  int
}

func (s *scripted) handler() http.HandlerFunc {
	return func(writer http.ResponseWriter, _ *http.Request) {
		index := min(s.calls, len(s.bodies)-1)
		s.calls++
		_, _ = writer.Write([]byte(s.bodies[index]))
	}
}

// textAnswer is a chat completion carrying prose.
func textAnswer(text string) string {
	encoded, _ := json.Marshal(text)
	return `{"id":"c1","model":"a-model","choices":[{"finish_reason":"stop",` +
		`"message":{"role":"assistant","content":` + string(encoded) + `}}]}`
}

// toolAnswer is a chat completion asking for one tool.
func toolAnswer(name, arguments string) string {
	encoded, _ := json.Marshal(arguments)
	return `{"id":"c1","model":"a-model","choices":[{"finish_reason":"tool_calls",` +
		`"message":{"role":"assistant","tool_calls":[{"id":"t1","type":"function",` +
		`"function":{"name":"` + name + `","arguments":` + string(encoded) + `}}]}}]}`
}

// workspace builds a project with one hashed migration pair.
func workspace(c *qt.C) string {
	c.Helper()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1700000000_init.up.sql"),
		[]byte("CREATE TABLE users (id BIGINT PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1700000000_init.down.sql"),
		[]byte("DROP TABLE users;\n"), 0o600), qt.IsNil)
	_, err := migrateops.Rehash(dir, migrator.MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)
	return root
}

func TestExplain_HappyPath(t *testing.T) {
	c := qt.New(t)
	stub := &scripted{bodies: []string{
		toolAnswer("ptah_read_artifact", `{"artifact":"migrations"}`),
		textAnswer("There is one migration pair, 1700000000_init."),
	}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, err := execute(c, home, "explain",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--trace", "what migrations are there?")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "There is one migration pair")
	c.Assert(out, qt.Contains, "ok       ptah_read_artifact")
	c.Assert(out, qt.Contains, `"artifact":"migrations"`,
		qt.Commentf("the trace shows the first line of what Ptah answered"))
	c.Assert(out, qt.Contains, "-- a-model via local, 2 turn(s), 1 tool call(s), answer")
}

func TestExplain_SaysWhenNothingWasChecked(t *testing.T) {
	// An answer with no tool behind it looks exactly like one Ptah verified.
	// The difference is the whole question a reader has.
	c := qt.New(t)
	stub := &scripted{bodies: []string{textAnswer("Databases usually have a users table.")}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))

	out, err := execute(c, home, "explain", "what is in this project?")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Databases usually have a users table.")
	c.Assert(out, qt.Contains, "No Ptah tool answered, so nothing above was checked")
}

func TestExplain_JSONCarriesTheEvidenceAlongsideTheAnswer(t *testing.T) {
	c := qt.New(t)
	stub := &scripted{bodies: []string{
		toolAnswer("ptah_describe_workspace", `{}`),
		textAnswer("One artifact class is configured."),
	}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, err := execute(c, home, "explain",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--format", "json", "what can you reach?")

	c.Assert(err, qt.IsNil)
	report := make(map[string]any)
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil)
	c.Assert(report["answer"], qt.Equals, "One artifact class is configured.")
	c.Assert(report["verified"], qt.IsTrue)
	c.Assert(report["model"], qt.Equals, "a-model")
	c.Assert(report["stop_reason"], qt.Equals, "answer")

	tools, _ := report["tools"].([]any)
	c.Assert(tools, qt.HasLen, 1)
	first, _ := tools[0].(map[string]any)
	c.Assert(first["name"], qt.Equals, "ptah_describe_workspace")
	c.Assert(first["failed"], qt.IsFalse)
}

func TestExplain_AWriteIsRefusedWhenTheOperatorEnabledNone(t *testing.T) {
	// The model may ask; the broker decides. The refusal reaches the model as a
	// tool result, and the run continues rather than ending.
	c := qt.New(t)
	stub := &scripted{bodies: []string{
		toolAnswer("ptah_preview_patch", `{"artifact":"migrations","changes":[`+
			`{"path":"1700000100_x.up.sql","operation":"create","content":"SELECT 1;\n"}]}`),
		textAnswer("I previewed it; applying is not permitted in this session."),
	}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, err := execute(c, home, "explain",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--format", "json", "add a migration")

	c.Assert(err, qt.IsNil)
	report := make(map[string]any)
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil)
	tools, _ := report["tools"].([]any)
	c.Assert(tools, qt.HasLen, 1)
	first, _ := tools[0].(map[string]any)
	c.Assert(first["name"], qt.Equals, "ptah_preview_patch")
	c.Assert(first["failed"], qt.IsFalse,
		qt.Commentf("previewing is allowed; it is applying that is not"))
	c.Assert(filepath.Join(root, "migrations", "1700000100_x.up.sql"), qt.Not(qt.Equals), "")

	_, statErr := os.Stat(filepath.Join(root, "migrations", "1700000100_x.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestExplain_StopsAtTheToolCallLimitAndStillReports(t *testing.T) {
	// A run that hit a limit produced a record worth printing, so the document
	// goes out and the outcome is the exit code.
	c := qt.New(t)
	stub := &scripted{bodies: []string{toolAnswer("ptah_describe_workspace", `{}`)}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, err := execute(c, home, "explain",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--max-tool-calls", "1", "--format", "json", "keep going")

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 2), qt.Equals, 1)
	report := make(map[string]any)
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil)
	c.Assert(report["stop_reason"], qt.Equals, "tool call limit")
}

func TestExplain_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "an unknown output format",
			args:    []string{"explain", "--format", "yaml", "hello"},
			wantErr: `--format: unknown format "yaml"; want text or json`,
		},
		{
			name:    "a profile that does not exist",
			args:    []string{"explain", "--provider-profile", "nope", "hello"},
			wantErr: `unknown provider profile "nope".*`,
		},
		{
			name:    "a workspace with no artifact directory",
			args:    []string{"explain", "--workspace", ".", "hello"},
			wantErr: `(?s)--workspace needs at least one of.*`,
		},
		{
			name: "a workspace with no dialect",
			args: []string{
				"explain", "--workspace", ".", "--migrations-dir", "./migrations", "hello",
			},
			wantErr: `--dialect is required with --workspace.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stub := &scripted{bodies: []string{textAnswer("ok")}}
			home := configHome(c, profileFor(endpoint(c, stub.handler())))

			_, err := execute(c, home, test.args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestExplain_KeepsTheDocumentOnStdoutAndTheNarrationOnStderr(t *testing.T) {
	// The tree's contract for a machine-readable command. A progress line
	// printed into the middle of the JSON would make the document unparsable
	// for exactly the callers the format exists for.
	c := qt.New(t)
	stub := &scripted{bodies: []string{
		toolAnswer("ptah_describe_workspace", `{}`),
		textAnswer("done"),
	}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, errOut, err := executeStreams(c, home, "explain",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--trace", "--format", "json", "what can you reach?")

	c.Assert(err, qt.IsNil)
	report := make(map[string]any)
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil)
	c.Assert(errOut, qt.Contains, "recording agent decisions to")
	c.Assert(errOut, qt.Contains, "tool: ptah_describe_workspace")
	c.Assert(out, qt.Not(qt.Contains), "recording agent decisions")
}
