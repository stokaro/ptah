package assist_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/assist"
)

// converse runs the interactive surface over a scripted stdin and returns what
// it printed.
//
// Driving the command rather than the loop, because what these tests are about
// is the surface: which lines a person sees, what a directive does, and where
// the conversation was written.
func converse(c *qt.C, configPath, input string, args ...string) (stdout, stderr string, _ error) {
	c.Helper()
	clearEnv(c, "PTAH_ASSIST_PROFILE", "PTAH_ASSIST_MODEL",
		"OPENAI_API_KEY", "OPENAI_BASE_URL", "ANTHROPIC_API_KEY", "OLLAMA_HOST")
	c.Setenv("PTAH_ASSIST_CONFIG", configPath)
	// A conversation with no --workspace saves itself in the working directory,
	// which during a test is the package's own source directory. Moving first is
	// what keeps the suite from writing session files into the repository, which
	// is exactly what it did the first time it ran.
	c.Chdir(c.TempDir())

	cmd := assist.NewCommand()
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	cmd.SetIn(strings.NewReader(input))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), errOut.String(), err
}

// sessionFiles lists what was written under a project's session directory.
func sessionFiles(c *qt.C, root string) []string {
	c.Helper()
	entries, err := os.ReadDir(filepath.Join(root, ".ptah", "sessions"))
	if os.IsNotExist(err) {
		return nil
	}
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestChat_AnswersAndSavesTheConversation(t *testing.T) {
	c := qt.New(t)
	stub := &scripted{bodies: []string{
		toolAnswer("read_artifact", `{"artifact":"migrations"}`),
		textAnswer("One migration pair."),
	}}
	config := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, _, err := converse(c, config, "what migrations are there?\n/exit\n",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Ptah Assist. a-model via local.")
	c.Assert(out, qt.Contains, "One migration pair.")
	c.Assert(out, qt.Contains, "1 tool call(s)")

	files := sessionFiles(c, root)
	c.Assert(files, qt.HasLen, 1)
	records := readSession(c, filepath.Join(root, ".ptah", "sessions", files[0]))
	c.Assert(records[0]["type"], qt.Equals, "session")
	c.Assert(records[0]["model"], qt.Equals, "a-model")
	c.Assert(records[1]["type"], qt.Equals, "request")
	c.Assert(records[2]["type"], qt.Equals, "tool")
	c.Assert(records[2]["tool"], qt.Equals, "read_artifact")
	c.Assert(records[3]["type"], qt.Equals, "answer")
	c.Assert(records[3]["verified"], qt.IsTrue)
}

// readSession decodes a session file into its records.
func readSession(c *qt.C, path string) []map[string]any {
	c.Helper()
	raw, err := os.ReadFile(path) // #nosec G304 -- the path is this test's own temporary directory
	c.Assert(err, qt.IsNil)
	records := make([]map[string]any, 0)
	for line := range strings.SplitSeq(strings.TrimSuffix(string(raw), "\n"), "\n") {
		record := make(map[string]any)
		c.Assert(json.Unmarshal([]byte(line), &record), qt.IsNil)
		records = append(records, record)
	}
	return records
}

func TestChat_EphemeralWritesNothing(t *testing.T) {
	// The escape for a sensitive project. A conversation that still left a file
	// behind would make the flag a lie.
	c := qt.New(t)
	stub := &scripted{bodies: []string{textAnswer("nothing to see")}}
	config := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, _, err := converse(c, config, "hello\n/exit\n",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--ephemeral")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "nothing to see")
	c.Assert(out, qt.Not(qt.Contains), "Ptah is saving this conversation")
	c.Assert(sessionFiles(c, root), qt.HasLen, 0)
}

func TestChat_ResumeCarriesTheEarlierExchange(t *testing.T) {
	// What a resumed session is for. The tool results are deliberately not
	// replayed -- they described the project as it was -- so what reaches the
	// model is the conversation.
	c := qt.New(t)
	first := &scripted{bodies: []string{textAnswer("The table is called users.")}}
	config := configHome(c, profileFor(endpoint(c, first.handler())))
	root := workspace(c)

	out, _, err := converse(c, config, "what is the table called?\n/exit\n",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "The table is called users.")

	files := sessionFiles(c, root)
	c.Assert(files, qt.HasLen, 1)
	id := strings.TrimSuffix(files[0], ".jsonl")

	second := &scripted{bodies: []string{textAnswer("As I said, users.")}}
	config = configHome(c, profileFor(endpoint(c, second.handler())))
	out, _, err = converse(c, config, "say it again\n/exit\n",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--resume", id)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Continuing "+id+": 2 earlier message(s).")
	c.Assert(second.requests, qt.HasLen, 1)
	c.Assert(second.requests[0], qt.Contains, "what is the table called?")
	c.Assert(second.requests[0], qt.Contains, "The table is called users.")
	c.Assert(second.requests[0], qt.Not(qt.Contains), "1700000000_init.up.sql",
		qt.Commentf("tool results are not replayed; the model re-reads instead"))
}

func TestChat_DirectivesAreSlashPrefixed(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "help", input: "/help\n/exit\n", want: "/session   where this conversation"},
		{name: "tools", input: "/tools\n/exit\n", want: "describe_workspace"},
		{name: "session", input: "/session\n/exit\n", want: "Continue it later with: ptah assist --resume"},
		{name: "trace on", input: "/trace\n/exit\n", want: "tool trace on"},
		{name: "unknown", input: "/nope\n/exit\n", want: `"/nope" is not a command`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stub := &scripted{bodies: []string{textAnswer("unused")}}
			config := configHome(c, profileFor(endpoint(c, stub.handler())))
			root := workspace(c)

			out, _, err := converse(c, config, test.input,
				"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, test.want)
			c.Assert(stub.calls, qt.Equals, 0,
				qt.Commentf("a directive is not a question and must not reach the model"))
		})
	}
}

func TestChat_EndOfInputLeavesWithoutAnError(t *testing.T) {
	// Ctrl-D. A conversation that ended by the person closing stdin has not
	// failed, and reporting it as a failure would make every scripted run
	// non-zero.
	c := qt.New(t)
	stub := &scripted{bodies: []string{textAnswer("answered")}}
	config := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, _, err := converse(c, config, "a question\n",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "answered")
}

func TestChat_SaysWhenThereIsNoWorkspace(t *testing.T) {
	// Without one the model cannot change a file, and a person should not have
	// to infer that from the absence of a tool.
	c := qt.New(t)
	stub := &scripted{bodies: []string{textAnswer("hello")}}
	config := configHome(c, profileFor(endpoint(c, stub.handler())))

	out, _, err := converse(c, config, "/exit\n")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "No workspace")
	c.Assert(out, qt.Contains, "cannot change a file")
}

func TestChat_RefusesEphemeralAndResumeTogether(t *testing.T) {
	c := qt.New(t)
	stub := &scripted{bodies: []string{textAnswer("unused")}}
	config := configHome(c, profileFor(endpoint(c, stub.handler())))

	_, _, err := converse(c, config, "/exit\n", "--ephemeral", "--resume", "whatever")

	c.Assert(err, qt.ErrorMatches, `--ephemeral and --resume ask for opposite things.*`)
}
