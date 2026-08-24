package assist_test

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// decodeJSONL parses every line of a record stream.
func decodeJSONL(c *qt.C, raw string) []map[string]any {
	c.Helper()
	records := make([]map[string]any, 0)
	for line := range strings.SplitSeq(strings.TrimSuffix(raw, "\n"), "\n") {
		record := make(map[string]any)
		c.Assert(json.Unmarshal([]byte(line), &record), qt.IsNil,
			qt.Commentf("every line of --format jsonl has to be one JSON object: %q", line))
		records = append(records, record)
	}
	return records
}

// kindsOf names each record in order.
func kindsOf(records []map[string]any) []string {
	kinds := make([]string, 0, len(records))
	for _, record := range records {
		kinds = append(kinds, record["type"].(string))
	}
	return kinds
}

func TestExplain_JSONLIsOneRecordPerLine(t *testing.T) {
	c := qt.New(t)
	stub := &scripted{bodies: []string{
		toolAnswer("read_artifact", `{"artifact":"migrations"}`),
		textAnswer("One migration pair."),
	}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, _, err := executeStreams(c, home, "explain", "what is here?",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--format", "jsonl")

	c.Assert(err, qt.IsNil)
	records := decodeJSONL(c, out)
	c.Assert(kindsOf(records), qt.DeepEquals, []string{"session", "request", "tool", "answer"})
	c.Assert(records[0]["schema_version"], qt.Equals, float64(1))
	c.Assert(records[0]["model"], qt.Equals, "a-model")
	c.Assert(records[1]["text"], qt.Equals, "what is here?")
	c.Assert(records[2]["tool"], qt.Equals, "read_artifact")
	c.Assert(records[3]["verified"], qt.IsTrue)
}

func TestExplain_JSONLIsTheSavedSessionByteForByte(t *testing.T) {
	// The reason this format is the session's own records rather than a second
	// document: what a consumer reads on stdout is what they will read back out
	// of the file, so there is one schema to version and one to learn.
	c := qt.New(t)
	stub := &scripted{bodies: []string{
		toolAnswer("read_artifact", `{"artifact":"migrations"}`),
		textAnswer("One migration pair."),
	}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, _, err := executeStreams(c, home, "explain", "what is here?",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--format", "jsonl")

	c.Assert(err, qt.IsNil)
	files := sessionFiles(c, root)
	c.Assert(files, qt.HasLen, 1)
	saved, readErr := os.ReadFile(filepath.Join(root, ".ptah", "sessions", files[0])) // #nosec G304 -- this test's own temporary directory
	c.Assert(readErr, qt.IsNil)
	c.Assert(out, qt.Equals, string(saved))
}

func TestExplain_JSONLKeepsStdoutToRecordsAlone(t *testing.T) {
	// A summary printed among the records would break every consumer that reads
	// this a line at a time, which is the only reason to choose the format.
	c := qt.New(t)
	stub := &scripted{bodies: []string{textAnswer("nothing to report")}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, errOut, err := executeStreams(c, home, "explain", "anything?",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--format", "jsonl")

	c.Assert(err, qt.IsNil)
	c.Assert(kindsOf(decodeJSONL(c, out)), qt.DeepEquals, []string{"session", "request", "answer"})
	c.Assert(errOut, qt.Contains, "Session ")
	c.Assert(errOut, qt.Contains, "--resume")
}

func TestExplain_JSONLWithEphemeralKeepsNoFile(t *testing.T) {
	// The shape a CI job wants: machine-readable output, and nothing left in
	// the checkout afterwards.
	c := qt.New(t)
	stub := &scripted{bodies: []string{textAnswer("nothing kept")}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, _, err := executeStreams(c, home, "explain", "anything?",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--format", "jsonl", "--ephemeral")

	c.Assert(err, qt.IsNil)
	records := decodeJSONL(c, out)
	c.Assert(kindsOf(records), qt.DeepEquals, []string{"session", "request", "answer"})
	c.Assert(records[0]["session_id"], qt.IsNil,
		qt.Commentf("an ephemeral stream has no session to resume, so it claims no identifier"))
	c.Assert(sessionFiles(c, root), qt.HasLen, 0)
}

// sessionText is everything the project's session files hold right now.
func sessionText(c *qt.C, root string) string {
	c.Helper()
	whole := ""
	for _, name := range sessionFiles(c, root) {
		raw, err := os.ReadFile(filepath.Join(root, ".ptah", "sessions", name)) // #nosec G304 -- this test's own temporary directory
		c.Assert(err, qt.IsNil)
		whole += string(raw)
	}
	return whole
}

func TestExplain_RecordsReachDiskBeforeTheAnswerDoes(t *testing.T) {
	// What writing records as the run happens buys: a process killed mid-run
	// leaves the question and every tool that had already answered, rather than
	// nothing at all.
	//
	// The session file is read on every model call, so the second reading is
	// the state after the first tool completed and before any answer exists.
	c := qt.New(t)
	root := workspace(c)
	stub := &scripted{bodies: []string{
		toolAnswer("read_artifact", `{"artifact":"migrations"}`),
		textAnswer("done"),
	}}
	seen := make([]string, 0, 2)
	answer := stub.handler()
	handler := func(writer http.ResponseWriter, request *http.Request) {
		seen = append(seen, sessionText(c, root))
		answer(writer, request)
	}
	home := configHome(c, profileFor(endpoint(c, handler)))

	_, _, err := executeStreams(c, home, "explain", "what is here?",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(seen, qt.HasLen, 2)
	c.Assert(kindsOf(decodeJSONL(c, seen[0])), qt.DeepEquals,
		[]string{"session", "request"})
	c.Assert(kindsOf(decodeJSONL(c, seen[1])), qt.DeepEquals,
		[]string{"session", "request", "tool"})
}

func TestExplain_JSONLSaysWhyARunEndedBadly(t *testing.T) {
	// A run that hit a limit still produces an answer record. Without the
	// reason in it, that record is an empty answer, which reads exactly like a
	// model with nothing to say -- and stdout is the only channel a consumer of
	// this format has.
	c := qt.New(t)
	stub := &scripted{bodies: []string{toolAnswer("read_artifact", `{"artifact":"migrations"}`)}}
	home := configHome(c, profileFor(endpoint(c, stub.handler())))
	root := workspace(c)

	out, _, err := executeStreams(c, home, "explain", "loop please",
		"--workspace", root, "--migrations-dir", "./migrations", "--dialect", "postgres",
		"--format", "jsonl")

	c.Assert(err, qt.ErrorMatches, "the run hit a limit: repeated tool call")
	records := decodeJSONL(c, out)
	answer := records[len(records)-1]
	c.Assert(answer["type"], qt.Equals, "answer")
	c.Assert(answer["stop_reason"], qt.Equals, "repeated tool call")
	c.Assert(answer["error"], qt.Equals, "the run hit a limit: repeated tool call")
}
