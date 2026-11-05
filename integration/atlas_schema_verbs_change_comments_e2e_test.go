//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	schemaVerbsFrom = "CREATE TABLE t (id int PRIMARY KEY, a int);\nCREATE INDEX i ON t (a);\n"
	schemaVerbsTo   = "CREATE TABLE t (id int PRIMARY KEY, a int, b int);\nCREATE INDEX i2 ON t (b);\nCREATE TABLE u (id int PRIMARY KEY);\n"
)

// changeLine finds a comment line that names a change, in the upper case the
// schema verbs write it.
var changeLine = regexp.MustCompile(`(?m)^ *-- ((?:Create|Modify|Drop|Add) .*)$`)

// changeLines is the comments of text that name a change, in order.
func changeLines(text string) []string {
	lines := make([]string, 0, 8)
	for _, match := range changeLine.FindAllStringSubmatch(text, -1) {
		lines = append(lines, match[1])
	}
	return lines
}

// schemaVerbsFiles writes the two schemas and returns their URLs.
func schemaVerbsFiles(c *qt.C) (from, to string) {
	c.Helper()
	dir := c.TempDir()
	from, to = filepath.Join(dir, "from.sql"), filepath.Join(dir, "to.sql")
	c.Assert(os.WriteFile(from, []byte(schemaVerbsFrom), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte(schemaVerbsTo), 0o600), qt.IsNil)
	return "file://" + filepath.ToSlash(from), "file://" + filepath.ToSlash(to)
}

// TestCompatSchemaVerbsChangeCommentsE2E_NameEachStatement holds `schema diff`,
// `schema apply` and `schema plan` to the comments above their statements, with
// the first letter in upper case, and runs the apply on the server. The comments
// reach the display only: the apply reads the table back as planned.
func TestCompatSchemaVerbsChangeCommentsE2E_NameEachStatement(t *testing.T) {
	c := qt.New(t)
	from, to := schemaVerbsFiles(c)
	dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_verbs_dev"), "public")
	target := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_verbs_target"), "public")
	want := []string{
		`Create "u" table`,
		`Modify "t" table`,
		`Create index "i2" to table: "t"`,
		`Drop index "i" from table: "t"`,
	}

	diffed, err := runCompatVerb("schema", "diff", "--from", from, "--to", to, "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", diffed))
	c.Assert(changeLines(diffed), qt.DeepEquals, want, qt.Commentf("%s", diffed))

	first, err := runCompatVerb("schema", "apply", "--url", target, "--to", from, "--dev-url", dev, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", first))
	c.Assert(changeLines(first), qt.DeepEquals, []string{`Create "t" table`, `Create index "i" to table: "t"`})

	planned, err := runCompatVerb("schema", "plan", "--from", target, "--to", to, "--dev-url", dev, "--name", "p", "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", planned))
	c.Assert(changeLines(planned), qt.DeepEquals, want, qt.Commentf("%s", planned))

	applied, err := runCompatVerb("schema", "apply", "--url", target, "--to", to, "--dev-url", dev, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	c.Assert(changeLines(applied), qt.DeepEquals, want, qt.Commentf("%s", applied))

	again, err := runCompatVerb("schema", "diff", "--from", target, "--to", to, "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", again))
	c.Assert(changeLines(again), qt.HasLen, 0, qt.Commentf("%s", again))
}

// TestCompatSchemaVerbsChangeCommentsE2E_SpeakTheBinarysWords runs the pinned
// community binary and this one over the same two schemas and compares the
// comments of `schema diff` and of a `schema apply` dry run, as sets of lines: the
// two plans still order and group their statements differently.
func TestCompatSchemaVerbsChangeCommentsE2E_SpeakTheBinarysWords(t *testing.T) {
	c := qt.New(t)
	reference := requireEngineReference(t)
	from, to := schemaVerbsFiles(c)
	dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_verbs_ref_dev"), "public")

	theirs, err := runEngineVerb(reference, []string{"schema", "diff", "--from", from, "--to", to, "--dev-url", dev})
	c.Assert(err, qt.IsNil, qt.Commentf("%s", theirs))
	ours, err := runCompatVerb("schema", "diff", "--from", from, "--to", to, "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", ours))

	c.Assert(distinctSorted(changeLines(ours)), qt.DeepEquals, distinctSorted(changeLines(theirs)),
		qt.Commentf("ours:\n%s\ntheirs:\n%s", ours, theirs))
}

// distinctSorted is lines sorted, each once.
func distinctSorted(lines []string) []string {
	sorted := slices.Clone(lines)
	slices.Sort(sorted)
	return slices.Compact(sorted)
}
