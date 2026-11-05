//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// Two declared sequences a --from document records it does not describe are
// two changes `schema diff` cannot plan. The run reports them as one warning
// that names both, on both binaries. The two documents are HCL, which the
// command reads without a database, so the dev database URL names a port
// nothing listens on: a run that tried to connect would fail rather than pass.

// undecidedFromDocument records that it describes no sequences.
const undecidedFromDocument = `// ptah:not-described sequence
schema "public" {
}
table "users" {
  schema = schema.public
  column "id" {
    type = int
  }
}
`

// undecidedToDocument declares two sequences beside the same table.
const undecidedToDocument = `schema "public" {
}
table "users" {
  schema = schema.public
  column "id" {
    type = int
  }
}
sequence "a_seq" {
  schema = schema.public
  type = "bigint"
}
sequence "b_seq" {
  schema = schema.public
  type = "bigint"
}
`

// undecidedSequencesWarning is the warning a run writes for both sequences.
const undecidedSequencesWarning = `warning: 2 objects get no planned change because the comparison could not decide them:` +
	` sequence "public.a_seq", sequence "public.b_seq" (--from records ` + "`ptah:not-described sequence`" +
	`); PTAH_LOG_LEVEL=debug lists them`

// undecidedDiffArgs writes both documents to a directory and returns the
// schema diff arguments that compare them.
func undecidedDiffArgs(c *qt.C) []string {
	c.Helper()
	dir := c.TempDir()
	from := filepath.Join(dir, "from.hcl")
	to := filepath.Join(dir, "to.hcl")
	c.Assert(os.WriteFile(from, []byte(undecidedFromDocument), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte(undecidedToDocument), 0o600), qt.IsNil)
	return []string{
		"schema", "diff", "--from", "file://" + filepath.ToSlash(from), "--to", "file://" + filepath.ToSlash(to),
		"--dev-url", "postgres://nobody@127.0.0.1:1/dev",
	}
}

// TestSchemaDiffNamesEveryUndecidedObjectInOneWarningE2E holds both binaries
// to one warning line naming both sequences.
func TestSchemaDiffNamesEveryUndecidedObjectInOneWarningE2E(t *testing.T) {
	for _, target := range []clirun.Target{clirun.Ptah, clirun.Compat} {
		t.Run(filepath.Base(string(target)), func(t *testing.T) {
			c := qt.New(t)

			got := clirun.Run(c, target, clirun.Options{Dir: c.TempDir()}, undecidedDiffArgs(c)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
			c.Assert(got.Stderr, qt.Equals, undecidedSequencesWarning+"\n")
			c.Assert(got.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
		})
	}
}

// TestSchemaDiffWritesUndecidedObjectsAsOneJSONRecordE2E holds the warning to
// one JSON record under PTAH_LOG_FORMAT=json, counting both sequences, so the
// stream holds no text line.
func TestSchemaDiffWritesUndecidedObjectsAsOneJSONRecordE2E(t *testing.T) {
	c := qt.New(t)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_LOG_FORMAT=json")},
		undecidedDiffArgs(c)...)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	lines := strings.Split(strings.TrimSuffix(got.Stderr, "\n"), "\n")
	c.Assert(lines, qt.HasLen, 1)
	var record map[string]any
	c.Assert(json.Unmarshal([]byte(lines[0]), &record), qt.IsNil, qt.Commentf("%s", lines[0]))
	c.Assert(record["level"], qt.Equals, "WARN")
	c.Assert(record["category"], qt.Equals, "undecided-object")
	c.Assert(record["count"], qt.Equals, float64(2))
	c.Assert(record["msg"], qt.Equals, strings.TrimPrefix(undecidedSequencesWarning, "warning: "))
}
