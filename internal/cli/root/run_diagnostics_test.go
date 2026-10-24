package root_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/root"
)

// longName is the i-th index name of a schema file: 68 bytes, distinct in its
// first byte, so the 63 the server keeps tell them apart.
func longName(i int) string {
	return "abcdefghij"[i:i+1] + strings.Repeat("x", 63) + "_idx"
}

// longNamesSchema writes a PostgreSQL schema file with the index names.
func longNamesSchema(c *qt.C, names ...string) string {
	c.Helper()
	var sql strings.Builder
	sql.WriteString("CREATE TABLE t (a int);\n")
	for _, name := range names {
		sql.WriteString("CREATE INDEX \"" + name + "\" ON t (a);\n")
	}
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(sql.String()), 0o600), qt.IsNil)
	return path
}

// render runs `schema render` on path as one run and returns its exit code
// and what it wrote.
func render(c *qt.C, path string) (int, string) {
	c.Helper()
	cmd := root.NewRootCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	code := root.RunContext(c.Context(), cmd, "schema", "render", "--schema-file", path, "--dialect", "postgres")
	return code, stdout.String() + stderr.String()
}

// renderWarnings is the warning lines of a successful render.
func renderWarnings(c *qt.C, path string) []string {
	c.Helper()
	code, output := render(c, path)
	c.Assert(code, qt.Equals, 0, qt.Commentf("output:\n%s", output))
	var warnings []string
	for line := range strings.SplitSeq(output, "\n") {
		if strings.HasPrefix(line, "warning: ") {
			warnings = append(warnings, line)
		}
	}
	return warnings
}

// Names PostgreSQL cuts are one warning for the run, with how many, and the
// names themselves go to the debug log.
func TestRunContext_ReportsCutNamesTogether(t *testing.T) {
	c := qt.New(t)

	warnings := renderWarnings(c, longNamesSchema(c, longName(0), longName(1), longName(2)))

	c.Assert(warnings, qt.DeepEquals, []string{
		"warning: 3 names are longer than 63 bytes; the server keeps their first 63 bytes, and they are read that way; PTAH_LOG_LEVEL=debug lists them",
	})
}

// One cut name is reported as itself, with the name written and the name kept.
func TestRunContext_ReportsOneCutNameAsItself(t *testing.T) {
	c := qt.New(t)
	long := longName(0)
	path := longNamesSchema(c, long)

	warnings := renderWarnings(c, path)

	c.Assert(warnings, qt.HasLen, 1)
	c.Assert(warnings[0], qt.Matches, `warning: schema file .*`+regexp.QuoteMeta(filepath.Base(path))+`: index "`+long+
		`" in schema "public" is 68 bytes; the server keeps its first 63: "`+long[:63]+`"`)
}

// Two names cut to one are refused, whatever else the run reports: the
// aggregation is for names the server keeps apart, never for a collision.
func TestRunContext_RefusesCutNamesThatCollide(t *testing.T) {
	c := qt.New(t)
	shared := strings.Repeat("x", 63)

	code, output := render(c, longNamesSchema(c, shared+"a_idx", shared+"b_idx", longName(0)))

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(output, qt.Contains, `are both stored as "`+shared+`", and the server refuses the second`)
}
