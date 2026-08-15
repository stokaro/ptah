package atlas_test

import (
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// The two `migrate lint --format` cells stokaro/ptah#1241 lists as
// "ptah-compat exits 1 where the pinned community binary v1.3.0 exits 0":
// item 9, `.Files[].Reports`, and item 10, top-level `.Schema`.
//
// Measured against that binary on 2026-08-09, SQLite, each cell in its own
// directory, the exit status read on a line of its own after a redirect rather
// than through a pipe. Before this change:
//
//	migrate lint --latest 1 --format '{{ range .Files }}{{ range .Reports }}…'
//	    binary 0 / ptah 1  can't evaluate field Reports in type MigrateLintFile
//	migrate lint --latest 1 --format '{{ .Schema.Current }}'
//	    binary 0 / ptah 1  can't evaluate field Schema in type MigrateLint
//
// The content rows below are not decoration. A model that answered both
// templates with empty values would close the exit-code cells and report
// nothing, so each field is pinned to a value measured on that binary: the
// report grouping and its diagnostic, and a Current/Desired pair that must
// differ from each other by exactly the table the analyzed version drops.

// lintFormatDropComment precedes the DROP so the diagnostic lands on line 2,
// which makes its byte offset 28 rather than the 0 an unresolved line would
// also produce.
const lintFormatDropComment = "-- retire the staging table\n"

// lintFormatDropPos is the byte offset of the DROP statement inside the
// analyzed file: len(lintFormatDropComment). The pinned binary reports the
// offset of the reported line's first byte, verified byte-identical on a
// `DROP TABLE` fixture where both tools name the same statement.
const lintFormatDropPos = "28"

type lintFormatCase struct {
	name   string
	format string
	assert func(c *qt.C, err error, output string)
}

// writeLintFormatFixture lays down two Atlas migrations: a base that creates
// two tables and an analyzed version that drops one of them. With --latest 1
// only the second is analyzed, so Current is the state the base leaves and
// Desired is the state after the drop -- the pair cannot be the same read
// twice.
func writeLintFormatFixture(c *qt.C, dir string) {
	c.Helper()
	writeHashedAtlasDir(
		c,
		dir,
		"20240101000000_base.sql",
		"CREATE TABLE kept (id INTEGER);\nCREATE TABLE staging (id INTEGER);\n",
	)
	writeHashedAtlasDir(c, dir, "20240101000001_drop.sql", lintFormatDropComment+"DROP TABLE staging;\n")
}

func TestCompatMigrateLintFormatModel(t *testing.T) {
	tests := []lintFormatCase{
		{
			// #1241 item 9, the exit-code cell exactly as the issue spells it.
			name:   "Files carry Reports",
			format: "{{ range .Files }}{{ range .Reports }}{{ .Text }}{{ end }}{{ end }}",
			assert: func(c *qt.C, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(output, qt.Contains, "destructive changes detected")
			},
		},
		{
			// The diagnostic inside the report, field by field. Measured on the
			// pinned binary as Pos/Code/Text/SuggestedFixes and compared with
			// `json .Reports`, which was byte-identical.
			name: "a report diagnostic carries Pos, Code, Text and its fix",
			format: "{{ range .Files }}{{ range .Reports }}{{ range .Diagnostics }}" +
				"pos={{ .Pos }} code={{ .Code }} text={{ .Text }}" +
				"{{ range .SuggestedFixes }} fix={{ .Message }}{{ end }}" +
				"{{ end }}{{ end }}{{ end }}",
			assert: func(c *qt.C, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(output, qt.Contains, "pos="+lintFormatDropPos+" code=DS102 text=Dropping table \"staging\"")
				c.Assert(output, qt.Contains, "fix=Add a pre-migration check to ensure table \"staging\" is empty before dropping it")
			},
		},
		{
			// #1241 item 10, the exit-code cell exactly as the issue spells it.
			name:   "Schema.Current is the state the analyzed version starts from",
			format: "{{ .Schema.Current }}",
			assert: func(c *qt.C, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(output, qt.Contains, "table \"staging\"")
				c.Assert(output, qt.Contains, "table \"kept\"")
			},
		},
		{
			// The row that separates a real before/after pair from the same
			// state rendered twice: the dropped table is in Current and must be
			// gone from Desired.
			name:   "Schema.Desired is the state the analyzed version leaves",
			format: "{{ .Schema.Desired }}",
			assert: func(c *qt.C, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(output, qt.Contains, "table \"kept\"")
				c.Assert(output, qt.Not(qt.Contains), "table \"staging\"")
			},
		},
		{
			// Compatibility adds the documented model; it does not delete
			// Ptah's own richer per-finding record from the same file
			// (AGENTS.md, "Compatibility never removes a capability").
			name:   "Findings survive beside Reports",
			format: "{{ range .Files }}{{ range .Findings }}{{ .Rule }}|{{ .Message }}{{ end }}{{ end }}",
			assert: func(c *qt.C, err error, output string) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
				c.Assert(output, qt.Contains, "DS102|")
				c.Assert(output, qt.Contains, "DROP TABLE permanently deletes")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			t.Chdir(root)
			writeLintFormatFixture(c, filepath.Join(root, "migrations"))

			output, err := runCompatCommand(t,
				"migrate", "lint",
				"--dir", "file://migrations",
				"--dev-url", "sqlite://file?mode=memory",
				"--latest", "1",
				"--format", tt.format,
			)

			tt.assert(c, err, output)
		})
	}
}

// TestCompatMigrateLintFormatCleanDirectoryExitsZero is the accept side of both
// cells: on a directory whose analyzed version raises nothing, a template
// naming Reports or Schema must exit 0, which is what the pinned binary does.
// Without it the tests above would pass on a build that evaluated the fields
// and then failed for some other reason.
func TestCompatMigrateLintFormatCleanDirectoryExitsZero(t *testing.T) {
	tests := []lintFormatCase{
		{
			name:   "Reports on a clean directory",
			format: "{{ range .Files }}{{ range .Reports }}{{ .Text }}{{ end }}{{ end }}",
			assert: func(c *qt.C, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				c.Assert(strings.TrimSpace(output), qt.Equals, "")
			},
		},
		{
			name:   "Schema on a clean directory",
			format: "{{ .Schema.Current }}",
			assert: func(c *qt.C, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				c.Assert(output, qt.Contains, "table \"kept\"")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			t.Chdir(root)
			dir := filepath.Join(root, "migrations")
			writeHashedAtlasDir(c, dir, "20240101000000_base.sql", "CREATE TABLE kept (id INTEGER);\n")
			writeHashedAtlasDir(c, dir, "20240101000001_more.sql", "CREATE TABLE also_kept (id INTEGER);\n")

			output, err := runCompatCommand(t,
				"migrate", "lint",
				"--dir", "file://migrations",
				"--dev-url", "sqlite://file?mode=memory",
				"--latest", "1",
				"--format", tt.format,
			)

			tt.assert(c, err, output)
		})
	}
}

// TestCompatMigrateLintFormatSchemaWithoutDevDatabase pins why `.Schema` is a
// value and not a pointer.
//
// PTAH_ATLAS_LINT_WITHOUT_DEV_URL is the opt-in that lets Ptah's analyzers
// review a directory with no dev database at all -- a capability the binary
// this surface stands in for does not have. There is then no schema to read,
// and a nil pointer would fail template execution and exit 1 on an argv this
// surface exits 0 on, which is the exact failure mode #1241 is about. The
// answer is two empty strings: truthful, and still evaluable.
func TestCompatMigrateLintFormatSchemaWithoutDevDatabase(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "1")
	dir := filepath.Join(root, "migrations")
	writeHashedAtlasDir(c, dir, "20240101000000_base.sql", "CREATE TABLE kept (id INTEGER);\n")
	writeHashedAtlasDir(c, dir, "20240101000001_more.sql", "CREATE TABLE also_kept (id INTEGER);\n")

	output, err := runCompatCommand(t,
		"migrate", "lint",
		"--dir", "file://migrations",
		"--latest", "1",
		"--format", "current=[{{ .Schema.Current }}] desired=[{{ .Schema.Desired }}]",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	c.Assert(strings.TrimSpace(output), qt.Equals, "current=[] desired=[]")
}

// TestCompatMigrateLintTextReportUnchangedBySchemaCapture is the
// non-interference control for item 10: the before/after reads ride the replay
// only when a template asks for them, so a run with no --format still produces
// the same text report, with no schema rendering leaking into it.
func TestCompatMigrateLintTextReportUnchangedBySchemaCapture(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	dir := filepath.Join(root, "migrations")
	writeHashedAtlasDir(c, dir, "20240101000000_base.sql", "CREATE TABLE kept (id INTEGER);\n")
	writeHashedAtlasDir(c, dir, "20240101000001_more.sql", "CREATE TABLE also_kept (id INTEGER);\n")

	output, err := runCompatCommand(t,
		"migrate", "lint",
		"--dir", "file://migrations",
		"--dev-url", "sqlite://file?mode=memory",
		"--latest", "1",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, "-- 1 version ok")
	c.Assert(output, qt.Not(qt.Contains), "table \"kept\"")
}
