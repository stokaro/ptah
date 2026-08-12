package atlas_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
)

// emptySelectionDDL holds a plainly named table and a table whose name
// literally contains dots, so both the reject side and the accept side of the
// deleted shape check are observable from one fixture.
const emptySelectionDDL = `
CREATE TABLE empty_sel_users (
  id INTEGER PRIMARY KEY,
  email TEXT
);
CREATE TABLE "a.b.c" (
  id INTEGER PRIMARY KEY
);
`

// emptySelectionSpellings enumerates every way an --include selector can end
// up matching nothing. The deleted shape check refused exactly one of them
// (the literal separator) and let the rest through silently, because
// path.Match treats "." as an ordinary character: only "/" separates. The
// character class is the escape the shape check created for itself by reading
// bracket runs as identifier quoting.
//
// The list is exhaustive over the metacharacters that can stand for a dot, not
// a sample: the last rule shipped with its escapes sampled, and `[.]` is what
// slipped through.
var emptySelectionSpellings = []struct {
	name     string
	selector string
}{
	{name: "plain typo", selector: "no_such_table"},
	{name: "star crosses the separator", selector: "empty_sel_users*email"},
	{name: "question mark crosses the separator", selector: "empty_sel_users?email"},
	{name: "character class crosses the separator", selector: "main.empty_sel_users[.]email"},
	{name: "literal separator", selector: "main.empty_sel_users.email"},
}

// TestSchemaDiffIncludeEmptySelectionRefuses pins the diff verb's fail-closed
// answer. A warning beside a successful "synced" report still lets a mistyped
// selector green-light a CI check, so a selector that met neither side is an
// error and produces no diff output.
func TestSchemaDiffIncludeEmptySelectionRefuses(t *testing.T) {
	c := qt.New(t)

	for _, spelling := range emptySelectionSpellings {
		c.Run(spelling.name, func(c *qt.C) {
			dir := t.TempDir()
			fromPath := filepath.Join(dir, "from.sql")
			toPath := filepath.Join(dir, "to.sql")
			c.Assert(os.WriteFile(fromPath, []byte(""), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(toPath, []byte(emptySelectionDDL), 0o600), qt.IsNil)

			stdout, stderr, err := runCompat("schema", "diff",
				"--from", "file://"+fromPath,
				"--to", "file://"+toPath,
				"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
				"--include", spelling.selector,
			)

			c.Assert(err, qt.ErrorMatches,
				`the --include selection matched no objects: `+
					regexp.QuoteMeta(`"`+spelling.selector+`"`))
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals,
				"Error: the --include selection matched no objects: \""+spelling.selector+"\"\n")
		})
	}
}

// TestSchemaInspectIncludeEmptySelectionIsReportedOnStderr pins the same
// choice for inspection. A read-only description of an empty selection is a
// legitimate answer and its documented contract says so, so exit 0 and the
// rendered bytes stay; only the notice is added.
func TestSchemaInspectIncludeEmptySelectionIsReportedOnStderr(t *testing.T) {
	c := qt.New(t)

	for _, spelling := range emptySelectionSpellings {
		c.Run(spelling.name, func(c *qt.C) {
			dbPath := seedSQLiteDB(t, emptySelectionDDL)

			stdout, stderr, err := runCompatInspect(
				"--url", "sqlite://"+dbPath,
				"--include", spelling.selector,
			)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
			c.Assert(stdout, qt.Not(qt.Contains), "table ")
			c.Assert(stderr, qt.Equals,
				"Warning: the --include selection matched no objects: \""+spelling.selector+"\".\n")
		})
	}
}

// TestSchemaApplyIncludeEmptySelectionRefuses pins the other half of the
// divergence. `schema apply` used to answer "Schema is synced, no changes to
// be made." with exit 0 and an untouched database — a verb reporting success
// for work it did not do. It now refuses.
//
// This is a deliberate choice, not a match: the pinned Atlas community binary
// implements no --include flag at all, and its sibling positive selector
// --schema answers a zero match with exit 0 and silence on every verb. There
// is no oracle to copy here, so the better behavior was taken instead.
func TestSchemaApplyIncludeEmptySelectionRefuses(t *testing.T) {
	allowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)

	for _, spelling := range emptySelectionSpellings {
		c.Run(spelling.name, func(c *qt.C) {
			dbPath := seedSQLiteDB(t, "CREATE TABLE keepme (id INTEGER PRIMARY KEY);")
			schemaPath := filepath.Join(t.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schemaPath, []byte(emptySelectionDDL), 0o600), qt.IsNil)

			stdout, _, err := runCompat("schema", "apply",
				"--url", "sqlite://"+dbPath,
				"--to", "file://"+schemaPath,
				"--include", spelling.selector,
				"--auto-approve",
			)

			c.Assert(err, qt.ErrorMatches,
				`the --include selection matched no objects: `+
					regexp.QuoteMeta(`"`+spelling.selector+`"`)+
					`; schema apply would change nothing`)
			// Bytes, not exit status: the old behavior printed a success line
			// here, and a refusal must print none.
			c.Assert(stdout, qt.Equals, "")
			// Nothing was applied either way; the refusal only makes that
			// visible.
			c.Assert(sqliteHasTable(t, dbPath, "keepme"), qt.IsTrue)
			c.Assert(sqliteHasTable(t, dbPath, "empty_sel_users"), qt.IsFalse)
		})
	}
}

// TestSchemaIncludeSelectionAcceptsBareDottedName is the accept-side proof
// that the shape check is gone rather than merely relocated: a table literally
// named "a.b.c" is selected by the bare selector `a.b.c`, which the old rule
// refused before any database was contacted.
func TestSchemaIncludeSelectionAcceptsBareDottedName(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		// selector is a spelling of the same dotted table name. All three must
		// work: the two escaped forms were shipped as documented workarounds
		// for the bare one, so nobody who adopted them may break.
		selector string
	}{
		{name: "bare", selector: "a.b.c"},
		{name: "escaped", selector: `a\.b\.c`},
		{name: "quoted and qualified", selector: `main."a.b.c"`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dbPath := seedSQLiteDB(t, emptySelectionDDL)

			stdout, stderr, err := runCompatInspect(
				"--url", "sqlite://"+dbPath,
				"--include", test.selector,
			)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
			c.Assert(stdout, qt.Contains, `table "a.b.c"`)
			c.Assert(stdout, qt.Not(qt.Contains), `table "empty_sel_users"`)
			// A selection that matched says nothing on stderr.
			c.Assert(stderr, qt.Equals, "")
		})
	}
}
