//go:build integration

package schemaload_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/schemaload"
)

// containmentFixture is one pair of destinations — one inside the working
// directory and one outside it — reachable by many spellings. Each table row
// names the same destination a different way, so the assertions compare
// spellings against each other rather than against a single expectation.
type containmentFixture struct {
	root    string
	outside string
}

// newContainmentFixture builds the tree below and chdirs into root, so the
// guard's allowed root is root for the whole test.
//
//	parent/
//	  root/          the working directory, and the allowed root
//	    a/           an ordinary subdirectory, for the mid-path ".." spelling
//	    inside.sql   destination inside the root: table inside_target
//	    link      -> ../outside, a symlink whose target leaves the root
//	  outside/
//	    schema.sql   destination outside the root: table outside_target
func newContainmentFixture(t *testing.T, tb testing.TB) containmentFixture {
	c := qt.New(tb)
	t.Helper()
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	outside := filepath.Join(parent, "outside")
	c.Assert(os.MkdirAll(filepath.Join(root, "a"), 0o750), qt.IsNil)
	c.Assert(os.MkdirAll(outside, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(outside, "schema.sql"),
		[]byte("CREATE TABLE outside_target (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "inside.sql"),
		[]byte("CREATE TABLE inside_target (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.Symlink(outside, filepath.Join(root, "link")), qt.IsNil)
	t.Chdir(root)
	return containmentFixture{root: root, outside: outside}
}

func assertReachedTable(tb testing.TB, database *goschema.Database, err error, name string) {
	c := qt.New(tb)
	c.Assert(err, qt.IsNil)
	c.Assert(database, qt.IsNotNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	// Assert which file was read, not merely that one was: an exit status
	// cannot tell a contained read from an escaped one.
	c.Assert(database.Tables[0].Name, qt.Equals, name)
}

// TestLoad_SchemaFileContainmentRefusesEscapes pins that the CLI path guard
// reaches the native desired-schema resolver used by schema-file commands.
//
// The resolver used to call filepath.Abs on the operator's path before handing
// it to the guard. Every spelling therefore arrived absolute, the guard's
// absolute branch skipped containment, and the native surface accepted
// "../outside/schema.sql" while ptah-compat refused the identical destination
// through the identical guard. The rewrite was not a decision about what should
// be allowed; it was a canonicalization that happened to disarm the check.
//
// Refusal here is judged on where a path lands, never on how it is written. In
// particular, the symlink row never writes ".." but is still refused.
func TestLoad_SchemaFileContainmentRefusesEscapes(t *testing.T) {
	testCases := []struct {
		name     string
		spelling func(containmentFixture) string
	}{{
		name:     "relative traversal leaves the root",
		spelling: func(containmentFixture) string { return filepath.Join("..", "outside", "schema.sql") },
	}, {
		// No ".." anywhere in the spelling. A rule that reads the text instead
		// of resolving it accepts this one.
		name:     "symlink inside the root points out of it",
		spelling: func(containmentFixture) string { return filepath.Join("link", "schema.sql") },
	}, {
		name:     "dot-dot in the middle of the path",
		spelling: func(containmentFixture) string { return filepath.Join("a", "..", "..", "outside", "schema.sql") },
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newContainmentFixture(t, c.TB)

			database, err := schemaload.Load(schemaload.Options{
				SchemaFiles: []string{tc.spelling(fixture)},
				Dialect:     "sqlite",
			})

			c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
			c.Assert(database, qt.IsNil)
		})
	}
}

// TestLoad_SchemaFileContainmentAllowsContainedDestinations keeps successful
// loads separate from the refusal table above. A path may contain ".." and
// remain valid when its resolved destination is inside the working directory.
func TestLoad_SchemaFileContainmentAllowsContainedDestinations(t *testing.T) {
	testCases := []struct {
		name     string
		spelling func(containmentFixture) string
	}{{
		// The converse of the row above: ".." is present and the destination is
		// still inside, so a text rule that refuses ".." refuses this wrongly.
		name:     "path leaves the root and returns to it",
		spelling: func(f containmentFixture) string { return filepath.Join("..", filepath.Base(f.root), "inside.sql") },
	}, {
		name:     "relative path inside the root",
		spelling: func(containmentFixture) string { return "inside.sql" },
	}, {
		// Acceptance is destination-based too: an absolute spelling of a
		// contained destination stays contained and stays allowed.
		name:     "absolute path inside the root",
		spelling: func(f containmentFixture) string { return filepath.Join(f.root, "inside.sql") },
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newContainmentFixture(t, c.TB)

			database, err := schemaload.Load(schemaload.Options{
				SchemaFiles: []string{tc.spelling(fixture)},
				Dialect:     "sqlite",
			})

			assertReachedTable(c.TB, database, err, "inside_target")
		})
	}
}

// TestLoad_AbsoluteSchemaFileOutsideTheRootIsStillAccepted records the half of
// stokaro/ptah#1241 item 11 that this change does NOT close, so the behavior is
// visible in the suite rather than only in an issue comment.
//
// pathguard.ResolveCLIPath exempts absolute pathnames from containment
// entirely. Closing that exemption is not a bug fix, it is the parity decision
// the issue is really asking about: an absolute pathname is how operators
// ordinarily name a file, the pinned community binary accepts it, and refusing
// it would remove reach that Ptah has always had. This row must be deleted, not
// edited, by whichever way that decision goes — it exists to make the decision
// loud.
func TestLoad_AbsoluteSchemaFileOutsideTheRootIsStillAccepted(t *testing.T) {
	c := qt.New(t)
	fixture := newContainmentFixture(t, c.TB)

	database, err := schemaload.Load(schemaload.Options{
		SchemaFiles: []string{filepath.Join(fixture.outside, "schema.sql")},
		Dialect:     "sqlite",
	})

	assertReachedTable(c.TB, database, err, "outside_target")
}
