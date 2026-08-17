//go:build integration

package schemaload_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
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
func newContainmentFixture(t *testing.T, c *qt.C) containmentFixture {
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

func assertReachedTable(c *qt.C, database *goschema.Database, err error, name string) {
	c.Assert(err, qt.IsNil)
	c.Assert(database, qt.IsNotNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	// Assert which file was read, not merely that one was: an exit status
	// cannot tell a contained read from an escaped one.
	c.Assert(database.Tables[0].Name, qt.Equals, name)
}

// TestLoad_SchemaFileSpellingsReachTheSameDestination is what the refusal table
// here became when stokaro/ptah#1622 answered the decision the test below was
// written to make loud.
//
// The three rows used to assert that each of these spellings was REFUSED as
// `outside allowed root`. That refusal was a spelling filter rather than a
// boundary: the same file named absolutely was always accepted, which the test
// below records and the pinned community binary does too. With the relative-only
// rule gone, all three reach the file, and the property worth pinning is that
// they reach the SAME file -- one destination, four spellings, one answer.
//
// The symlink row still earns its place. It writes no ".." anywhere, so a rule
// that read the text rather than resolving it would treat it differently from
// the other two, and the four-way agreement below is what would catch that.
func TestLoad_SchemaFileSpellingsReachTheSameDestination(t *testing.T) {
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
	}, {
		// The spelling that was always accepted, kept beside the three that
		// were not so the agreement is visible in one table.
		name:     "absolute path to the same file",
		spelling: func(f containmentFixture) string { return filepath.Join(f.outside, "schema.sql") },
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newContainmentFixture(t, c)

			database, err := schemaload.Load(schemaload.Options{
				SchemaFiles: []string{tc.spelling(fixture)},
				Dialect:     "sqlite",
			})

			assertReachedTable(c, database, err, "outside_target")
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
			fixture := newContainmentFixture(t, c)

			database, err := schemaload.Load(schemaload.Options{
				SchemaFiles: []string{tc.spelling(fixture)},
				Dialect:     "sqlite",
			})

			assertReachedTable(c, database, err, "inside_target")
		})
	}
}
