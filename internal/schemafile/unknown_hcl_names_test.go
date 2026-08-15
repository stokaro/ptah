package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/schemafile"
)

// unknownHCLNamesSource carries one unmodeled name in each of the two
// positions the loader has to forward the option for: a top-level block and a
// column attribute.
const unknownHCLNamesSource = `
annotation "gql" {
  attr "name" {
    type = string
  }
}
schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type      = int
    invisible = true
  }
}
`

func writeUnknownHCLNamesFixture(tb testing.TB) string {
	c := qt.New(tb)
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(unknownHCLNamesSource), 0o600), qt.IsNil)
	return "file://" + path
}

// TestLoad_RefusesUnknownHCLNamesByDefault pins the native half of the split:
// Ptah's own schema loading keeps naming an unmodeled construct instead of
// dropping it, so a typo in a hand-written schema stays a diagnostic.
func TestLoad_RefusesUnknownHCLNamesByDefault(t *testing.T) {
	c := qt.New(t)

	_, err := schemafile.Load(writeUnknownHCLNamesFixture(c.TB), schemafile.Options{})

	c.Assert(err, qt.ErrorMatches, `.*unsupported top-level block "annotation".*`)
}

// TestLoad_IgnoresUnknownHCLNamesWhenAsked pins the compat half: the option has
// to reach the HCL parser rather than being recorded and dropped, and it has to
// cover both positions in one pass -- the file still parses after the top-level
// block is dropped only if the column attribute is dropped too.
func TestLoad_IgnoresUnknownHCLNamesWhenAsked(t *testing.T) {
	c := qt.New(t)

	db, err := schemafile.Load(
		writeUnknownHCLNamesFixture(c.TB),
		schemafile.Options{IgnoreUnknownHCLNames: true},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].Name, qt.Equals, "id")
}
