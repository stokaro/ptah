package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

func TestParse_RefusesUnknownCommonConstraintKinds(t *testing.T) {
	for _, kind := range []string{"EXCLUDE", "UNQIUE"} {
		t.Run(kind, func(t *testing.T) {
			c := qt.New(t)
			source := `table "rooms" {
  column "id" { type = integer }
  constraint "c" { type = "` + kind + `" }
}`
			db, err := atlashcl.Parse([]byte(source), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, `(?s).*constraint "c" has unsupported common kind .*`)
			c.Assert(db, qt.IsNil)
		})
	}
}
