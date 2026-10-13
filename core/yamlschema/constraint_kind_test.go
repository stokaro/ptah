package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

func TestParse_RefusesUnknownCommonConstraintKinds(t *testing.T) {
	for _, kind := range []string{"EXCLUDE", "UNQIUE"} {
		t.Run(kind, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse(yamlext.None(), []byte(`constraints:
  c:
    table: rooms
    type: `+kind+"\n"))
			c.Assert(err, qt.ErrorMatches, `(?s).*constraint "c" has unsupported type .*`)
			c.Assert(db, qt.IsNil)
		})
	}
}
