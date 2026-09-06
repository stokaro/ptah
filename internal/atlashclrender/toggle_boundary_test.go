package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/pgindexstorage"
)

// A malformed toggle fails the export even for a schema that carries no index
// storage parameter at all.
//
// Found in review of stokaro/ptah#2183: resolving the value inside the
// per-index branch left a typo dormant until the day a schema had such a
// parameter, and then changed behavior for a reason nobody would connect to it.
// It is resolved once at the render boundary instead.
func TestRender_AMalformedToggleFailsEvenWithNothingToCarry(t *testing.T) {
	c := qt.New(t)
	t.Setenv(pgindexstorage.EnvVar, "yes")

	_, err := atlashclrender.Render(&schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INTEGER", Primary: true}},
	})

	c.Assert(err, qt.ErrorMatches, `.*PTAH_POSTGRES_INDEX_STORAGE_PARAMS.*`)
}

// A valid value renders, which is the control the refusal above needs.
func TestRender_AValidToggleStillRenders(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "false", value: "false"},
		{name: "true", value: "true"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(pgindexstorage.EnvVar, test.value)

			result, err := atlashclrender.Render(&schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INTEGER", Primary: true}},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, `table "t"`)
		})
	}
}
