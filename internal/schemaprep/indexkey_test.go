package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

func TestIndexKeyParts(t *testing.T) {
	for _, test := range []struct {
		name  string
		index schemamodel.Index
		want  []string
	}{
		{name: "fields", index: schemamodel.Index{Fields: []string{"a", "b"}}, want: []string{"a", "b"}},
		{name: "parts win over fields", index: schemamodel.Index{
			Fields: []string{"ignored"},
			Parts:  []schemamodel.IndexPart{{Name: "a"}, {Expr: "lower(b)"}, {Name: "c", Expr: "upper(c)"}},
		}, want: []string{"a", "lower(b)", "upper(c)"}},
		{name: "nothing declared", index: schemamodel.Index{}, want: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprep.IndexKeyParts(test.index), qt.DeepEquals, test.want)
		})
	}
}
