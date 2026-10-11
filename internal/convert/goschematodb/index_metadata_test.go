package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/convert/goschematodb"
)

func TestIndexConversionPreservesPrefixAndDependenciesWithoutAliasing(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	desired := &schemamodel.Database{Indexes: []schemamodel.Index{{
		TableName: "items", Name: "by_label", Parts: []schemamodel.IndexPart{{Name: "label", Prefix: "20"}},
		NullsDistinct: new(false), Requires: []objectidentity.ID{pgextension.Subject(pgextension.Ref("bloom"))},
	}}}
	current, err := goschematodb.ToDBSchema(t.Context(), desired, "mysql", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(current.Indexes, qt.HasLen, 1)
	c.Assert(current.Indexes[0].Parts[0].Prefix, qt.Equals, "20")
	c.Assert(current.Indexes[0].Requires, qt.DeepEquals, []objectidentity.ID{pgextension.Subject(pgextension.Ref("bloom"))})
	c.Assert(*current.Indexes[0].NullsDistinct, qt.IsFalse)
	current.Indexes[0].Parts[0].Prefix = "40"
	current.Indexes[0].Requires[0].Name.Source = "changed"
	*current.Indexes[0].NullsDistinct = true
	c.Assert(desired.Indexes[0].Parts[0].Prefix, qt.Equals, "20")
	c.Assert(desired.Indexes[0].Requires, qt.DeepEquals, []objectidentity.ID{pgextension.Subject(pgextension.Ref("bloom"))})
	c.Assert(*desired.Indexes[0].NullsDistinct, qt.IsFalse)
}
