package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

func TestQualifyDeclaredUserTypes(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	database := &schemamodel.Database{
		Domains: []schemamodel.Domain{{Name: "amount", Schema: "billing"}},
		Enums:   []schemamodel.Enum{{Name: "mood", Schema: "shared"}},
		Fields: []schemamodel.Field{
			{Name: "a", Type: "amount"},
			{Name: "aa", Type: "amount[3][]"},
			{Name: "e", Type: "mood"},
			{Name: "ea", Type: "mood[]"},
			{Name: "built_in", Type: "money"},
		},
	}

	qualified := schemaprep.QualifyDeclaredUserTypes(database, platform.Postgres)
	c.Assert(qualified.Fields[0].Type, qt.Equals, "billing.amount")
	c.Assert(qualified.Fields[1].Type, qt.Equals, "billing.amount[3][]")
	c.Assert(qualified.Fields[2].Type, qt.Equals, "mood")
	c.Assert(qualified.Fields[3].Type, qt.Equals, "shared.mood[]")
	c.Assert(qualified.Fields[4].Type, qt.Equals, "money")
	c.Assert(database.Fields[0].Type, qt.Equals, "amount")
}

func TestQualifyFieldUserTypesLeavesAmbiguousAndQualifiedNamesAlone(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	fields := []schemamodel.Field{{Type: "amount"}, {Type: "one.amount"}}
	declared := schemaprep.DeclaredUserTypes{Domains: []schemamodel.Domain{
		{Name: "amount", Schema: "one"},
		{Name: "amount", Schema: "two"},
	}}

	c.Assert(schemaprep.QualifyFieldUserTypes(fields, declared, platform.Postgres), qt.DeepEquals, fields)
}
