package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemaprep"
)

func TestQualifyDeclaredUserTypes(t *testing.T) {
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
	qt.Assert(t, qualified.Fields[0].Type, qt.Equals, "billing.amount")
	qt.Assert(t, qualified.Fields[1].Type, qt.Equals, "billing.amount[3][]")
	qt.Assert(t, qualified.Fields[2].Type, qt.Equals, "mood")
	qt.Assert(t, qualified.Fields[3].Type, qt.Equals, "shared.mood[]")
	qt.Assert(t, qualified.Fields[4].Type, qt.Equals, "money")
	qt.Assert(t, database.Fields[0].Type, qt.Equals, "amount")
}

func TestQualifyFieldUserTypesLeavesAmbiguousAndQualifiedNamesAlone(t *testing.T) {
	t.Parallel()
	fields := []schemamodel.Field{{Type: "amount"}, {Type: "one.amount"}}
	declared := schemaprep.DeclaredUserTypes{Domains: []schemamodel.Domain{
		{Name: "amount", Schema: "one"},
		{Name: "amount", Schema: "two"},
	}}

	qt.Assert(t, schemaprep.QualifyFieldUserTypes(fields, declared, platform.Postgres), qt.DeepEquals, fields)
}
