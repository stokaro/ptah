package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/definedtypetest"
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

// A type a feature owner's object declares without a schema leaves a column
// that names it as written, as a common domain without a schema does: the
// declaration left the schema to the connection's default, and qualifying
// the column with the default spelled out would name a schema the author did
// not write.
func TestQualifyDeclaredUserTypes_OwnerTypeInTheDefaultSchema(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(definedtypetest.Object(objectidentity.KindDomain, "", "amount", "numeric"))),
		Fields:         []schemamodel.Field{{Name: "a", Type: "amount"}},
	}

	qualified := schemaprep.QualifyDeclaredUserTypes(database, platform.Postgres)

	c.Assert(qualified.Fields[0].Type, qt.Equals, "amount")
}

// A type a feature owner's object defines qualifies a column as a common one
// does: in the schema it is declared in, scalar and array alike, and not at
// all when another declaration takes the same name.
func TestQualifyDeclaredUserTypes_OwnerTypes(t *testing.T) {
	tests := []struct {
		name    string
		domains []schemamodel.Domain
		want    []string
	}{
		{name: "the owner's types alone", want: []string{"billing.amount", "geo.addr[]", "money"}},
		{name: "a common domain of the same name in another schema",
			domains: []schemamodel.Domain{{Name: "amount", Schema: "other"}}, want: []string{"amount", "geo.addr[]", "money"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{
				Domains: test.domains,
				FeatureObjects: must.Must(schemaext.NewObjects(
					definedtypetest.Object(objectidentity.KindDomain, "billing", "amount", "numeric"),
					definedtypetest.Object(objectidentity.KindComposite, "geo", "addr", "text"),
				)),
				Fields: []schemamodel.Field{{Name: "a", Type: "amount"}, {Name: "b", Type: "addr[]"}, {Name: "c", Type: "money"}},
			}

			qualified := schemaprep.QualifyDeclaredUserTypes(database, platform.Postgres)

			c.Assert([]string{qualified.Fields[0].Type, qualified.Fields[1].Type, qualified.Fields[2].Type}, qt.DeepEquals, test.want)
		})
	}
}
