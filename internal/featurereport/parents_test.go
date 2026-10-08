package featurereport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/featurereport"
)

func TestTableOwners_UsesSeparateSourceComponents(t *testing.T) {
	c := qt.New(t)
	tables := []schemamodel.Table{
		{Name: "tenant.events"}, {Schema: "tenant", Name: "events"},
		{Name: " events "}, {Schema: "public", Name: "tenant.events"},
	}
	owners := featurereport.NewTableOwners(tables)
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	for i, table := range tables {
		parent := builder.TablePartsVerbatim(table.Schema, table.Name)
		ref := objectidentity.ID{Kind: "example.org/child", Schema: parent.Schema, Parent: parent.Name}
		position, err := owners.Resolve(ref)
		c.Assert(err, qt.IsNil)
		c.Assert(position, qt.Equals, i)
	}
	position, err := owners.Resolve(objectidentity.ID{Kind: "example.org/standalone"})
	c.Assert(err, qt.IsNil)
	c.Assert(position, qt.Equals, -1)
}

func TestTableOwners_RefusesUnresolvedParents(t *testing.T) {
	part := func(value string) objectidentity.Part { return objectidentity.Part{Source: value, Normalized: value} }
	cases := []struct {
		name string
		ref  objectidentity.ID
	}{
		{name: "missing", ref: objectidentity.ID{Parent: part("missing")}},
		{name: "ambiguous", ref: objectidentity.ID{Parent: part("events")}},
		{name: "foreign catalog", ref: objectidentity.ID{Catalog: part("foreign"), Parent: part("single")}},
		{name: "missing source", ref: objectidentity.ID{Parent: objectidentity.Part{Normalized: "single"}}},
		{name: "case differs", ref: objectidentity.ID{Parent: part("SINGLE")}},
		{name: "wrong schema", ref: objectidentity.ID{Schema: part("foreign"), Parent: part("single")}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owners := featurereport.NewTableOwners([]schemamodel.Table{{Name: "events"}, {Name: "events"}, {Name: "single"}})
			position, err := owners.Resolve(test.ref)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(position, qt.Equals, -1)
		})
	}
}
