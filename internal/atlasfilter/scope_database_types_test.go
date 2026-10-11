package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/atlasfilter"
)

// scopeDatabaseTypesFixture is a read of a database whose users table has one
// column of each user-defined type kind, as the PostgreSQL reader reports
// them. A column declared with a domain reports the domain's base type in
// data_type and udt_name; only domain_name and domain_schema name the domain.
// A composite or a range column reports USER-DEFINED with the type in
// udt_name. The connection's own schema is blank on the objects and raw on
// the column, as the reader leaves them.
func scopeDatabaseTypesFixture() *catalog.Database {
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "public"}},
		Tables: []catalog.Table{{
			Name: "users",
			Columns: []catalog.Column{
				{Name: "id", DataType: "integer", UDTName: "int4"},
				{Name: "e", DataType: "text", UDTName: "text", DomainName: "email", DomainSchema: "public"},
				{Name: "a", DataType: "USER-DEFINED", UDTName: "addr"},
				{Name: "r", DataType: "USER-DEFINED", UDTName: "fr"},
			},
		}, {
			Name:    "audit",
			Columns: []catalog.Column{{Name: "z", DataType: "text", UDTName: "text", DomainName: "zip", DomainSchema: "public"}},
		}},
		Domains:    []catalog.Domain{{Name: "email", BaseType: "text"}, {Name: "zip", BaseType: "text"}},
		Composites: []catalog.CompositeType{{Name: "addr", Fields: []catalog.CompositeField{{Name: "city", Type: "text"}}}},
		Ranges:     []catalog.Range{{Name: "fr", Subtype: "double precision"}},
	}
}

func TestScopeDatabase_TypeReferences_HappyPath(t *testing.T) {
	c := qt.New(t)

	got, err := atlasfilter.ScopeDatabase(scopeDatabaseTypesFixture(), atlasfilter.Scope{
		Selections:    ownerSelections(),
		Include:       []string{"users"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	// The domain the users table names stays, as its composite and range
	// do; the domain only the unselected audit table names goes.
	c.Assert(got.Domains, qt.DeepEquals, []catalog.Domain{{Name: "email", BaseType: "text"}})
	c.Assert(got.Composites, qt.HasLen, 1)
	c.Assert(got.Ranges, qt.HasLen, 1)
}

func TestScopeDatabase_TypeReferences_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		exclude string
		want    string
	}{
		{name: "excluded domain", exclude: "email",
			want: `selected tables use domain "public.email", but "public.email" is not selected`},
		{name: "excluded composite type", exclude: "addr",
			want: `selected tables use composite type "public.addr", but "public.addr" is not selected`},
		{name: "excluded range type", exclude: "fr",
			want: `selected tables use range type "public.fr", but "public.fr" is not selected`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ScopeDatabase(scopeDatabaseTypesFixture(), atlasfilter.Scope{
				Selections:    ownerSelections(),
				Include:       []string{"users"},
				Exclude:       []string{test.exclude},
				DefaultSchema: "public",
			})

			c.Assert(got, qt.IsNil)
			var crossScope *atlasfilter.CrossScopeError
			c.Assert(err, qt.ErrorAs, &crossScope)
			c.Assert(crossScope.Diagnostics, qt.DeepEquals, []string{test.want})
		})
	}
}
