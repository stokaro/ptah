package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlasfilter"
)

// These tests build the type owner's objects themselves, rather than read them
// from a source: no source produces them yet. The filter names them with the
// selector types of the common types and keeps the ones a kept column uses.

// typeNames lists the owner's types objects holds, as kind:name.
func typeNames(c *qt.C, objects schemaext.Objects) []string {
	c.Helper()
	names := make([]string, 0, objects.Len())
	for _, ref := range objects.Refs() {
		names = append(names, pgtype.Label(ref)+":"+ref.Name.Source)
	}
	return names
}

// declaredTypes declares the domains app.email and app.code, the composite
// type addr and the range type span, the last two in no schema.
func declaredTypes() schemaext.Objects {
	return must.Must(schemaext.NewObjects(
		must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, "app", "email"), &pgtype.DesiredDomain{BaseType: "text"})),
		must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, "app", "code"), &pgtype.DesiredDomain{BaseType: "integer"})),
		must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.CompositeKind, "", "addr"),
			&pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "text"}}})),
		must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, "", "span"), &pgtype.DesiredRange{Subtype: "int4"})),
	))
}

// heldTypes is what a read of the same types reports, each in its schema.
func heldTypes() schemaext.Objects {
	return must.Must(schemaext.NewObjects(
		must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.DomainKind, "app", "email"), &pgtype.ObservedDomain{BaseType: "text"})),
		must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.DomainKind, "app", "code"), &pgtype.ObservedDomain{BaseType: "integer"})),
		must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.CompositeKind, "public", "addr"),
			&pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "text"}}})),
		must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.RangeKind, "public", "span"), &pgtype.ObservedRange{Subtype: "int4"})),
	))
}

// TestExclude_ExcludesAnOwnersTypeOnBothSides drops a type an exclusion
// names, by its selector type or by its schema, from the declaration and from
// the read alike, and reports the selector as having named something.
func TestExclude_ExcludesAnOwnersTypeOnBothSides(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
		want    []string
	}{
		{name: "a domain by name", exclude: []string{"app.email[type=domain]"}, want: []string{"composite type:addr", "domain:code", "range type:span"}},
		{name: "every composite type", exclude: []string{"*[type=composite_type]"}, want: []string{"domain:code", "domain:email", "range type:span"}},
		{name: "a range type", exclude: []string{"span[type=range]"}, want: []string{"composite type:addr", "domain:code", "domain:email"}},
		{name: "a schema", exclude: []string{"app[type=schema]"}, want: []string{"composite type:addr", "range type:span"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			generated, generatedReport, err := atlasfilter.ExcludeGeneratedReport(ownerSelections(),
				&schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}}, FeatureObjects: declaredTypes()}, test.exclude, "public")
			c.Assert(err, qt.IsNil)
			current, currentReport, err := atlasfilter.ExcludeDatabaseReport(ownerSelections(),
				&catalog.Database{Schemas: []catalog.Schema{{Name: "app"}}, FeatureObjects: heldTypes()}, test.exclude, "public")
			c.Assert(err, qt.IsNil)

			c.Assert(typeNames(c, generated.FeatureObjects), qt.ContentEquals, test.want)
			c.Assert(typeNames(c, current.FeatureObjects), qt.ContentEquals, test.want)
			c.Assert(generatedReport.Unmatched, qt.HasLen, 0)
			c.Assert(currentReport.Unmatched, qt.HasLen, 0)
		})
	}
}

// TestScopeGenerated_KeepsATypeAKeptColumnUses pins that a scope keeps an
// owner's type a kept column names, as it keeps a common type, and the types
// its selectors name. A kept table whose column type left the selection
// would describe a table the description cannot create.
func TestScopeGenerated_KeepsATypeAKeptColumnUses(t *testing.T) {
	tests := []struct {
		name    string
		include []string
		want    []string
	}{
		{name: "a table using a qualified domain and a composite type", include: []string{"users[type=table]"},
			want: []string{"composite type:addr", "domain:email"}},
		{name: "a selected range type", include: []string{"span[type=range]"}, want: []string{"range type:span"}},
		{name: "nothing that names a type", include: []string{"other[type=table]"}, want: make([]string, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "User", Name: "users"}, {StructName: "Other", Name: "other"}},
				Fields: []schemamodel.Field{{StructName: "User", Name: "email", Type: "app.email"}, {StructName: "User", Name: "home", Type: "addr[]"},
					{StructName: "Other", Name: "n", Type: "integer"}},
				FeatureObjects: declaredTypes(),
			}

			scoped, err := atlasfilter.ScopeGenerated(db, atlasfilter.Scope{Include: test.include, DefaultSchema: "public",
				Selections: ownerSelections()})

			c.Assert(err, qt.IsNil)
			c.Assert(typeNames(c, scoped.FeatureObjects), qt.ContentEquals, test.want)
		})
	}
}

// TestScopeDatabase_KeepsADomainAKeptColumnUses pins the read side: a column
// declared with a domain reports the domain by its domain name, and the
// scope keeps the owner's domain of that name with the column's table.
func TestScopeDatabase_KeepsADomainAKeptColumnUses(t *testing.T) {
	c := qt.New(t)
	db := &catalog.Database{
		Tables: []catalog.Table{{Schema: "public", Name: "users", Columns: []catalog.Column{
			{Name: "email", DataType: "text", DomainName: "email", DomainSchema: "app"},
			{Name: "home", DataType: "addr"},
		}}},
		FeatureObjects: heldTypes(),
	}

	scoped, err := atlasfilter.ScopeDatabase(db, atlasfilter.Scope{Include: []string{"users[type=table]"}, DefaultSchema: "public",
		Selections: ownerSelections()})

	c.Assert(err, qt.IsNil)
	c.Assert(typeNames(c, scoped.FeatureObjects), qt.ContentEquals, []string{"composite type:addr", "domain:email"})
}

// TestScopeGenerated_RefusesAKeptColumnWhoseTypeIsExcluded pins that a scope
// refuses to keep a column whose owner's type an exclusion took away, naming
// the type as it names a common one.
func TestScopeGenerated_RefusesAKeptColumnWhoseTypeIsExcluded(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables:         []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields:         []schemamodel.Field{{StructName: "User", Name: "email", Type: "app.email"}},
		FeatureObjects: declaredTypes(),
	}

	scoped, err := atlasfilter.ScopeGenerated(db, atlasfilter.Scope{Include: []string{"users[type=table]"}, Exclude: []string{"app.email[type=domain]"},
		DefaultSchema: "public", Selections: ownerSelections()})

	c.Assert(err, qt.ErrorMatches, `(?s).*selected tables use domain "app.email", but "app.email" is not selected.*`)
	c.Assert(scoped, qt.IsNil)
}
