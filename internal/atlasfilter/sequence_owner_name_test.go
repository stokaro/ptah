package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

// The owner of a sequence is written as a column reference, a name quoted when
// a bare one would read back as another name: the readers write the column n.x
// of the table items as `items."n.x"`, and the column n"x as `items."n""x"`.
// Split on every dot, the first owner names a table `"n` in a schema items,
// and the second a column `"n""x"`, so the sequence leaves the selection of its
// table and stays behind the exclusion of its table or its column.

func namedOwnerDatabase(column, ownedBy string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "items", Schema: "public", Columns: []catalog.Column{{Name: column, DataType: "integer"}}},
			{Name: "other", Schema: "public", Columns: []catalog.Column{{Name: "id", DataType: "integer"}}},
		},
		Sequences: []catalog.Sequence{{Name: "s", Schema: "public", OwnedBy: ownedBy}},
	}
}

func namedOwnerDesired(column, ownedBy string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Items", Name: "items"}, {StructName: "Other", Name: "other"}},
		Fields: []schemamodel.Field{
			{StructName: "Items", Name: column, Type: "integer"},
			{StructName: "Other", Name: "id", Type: "integer"},
		},
		Sequences: []schemamodel.Sequence{{StructName: "Seq", Name: "s", OwnedBy: ownedBy}},
	}
}

// TestInclude_SequenceOwnedByAQuotedColumnRidesWithItsTable includes the table
// that owns the sequence.
func TestInclude_SequenceOwnedByAQuotedColumnRidesWithItsTable(t *testing.T) {
	c := qt.New(t)
	scope := atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"items"}, DefaultSchema: "public"}

	gotDatabase, err := atlasfilter.ScopeDatabase(namedOwnerDatabase("n.x", `items."n.x"`), scope)
	c.Assert(err, qt.IsNil)
	gotDesired, err := atlasfilter.ScopeGenerated(namedOwnerDesired("n.x", `items."n.x"`), scope)
	c.Assert(err, qt.IsNil)

	c.Assert(databaseSequenceNames(gotDatabase.Sequences), qt.DeepEquals, []string{"public.s"})
	c.Assert(generatedSequenceNames(gotDesired.Sequences), qt.DeepEquals, []string{"s"})
}

// TestExclude_SequenceOwnedByAQuotedColumnLeavesWithItsOwner excludes the table
// or the column that owns the sequence.
func TestExclude_SequenceOwnedByAQuotedColumnLeavesWithItsOwner(t *testing.T) {
	tests := []struct {
		name, column, ownedBy, exclude string
	}{
		{name: "the table of a column holding a dot", column: "n.x", ownedBy: `items."n.x"`, exclude: "items"},
		{name: "a column holding a double quote", column: `n"x`, ownedBy: `items."n""x"`, exclude: `items.n"x`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			gotDatabase, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(ownerSelections(),
				namedOwnerDatabase(test.column, test.ownedBy), []string{test.exclude}, "public")
			c.Assert(err, qt.IsNil)
			gotDesired, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(ownerSelections(),
				namedOwnerDesired(test.column, test.ownedBy), []string{test.exclude}, "public")
			c.Assert(err, qt.IsNil)

			c.Assert(gotDatabase.Sequences, qt.HasLen, 0)
			c.Assert(gotDesired.Sequences, qt.HasLen, 0)
		})
	}
}

// anotherSchemaOwnerDatabase holds the sequence public.s, owned by the column id
// of app.items, beside a table public.items of the same name. A reader writes
// an owner in another schema with its schema, a link CockroachDB accepts.
func anotherSchemaOwnerDatabase() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "items", Schema: "app", Columns: []catalog.Column{{Name: "id", DataType: "integer"}}},
			{Name: "items", Schema: "public", Columns: []catalog.Column{{Name: "id", DataType: "integer"}}},
		},
		Sequences: []catalog.Sequence{{Name: "s", Schema: "public", OwnedBy: "app.items.id"}},
	}
}

// TestScopeDatabase_SequenceOwnedInAnotherSchemaRidesWithItsTable includes one of
// the two tables. The sequence rides with the table that owns it, not with the
// table of the same name in the sequence's own schema.
func TestScopeDatabase_SequenceOwnedInAnotherSchemaRidesWithItsTable(t *testing.T) {
	tests := []struct {
		name, include string
		want          []string
	}{
		{name: "the owner", include: "app.items", want: []string{"public.s"}},
		{name: "the table of the same name", include: "public.items", want: make([]string, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := atlasfilter.Scope{Selections: ownerSelections(), Include: []string{test.include}}

			got, err := atlasfilter.ScopeDatabase(anotherSchemaOwnerDatabase(), scope)

			c.Assert(err, qt.IsNil)
			c.Assert(databaseSequenceNames(got.Sequences), qt.DeepEquals, test.want)
		})
	}
}

// TestExcludeDatabase_SequenceOwnedInAnotherSchemaLeavesWithItsOwner excludes one
// of the two tables, or the owning column. The sequence leaves with its owner and
// stays behind the exclusion of the table of the same name.
func TestExcludeDatabase_SequenceOwnedInAnotherSchemaLeavesWithItsOwner(t *testing.T) {
	tests := []struct {
		name, exclude string
		want          []string
	}{
		{name: "the owner", exclude: "app.items", want: make([]string, 0)},
		{name: "the owning column", exclude: "app.items.id", want: make([]string, 0)},
		{name: "the table of the same name", exclude: "public.items", want: []string{"public.s"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabase(ownerSelections(), anotherSchemaOwnerDatabase(), []string{test.exclude})

			c.Assert(err, qt.IsNil)
			c.Assert(databaseSequenceNames(got.Sequences), qt.DeepEquals, test.want)
		})
	}
}
