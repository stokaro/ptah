package droppedviews_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/droppedviews"
)

func views(names ...string) difftypes.ViewChanges {
	var changes difftypes.ViewChanges
	for _, name := range names {
		changes = append(changes, schemamodel.View{Name: name})
	}
	return changes
}

func materialized(names ...string) difftypes.MaterializedViewChanges {
	var changes difftypes.MaterializedViewChanges
	for _, name := range names {
		changes = append(changes, schemamodel.MaterializedView{Name: name})
	}
	return changes
}

// TestSplit separates the removals an addition of the other kind replaces
// from the ones the plan drops for good, matching a name across the two
// spellings a diff can carry, and orders each set as its doc says.
func TestSplit(t *testing.T) {
	semantics := identifier.ForDialect(platform.ClickHouse)
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		replaced []droppedviews.Removal
		gone     []droppedviews.Removal
	}{
		{name: "no diff"},
		{name: "every kind dropped for good", diff: &difftypes.SchemaDiff{ViewsRemoved: views("v1", "v2"), MaterializedViewsRemoved: materialized("m1")},
			gone: []droppedviews.Removal{{Name: "v1"}, {Name: "v2"}, {Name: "m1", Materialized: true}}},
		{name: "each kind replaced by the other",
			diff:     &difftypes.SchemaDiff{ViewsRemoved: views("a", "kept"), MaterializedViewsRemoved: materialized("b"), ViewsAdded: views("b"), MaterializedViewsAdded: materialized("a")},
			replaced: []droppedviews.Removal{{Name: "b", Materialized: true}, {Name: "a"}}, gone: []droppedviews.Removal{{Name: "kept"}}},
		{name: "a qualified name matches its unqualified spelling",
			diff:     &difftypes.SchemaDiff{ViewsRemoved: views("d1.v"), MaterializedViewsAdded: materialized("v")},
			replaced: []droppedviews.Removal{{Name: "d1.v"}}},
		{name: "a view of the same kind is not a replacement", diff: &difftypes.SchemaDiff{ViewsRemoved: views("v"), ViewsAdded: views("v")},
			gone: []droppedviews.Removal{{Name: "v"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			replaced, gone := droppedviews.Split(test.diff, semantics)
			c.Assert(replaced, qt.DeepEquals, test.replaced)
			c.Assert(gone, qt.DeepEquals, test.gone)
		})
	}
}

// TestHolds matches a relation to a removal under the identifier rules, and
// a qualified name to its unqualified spelling.
func TestHolds(t *testing.T) {
	semantics := identifier.ForDialect(platform.ClickHouse)
	removals := []droppedviews.Removal{{Name: "d1.v"}, {Name: "m", Materialized: true}}
	tests := []struct {
		name string
		want bool
	}{
		{name: "d1.v", want: true},
		{name: "v", want: true},
		{name: "m", want: true},
		{name: "V", want: false},
		{name: "d2.v", want: false},
		{name: "other", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(droppedviews.Holds(removals, test.name, semantics), qt.Equals, test.want)
		})
	}
}

// TestVacated keeps a removal dropped for good whose name no view addition
// takes back, and leaves out a replacement and a rebuild of the same kind.
func TestVacated(t *testing.T) {
	semantics := identifier.ForDialect(platform.ClickHouse)
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []droppedviews.Removal
	}{
		{name: "no diff"},
		{name: "dropped for good", diff: &difftypes.SchemaDiff{ViewsRemoved: views("v"), MaterializedViewsRemoved: materialized("m")},
			want: []droppedviews.Removal{{Name: "v"}, {Name: "m", Materialized: true}}},
		{name: "replaced by the other kind", diff: &difftypes.SchemaDiff{ViewsRemoved: views("v"), MaterializedViewsAdded: materialized("v")}},
		{name: "rebuilt as the same kind", diff: &difftypes.SchemaDiff{ViewsRemoved: views("d1.v"), ViewsAdded: views("v"),
			MaterializedViewsRemoved: materialized("m"), MaterializedViewsAdded: materialized("m")}},
		{name: "a table takes the name", diff: &difftypes.SchemaDiff{ViewsRemoved: views("v"), TablesAdded: difftypes.TableChanges{{Name: "v"}}},
			want: []droppedviews.Removal{{Name: "v"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(droppedviews.Vacated(test.diff, semantics), qt.DeepEquals, test.want)
		})
	}
}

// TestBelow matches an object to the removal its parent names, qualified by
// the database its schema names when the source named one, and leaves out an
// object with no parent.
func TestBelow(t *testing.T) {
	semantics := identifier.ForDialect(platform.ClickHouse)
	removals := []droppedviews.Removal{{Name: "d1.v"}}
	builder := objectidentity.NewBuilder(semantics)
	tests := []struct {
		name    string
		subject objectidentity.ID
		want    bool
	}{
		{name: "the parent in the removal's database", subject: objectidentity.ID{Schema: builder.Table("d1.v").Schema, Parent: builder.Table("d1.v").Name}, want: true},
		{name: "the parent with no database", subject: objectidentity.ID{Parent: builder.Table("v").Name}, want: true},
		{name: "the parent in another database", subject: objectidentity.ID{Schema: builder.Table("d2.v").Schema, Parent: builder.Table("d2.v").Name}},
		{name: "another parent", subject: objectidentity.ID{Parent: builder.Table("w").Name}},
		{name: "no parent", subject: objectidentity.ID{Name: builder.Table("v").Name}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(droppedviews.Below(removals, test.subject, semantics), qt.Equals, test.want)
		})
	}
}
