package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

const parentFacetKind schemaext.Kind = "example.org/parent"

// parentFacet is a table facet value that requires another table.
type parentFacet struct{ schema, name string }

func (*parentFacet) Kind() schemaext.Kind     { return parentFacetKind }
func (v *parentFacet) Clone() schemaext.Value { return &parentFacet{schema: v.schema, name: v.name} }
func (v *parentFacet) Equal(other schemaext.Value) bool {
	w, ok := other.(*parentFacet)
	return ok && *v == *w
}
func (*parentFacet) DefinesTable() bool                   { return true }
func (v *parentFacet) ParentTable() (schema, name string) { return v.schema, v.name }

func childOf(structName, schema, name, parentSchema, parent string) schemamodel.Table {
	return schemamodel.Table{StructName: structName, Schema: schema, Name: name,
		Facets: must.Must(schemaext.NewFacets(&parentFacet{schema: parentSchema, name: parent}))}
}

func TestFacetParentTable_ResolvesAsTheDeclarationsSpellIt(t *testing.T) {
	parent := schemamodel.Table{StructName: "P", Name: "p"}
	qualified := schemamodel.Table{StructName: "Q", Schema: "app", Name: "q"}
	for _, test := range []struct {
		name  string
		child schemamodel.Table
		want  string
	}{
		{name: "the same spelling", child: childOf("C", "", "c", "", "p"), want: "p"},
		{name: "a default schema written on the reference alone", child: childOf("C", "", "c", "public", "p"), want: "p"},
		{name: "a default schema written on the declaration alone", child: childOf("C", "", "c", "", "q"), want: "app.q"},
		{name: "a qualified match", child: childOf("C", "other", "c", "app", "q"), want: "app.q"},
		{name: "a parent no table declares stays as written", child: childOf("C", "", "c", "x", "missing"), want: "x.missing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, found := schemamodel.FacetParentTable([]schemamodel.Table{parent, qualified, test.child}, test.child)

			c.Assert(found, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestFacetParentTable_NoFacetNamesNoParent(t *testing.T) {
	c := qt.New(t)
	table := schemamodel.Table{StructName: "P", Name: "p"}

	got, found := schemamodel.FacetParentTable([]schemamodel.Table{table}, table)

	c.Assert(found, qt.IsFalse)
	c.Assert(got, qt.Equals, "")
}

func TestFacetAncestors(t *testing.T) {
	root := schemamodel.Table{StructName: "R", Name: "r"}
	middle := childOf("M", "", "m", "", "r")
	leaf := childOf("L", "", "l", "", "m")
	orphan := childOf("O", "", "o", "", "missing")
	cycleA := childOf("A", "", "a", "", "b")
	cycleB := childOf("B", "", "b", "", "a")
	tables := []schemamodel.Table{root, middle, leaf, orphan, cycleA, cycleB}
	for _, test := range []struct {
		name     string
		table    schemamodel.Table
		want     []string
		complete bool
	}{
		{name: "a table with no parent", table: root, complete: true},
		{name: "two levels, nearest first", table: leaf, want: []string{"m", "r"}, complete: true},
		{name: "a parent no table declares", table: orphan},
		{name: "a cycle ends", table: cycleA, want: []string{"b"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			ancestors, complete := schemamodel.FacetAncestors(tables, test.table)

			var names []string
			for _, ancestor := range ancestors {
				names = append(names, ancestor.Name)
			}
			c.Assert(names, qt.DeepEquals, test.want)
			c.Assert(complete, qt.Equals, test.complete)
		})
	}
}

func TestWithParentColumns_CopiesColumnsWithoutTheirForeignKeys(t *testing.T) {
	c := qt.New(t)
	tables := []schemamodel.Table{{StructName: "R", Name: "r"}, childOf("M", "", "m", "", "r"), childOf("L", "", "l", "", "m")}
	fields := []schemamodel.Field{{StructName: "R", Name: "id", Type: "INT", Foreign: "o(id)", ForeignKeyName: "fk", OnDelete: "CASCADE"}}

	got := schemamodel.WithParentColumns(tables, fields)

	c.Assert(got, qt.DeepEquals, []schemamodel.Field{
		fields[0],
		{StructName: "M", Name: "id", Type: "INT"},
		{StructName: "L", Name: "id", Type: "INT"},
	})
	c.Assert(fields, qt.HasLen, 1)
}

func TestColumnsComeFromElsewhere(t *testing.T) {
	declared := schemamodel.Table{StructName: "R", Name: "r"}
	child := childOf("C", "", "c", "", "r")
	stated := []schemamodel.Field{{StructName: "R", Name: "id"}, {StructName: "C", Name: "id"}}
	for _, test := range []struct {
		name   string
		table  schemamodel.Table
		fields []schemamodel.Field
		want   bool
	}{
		{name: "a defined table with no stated column", table: child, want: true},
		{name: "a defined table with stated columns", table: child, fields: stated},
		{name: "an ordinary table with no column", table: declared},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemamodel.ColumnsComeFromElsewhere(test.fields, test.table), qt.Equals, test.want)
		})
	}
}

func TestFinalize_OrdersATableAfterTheParentAFacetNames(t *testing.T) {
	c := qt.New(t)
	database := schemamodel.Database{Tables: []schemamodel.Table{childOf("C", "", "c", "", "p"), {StructName: "P", Name: "p"}}}

	schemamodel.Finalize(&database)

	c.Assert(database.Dependencies["c"], qt.DeepEquals, []string{"p"})
	names := make([]string, 0, 2)
	for _, table := range database.Tables {
		names = append(names, table.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{"p", "c"})
}
