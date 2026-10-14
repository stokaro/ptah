package schemacapture_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
)

func TestDeclareTableKeepsExplicitConstraintOwnership(t *testing.T) {
	for _, structName := range []string{"", "Shared"} {
		t.Run("struct="+structName, func(t *testing.T) {
			c := qt.New(t)
			table := schemamodel.Table{Schema: "app", Name: "events", StructName: structName}
			source := &schemamodel.Database{Tables: []schemamodel.Table{table}, Constraints: []schemamodel.Constraint{
				{Name: "foreign_key", Type: "PRIMARY KEY", Table: "other", StructName: structName, Columns: []string{"other_id"}},
				{Name: "own_key", Type: "PRIMARY KEY", Table: "app.events", StructName: structName, Columns: []string{"id"}},
			}}
			capture := schemacapture.DeclareTable(source, table, identifier.ForDialect("clickhouse"))
			c.Assert(capture.Constraints, qt.HasLen, 1)
			c.Assert(capture.Constraints[0].Name, qt.Equals, "own_key")
			capture.Constraints[0].Columns[0] = "changed"
			c.Assert(source.Constraints[1].Columns, qt.DeepEquals, []string{"id"})
		})
	}
}

func TestConstraintsForUsesStructOnlyWithoutExplicitTable(t *testing.T) {
	c := qt.New(t)
	source := []schemamodel.Constraint{
		{Name: "by_struct", StructName: "Event"},
		{Name: "foreign", StructName: "Event", Table: "other"},
		{Name: "by_table", StructName: "Other", Table: "events"},
		{Name: "unowned"},
	}
	result := schemacapture.ConstraintsFor(source, schemamodel.Table{Name: "events", StructName: "Event"})
	c.Assert(result, qt.DeepEquals, []schemamodel.Constraint{source[0], source[2]})
}

// TestOwnsKeepsABareReferenceInItsSchema pins which table a bare reference
// reaches. A read of two schemas holding children writes the default schema's
// constraint with the bare name, and that constraint is not the one of
// app.children, whose struct differs.
func TestOwnsKeepsABareReferenceInItsSchema(t *testing.T) {
	table := schemamodel.Table{Schema: "app", Name: "children", StructName: "AppChildren"}
	for _, test := range []struct {
		name       string
		constraint schemamodel.Constraint
		want       bool
	}{
		{name: "qualified reference", constraint: schemamodel.Constraint{Table: "app.children", StructName: "Children"}, want: true},
		{name: "bare reference from another struct", constraint: schemamodel.Constraint{Table: "children", StructName: "Children"}, want: false},
		{name: "bare reference from the table's struct", constraint: schemamodel.Constraint{Table: "children", StructName: "AppChildren"}, want: true},
		{name: "bare reference without a struct", constraint: schemamodel.Constraint{Table: "children"}, want: true},
		{name: "the table's struct alone", constraint: schemamodel.Constraint{StructName: "AppChildren"}, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemacapture.Owns(table, test.constraint), qt.Equals, test.want)
		})
	}
}

// TestDeclareTableSeparatesSameNamedChildrenOfTwoSchemas captures app.children
// from a schema where the default schema's children carries a constraint and
// an index of the same names. Each stays with its own table, so neither
// capture holds a name twice.
func TestDeclareTableSeparatesSameNamedChildrenOfTwoSchemas(t *testing.T) {
	c := qt.New(t)
	app := schemamodel.Table{Schema: "app", Name: "children", StructName: "AppChildren"}
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{app, {Name: "children", StructName: "Children"}},
		Constraints: []schemamodel.Constraint{
			{Name: "children_parent", Type: "FOREIGN KEY", Table: "app.children", StructName: "AppChildren"},
			{Name: "children_parent", Type: "FOREIGN KEY", Table: "children", StructName: "Children"},
		},
		Indexes: []schemamodel.Index{
			{Name: "children_idx", TableName: "app.children", StructName: "AppChildren"},
			{Name: "children_idx", TableName: "children", StructName: "Children"},
		},
	}

	capture := schemacapture.DeclareTable(source, app, identifier.ForDialect("postgres"))

	c.Assert(capture.Constraints, qt.DeepEquals, source.Constraints[:1])
	c.Assert(capture.Indexes, qt.DeepEquals, source.Indexes[:1])
}
