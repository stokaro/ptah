package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// selfReferenceTable declares one table whose foreign key is a table-level
// constraint pointing at the table named by foreignTable.
func selfReferenceTable(columns, foreignColumns []string, foreignTable string) *schemamodel.Database {
	fields := []schemamodel.Field{
		{StructName: "Node", Name: "id", Type: "INTEGER", Primary: true},
		{StructName: "Node", Name: "a", Type: "INTEGER", Primary: true},
		{StructName: "Node", Name: "b", Type: "INTEGER", Primary: true},
	}
	for _, column := range columns {
		fields = append(fields, schemamodel.Field{StructName: "Node", Name: column, Type: "INTEGER"})
	}
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Node", Name: "nodes"},
			{StructName: "Other", Name: "others"},
		},
		Fields: append(fields,
			schemamodel.Field{StructName: "Other", Name: "a", Type: "INTEGER", Primary: true},
			schemamodel.Field{StructName: "Other", Name: "b", Type: "INTEGER", Primary: true},
		),
		Constraints: []schemamodel.Constraint{{
			StructName: "Node", Table: "nodes", Name: "nodes_owner_fk",
			Type: "FOREIGN KEY", Columns: columns,
			ForeignTable: foreignTable, ForeignColumns: foreignColumns,
		}},
	}
}

// countForeignKeyStatements counts the planned statements that add a foreign
// key, which is the number the object should appear as.
func countForeignKeyStatements(c *qt.C, desired *schemamodel.Database, dialect string) (int, string) {
	c.Helper()
	diff := schemadiff.CompareWithDialect(desired, &catalog.Database{}, dialect)
	sql, err := planner.GenerateSchemaDiffSQL(diff, dialect)
	c.Assert(err, qt.IsNil)
	count := 0
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.Contains(line, "FOREIGN KEY") {
			count++
		}
	}
	return count, sql
}

// TestPlan_SelfReferencingTableConstraintIsEmittedOnce covers
// stokaro/ptah#2583.
//
// The object was carried by two pools -- the constraint list and the derived
// self-reference set -- and both were emitted. The single-column form produced
// two identical ALTERs under one constraint name, which the server refuses as a
// duplicate; the composite form produced a second statement naming the column
// `"owner_a, owner_b"`, which no table has, because SelfReferencingFK holds one
// column and joining a list is not a representation of it.
func TestPlan_SelfReferencingTableConstraintIsEmittedOnce(t *testing.T) {
	tests := []struct {
		name        string
		columns     []string
		foreignCols []string
	}{
		{name: "single column", columns: []string{"parent_id"}, foreignCols: []string{"id"}},
		{name: "composite", columns: []string{"owner_a", "owner_b"}, foreignCols: []string{"a", "b"}},
	}

	for _, test := range tests {
		for _, dialect := range []string{platform.Postgres, platform.MySQL} {
			t.Run(test.name+"/"+dialect, func(t *testing.T) {
				c := qt.New(t)
				desired := selfReferenceTable(test.columns, test.foreignCols, "nodes")

				count, sql := countForeignKeyStatements(c, desired, dialect)

				c.Assert(count, qt.Equals, 1, qt.Commentf("plan:\n%s", sql))
			})
		}
	}
}

// TestPlan_CompositeSelfReferenceNamesNoJoinedColumn is the other half of the
// defect, and it needs the composite shape to be visible at all.
//
// SelfReferencingFK holds ONE column, so the derivation joined the list into
// it and the second statement asked for a column named `owner_a, owner_b`.
// Counting statements alone would not have caught that: a plan emitting the
// mangled statement INSTEAD of the correct one still counts one.
func TestPlan_CompositeSelfReferenceNamesNoJoinedColumn(t *testing.T) {
	c := qt.New(t)
	desired := selfReferenceTable([]string{"owner_a", "owner_b"}, []string{"a", "b"}, "nodes")

	_, sql := countForeignKeyStatements(c, desired, platform.Postgres)

	c.Assert(sql, qt.Not(qt.Contains), `"owner_a, owner_b"`)
	c.Assert(sql, qt.Not(qt.Contains), `"a, b"`)
	c.Assert(sql, qt.Contains, `FOREIGN KEY ("owner_a", "owner_b") REFERENCES "nodes"("a", "b")`)
}

// TestPlan_ForeignKeyToAnotherTableIsUnchanged is the control.
//
// The same declaration pointing at a different table never reached the
// self-reference path, so it was correct before and has to stay correct: a fix
// that emitted nothing would pass the assertion above.
func TestPlan_ForeignKeyToAnotherTableIsUnchanged(t *testing.T) {
	c := qt.New(t)
	desired := selfReferenceTable([]string{"owner_a", "owner_b"}, []string{"a", "b"}, "others")

	count, sql := countForeignKeyStatements(c, desired, platform.Postgres)

	c.Assert(count, qt.Equals, 1, qt.Commentf("plan:\n%s", sql))
	c.Assert(sql, qt.Contains, `REFERENCES "others"`)
}
