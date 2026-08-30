package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestGenerate_ASelfReferencingForeignKeyIsPlannedWithoutFinalize is
// stokaro/ptah#2471, at the level the defect was observed.
//
// A declaration assembled in memory rather than read through one of the readers
// lost the constraint: the table was created, the plan reported success, and
// `fk_nodes_parent` was not there. `core/schemamodel` is a stable public
// package, so building a Database is a supported thing to do -- and everything
// downstream read a map only [schemamodel.Finalize] fills.
func TestGenerate_ASelfReferencingForeignKeyIsPlannedWithoutFinalize(t *testing.T) {
	c := qt.New(t)
	desired := selfReferencingDeclaration()

	sql, err := planner.GenerateSchemaDiffSQL(&difftypes.SchemaDiff{
		TablesAdded: difftypes.TableCreationsFor(desired, "nodes"),
	}, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "fk_nodes_parent")
	c.Assert(sql, qt.Contains, `REFERENCES "nodes"("id")`)
}

// TestGenerate_AFinalizedDeclarationPlansItOnce is the control the union needs.
//
// Finalize fills the map from the same fields the derivation reads, so a
// declaration that has been through it offers every self-reference twice. One
// constraint, planned once.
func TestGenerate_AFinalizedDeclarationPlansItOnce(t *testing.T) {
	c := qt.New(t)
	desired := selfReferencingDeclaration()
	schemamodel.Finalize(desired)

	sql, err := planner.GenerateSchemaDiffSQL(&difftypes.SchemaDiff{
		TablesAdded: difftypes.TableCreationsFor(desired, "nodes"),
	}, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(countOccurrences(sql, "fk_nodes_parent"), qt.Equals, 1)
}

// selfReferencingDeclaration is one table whose parent column names itself.
func selfReferencingDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Node", Name: "nodes"}},
		Fields: []schemamodel.Field{
			{StructName: "Node", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Node", Name: "parent_id", Type: "BIGINT",
				Foreign: "nodes(id)", ForeignKeyName: "fk_nodes_parent"},
		},
	}
}

// countOccurrences counts non-overlapping appearances of a substring.
func countOccurrences(haystack, needle string) int {
	count := 0
	for rest := haystack; ; {
		index := indexOf(rest, needle)
		if index < 0 {
			return count
		}
		count++
		rest = rest[index+len(needle):]
	}
}

// indexOf is strings.Index, named so the loop above reads as counting.
func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
