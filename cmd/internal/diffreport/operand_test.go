package diffreport_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/diffreport"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestCategories_AnOperandIsNotADifference is the property the report lost when
// the diff gained a context field.
//
// A planner reads DeclaredTables to resolve a foreign key's target; it is not
// something the comparison found. Reported as a category, it made `ptah schema
// compare` answer "Differences detected (1 category)" for a schema that matched
// its database, and then warn that the planner produced no statements for it --
// the shape stokaro/ptah#1284 is about, arrived at from the other side
// (stokaro/ptah#2315).
func TestCategories_AnOperandIsNotADifference(t *testing.T) {
	c := qt.New(t)

	categories := diffreport.Categories(&difftypes.SchemaDiff{
		DeclaredTables: []schemamodel.Table{{Name: "orders", StructName: "Order"}},
	})

	c.Assert(categories, qt.HasLen, 0)
}

// TestCategories_AChangeIsStillADifference is the control.
//
// A predicate that excluded too much would pass the test above by reporting
// nothing at all, which is the failure #1284 recorded: a category no path reads
// renders as nothing, and nothing is indistinguishable from agreement.
func TestCategories_AChangeIsStillADifference(t *testing.T) {
	c := qt.New(t)

	categories := diffreport.Categories(&difftypes.SchemaDiff{
		TablesRemoved: []string{"orders"},
	})

	c.Assert(categories, qt.HasLen, 1)
	c.Assert(categories[0].Name, qt.Equals, "tables_removed")
}

// TestSchemaDiff_EveryOperandIsOffTheWire holds the two marks together.
//
// An operand is context a planner reads, so it has nothing to say to a reader
// of a stored plan. A field marked one and still serialized would be shipping
// the diff's working state to consumers, and a reader would have no way to know
// it was not a change.
func TestSchemaDiff_EveryOperandIsOffTheWire(t *testing.T) {
	c := qt.New(t)

	operands := operandFields()

	c.Assert(operands, qt.Not(qt.HasLen), 0,
		qt.Commentf("no field carries the operand mark, so this test measures nothing"))
	for _, field := range operands {
		c.Assert(field.Tag.Get("json"), qt.Equals, "-",
			qt.Commentf("%s is marked an operand and is on the wire", field.Name))
	}
}

// operandFields returns the SchemaDiff fields marked as planner operands.
func operandFields() []reflect.StructField {
	fields := make([]reflect.StructField, 0)
	for field := range reflect.TypeFor[difftypes.SchemaDiff]().Fields() {
		if field.Tag.Get("diff") == "operand" {
			fields = append(fields, field)
		}
	}
	return fields
}
