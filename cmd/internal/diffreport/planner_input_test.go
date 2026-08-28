package diffreport_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/diffreport"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestCategoriesLeavesOutEveryListTheWireDoesNotCarry is the property that keeps
// a planner input out of a difference report.
//
// A diff carries two kinds of list. Most are observations -- what the two
// schemas disagree about -- and each has a name on the wire. A few are inputs a
// planner reads while rendering: the declaration's tables, so a foreign key can
// name the table it points at, and the policy identity collisions the planner
// refuses on. Those are marked `json:"-"`, and they are populated on an ordinary
// comparison of two schemas that AGREE.
//
// Reporting one is therefore not a cosmetic defect. `ptah schema compare` against
// a converged database printed `DeclaredTables (2): dbml_authors dbml_authors,
// dbml_books dbml_books` and told the operator the schemas differ
// (stokaro/ptah#2315).
//
// The fields are discovered rather than listed, so a list added to SchemaDiff
// without a wire name is covered by this the moment it exists.
func TestCategoriesLeavesOutEveryListTheWireDoesNotCarry(t *testing.T) {
	fields := wireLessListFields()

	c := qt.New(t)
	c.Assert(fields, qt.Not(qt.HasLen), 0,
		qt.Commentf("no `json:\"-\"` list found on SchemaDiff; this test is asserting nothing"))

	for _, field := range fields {
		t.Run(field.Name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			reflect.ValueOf(diff).Elem().FieldByIndex(field.Index).Set(reflect.MakeSlice(field.Type, 2, 2))

			c.Assert(diffreport.Names(diffreport.Categories(diff)), qt.HasLen, 0)
			c.Assert(diff.HasChanges(), qt.IsFalse,
				qt.Commentf("a planner input must not make a converged comparison answer that it has changes"))
		})
	}
}

// TestCategoriesStillNamesAListTheWireCarries is the control for the test above.
//
// Without it, excluding every list would satisfy the property and delete the
// report. This pins that a list with a wire name is still reported under it.
func TestCategoriesStillNamesAListTheWireCarries(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{TablesRemoved: []string{"orders"}}

	c.Assert(diffreport.Names(diffreport.Categories(diff)), qt.DeepEquals, []string{"tables_removed"})
}

// wireLessListFields returns the SchemaDiff lists marked `json:"-"`.
func wireLessListFields() []reflect.StructField {
	structType := reflect.TypeFor[difftypes.SchemaDiff]()
	var fields []reflect.StructField
	for field := range structType.Fields() {
		if !field.IsExported() || field.Type.Kind() != reflect.Slice {
			continue
		}
		tag, ok := field.Tag.Lookup("json")
		if !ok {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == "-" {
			fields = append(fields, field)
		}
	}
	slices.SortFunc(fields, func(a, b reflect.StructField) int { return strings.Compare(a.Name, b.Name) })
	return fields
}
