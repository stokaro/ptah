package generator

// White-box testing required: this is a census over the reverse*Diffs builders,
// which are package-local, and it reads this package's own source to find them.

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/schemadiff/difftypes"
)

// nestedCoverageExempt names the element fields no reversal builder mentions,
// with the reason. An entry here is a decision; an omission is a field silently
// dropped.
var nestedCoverageExempt = map[string]string{
	// Nothing populates these. A constraint change is recorded in the
	// TOP-LEVEL ConstraintsAdded/ConstraintsRemoved and their WithTables
	// siblings; measured, a real constraint removal fills the top-level list
	// and leaves these empty (stokaro/ptah#2315).
	"TableDiff.ConstraintsAdded":   "no producer writes it",
	"TableDiff.ConstraintsRemoved": "no producer writes it",

	// The names are the SERVER's. compare.usertypes fills this from
	// catalog.Domain's constraint rows, and a declaration carries a CHECK
	// EXPRESSION with no name at all -- schemamodel.Domain has `Check` and
	// nothing to hold what PostgreSQL called it. The pre-change schema is
	// reached through the same conversion and loses them the same way, so
	// there is nowhere for a reversal to read them from.
	//
	// The consequence is real and is not this gate's to fix: a reversed CHECK
	// change emits the ADD with no DROP CONSTRAINT beside it. Carrying the
	// names would need the reversal to be handed the catalog rather than a
	// schema converted from it.
	"DomainDiff.CurrentCheckConstraints": "the constraint names are server-chosen and no schema carries them",

	// Filling it would ADD a copy rather than restore a missing one. A
	// self-reference declared as a table-level constraint is already emitted
	// twice on the FORWARD path -- once from the table's constraints and once
	// from this list -- and a composite one's second copy quotes the whole
	// column list as a single identifier, naming a column that does not exist.
	// Measured on postgres against master, and identical at cf84d08d7^, so it
	// predates the constraint carry (stokaro/ptah#2583).
	//
	// The reverse path fills DependsOn, which emits nothing of its own and
	// only feeds InDependencyOrder(). This one waits for #2583 to decide who
	// owns the emission and how a composite key is represented.
	"TableCreation.SelfReferencingForeignKeys": "already emitted twice on the forward path; stokaro/ptah#2583",

	// The names are the forward direction's, derived from the desired state.
	// A key the rollback puts back is one the pre-change database held under
	// its real name, and the reversal restores it from the removed constraint,
	// which carries that name. Measured on PostgreSQL 18.6: a column losing
	// its key beside a unique index c_x_key drops c_x_key1 on the way up, and
	// the down migration adds c_x_key1 back (stokaro/ptah#3859).
	"TableDiff.ColumnKeyNames": "the rollback restores the key from the removed constraint, which carries its name",
}

// TestReverseSchemaDiff_NamesEveryFieldOfEveryDiffElement is the half of the
// census the top-level one structurally cannot see.
//
// TestReverseSchemaDiff_AccountsForEverySchemaDiffField zeroes whole
// collections, so a collection that survives satisfies it even when its
// ELEMENTS lose fields on the way through. Measured before this test existed:
// reverseTableDiffs named four of TableDiff's nine fields, and a migration that
// changed a table's comment rolled back to "No rollback operations needed"
// while the top-level census stayed green (stokaro/ptah#2418).
//
// The property is a source one rather than a behavioral one, deliberately. A
// behavioral probe -- zero the field, expect the plan to change -- cannot tell
// a field the builder DROPS from one it OVERWRITES, and the direction-dependent
// operands are all overwritten (stokaro/ptah#2315). Reading the builder answers
// both: a field it neither reads nor assigns is not carried, whatever the
// runtime shows.
func TestReverseSchemaDiff_NamesEveryFieldOfEveryDiffElement(t *testing.T) {
	c := qt.New(t)

	named := fieldsNamedByReversalBuilders(c)
	c.Assert(len(named) > 0, qt.IsTrue,
		qt.Commentf("the source scan found no reversal builder; the gate would pass vacuously"))

	elements := reversedDiffElementTypes(c, named)
	c.Assert(len(elements) > 0, qt.IsTrue,
		qt.Commentf("reflection found no element types; the gate would pass vacuously"))

	for _, element := range elements {
		for field := range element.Fields() {
			name := element.Name() + "." + field.Name
			t.Run(name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(namedOrExempt(named, name), qt.IsTrue, qt.Commentf(
					"no reachable reversal builder names %s, so the down direction drops it. "+
						"Carry it through the reversal -- swapping the pair where the field is "+
						"a Desired/Current one -- or give it an entry in nestedCoverageExempt "+
						"saying why the down direction cannot.", name))
			})
		}
	}
}

// reversedDiffElementTypes is every diff element type a reversal builds, found
// by reflection over SchemaDiff rather than listed: a hand-written list is what
// rotted in the gate this one accompanies.
//
// A collection whose element type no builder mentions at all is skipped here
// and covered by the top-level census instead, which asks whether the whole
// collection reaches the plan.
func reversedDiffElementTypes(c *qt.C, named map[string]bool) []reflect.Type {
	diffType := reflect.TypeFor[difftypes.SchemaDiff]()
	var elements []reflect.Type
	seen := make(map[reflect.Type]struct{})
	for field := range diffType.Fields() {
		if field.Type.Kind() != reflect.Slice {
			continue
		}
		element := field.Type.Elem()
		if element.Kind() != reflect.Struct || element.NumField() == 0 {
			continue
		}
		if _, already := seen[element]; already {
			continue
		}
		// Only the element types a builder actually constructs. The others are
		// carried whole -- `ExtensionsAdded: diff.ExtensionsRemoved` -- and a
		// whole-slice carry cannot lose a field.
		if !elementTypeIsRebuilt(named, element) {
			continue
		}
		seen[element] = struct{}{}
		elements = append(elements, element)
	}
	return elements
}

// elementTypeIsRebuilt reports whether some builder constructs this element
// field by field, which is the only way a field can go missing.
func elementTypeIsRebuilt(named map[string]bool, element reflect.Type) bool {
	return named["type:"+element.Name()]
}

// namedOrExempt reports whether a reversal builder names this field, or whether
// the tree records a reason it cannot.
func namedOrExempt(named map[string]bool, name string) bool {
	_, exempt := nestedCoverageExempt[name]
	return named[name] || exempt
}
