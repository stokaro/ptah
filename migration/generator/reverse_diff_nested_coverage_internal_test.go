package generator

// White-box testing required: this is a census over the reverse*Diffs builders,
// which are package-local, and it reads this package's own source to find them.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
					"no reverse*Diffs builder names %s, so the down direction drops it. "+
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

// fieldsNamedByReversalBuilders reads this package's source for every
// `reverse<Something>Diffs` function and records the field names it mentions,
// plus a `type:<Element>` marker for each element type it constructs.
//
// It reads the identifiers rather than taking a list, because a list is the
// thing this test exists to make unnecessary.
func fieldsNamedByReversalBuilders(c *qt.C) map[string]bool {
	entries, err := os.ReadDir(".")
	c.Assert(err, qt.IsNil)

	named := make(map[string]bool)
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		c.Assert(err, qt.IsNil, qt.Commentf("parsing %s", name))
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || !isReversalBuilder(function.Name.Name) {
				continue
			}
			collectNamedFields(function, named)
		}
	}
	return named
}

// isReversalBuilder matches the two namings this package uses for code that
// assembles a diff element for the down direction.
//
// reverse<Something>Diffs is the per-collection builder. prior<Something> is
// the per-element one, which reads the pre-change schema for a single object,
// and it is here because leaving it out is what let TableCreation sit outside
// the census entirely while priorTableCreation filled four of its six fields
// (stokaro/ptah#2541).
//
// Both are conventions rather than a list, which is the property this gate
// exists to keep: a builder added under either name is covered by existing.
// Most prior* functions return a schemamodel type rather than a diff element,
// and reversedDiffElementTypes drops those on its own -- it only considers
// element types of SchemaDiff's own slices.
func isReversalBuilder(name string) bool {
	if strings.HasPrefix(name, "reverse") && strings.HasSuffix(name, "Diffs") {
		return true
	}
	return strings.HasPrefix(name, "prior")
}

// collectNamedFields records, PER ELEMENT TYPE, the fields a builder names,
// and marks the types it assembles field by field.
//
// Per type rather than a flat set of identifiers: `Name` appears in half the
// diff element types, and a flat set would let a builder that names one type's
// field vouch for every other type's field of the same name.
//
// Two shapes count, because this package writes both. A keyed composite
// literal names its fields directly. A literal bound to a variable that is
// then filled by assignment -- which is how priorTableCreation writes four of
// TableCreation's six fields -- names them one statement at a time, and
// reading only the literal credits it with `Name` alone.
//
// An EMPTY literal marks nothing. `return schemamodel.Sequence{}` is a
// zero-value return on a lookup miss, not an assembly, and the found path
// beside it copies the whole struct; counting it made every one of Sequence's
// fourteen fields report as dropped while nothing was dropped at all.
func collectNamedFields(function *ast.FuncDecl, named map[string]bool) {
	assembled := assembledLocals(function)
	ast.Inspect(function, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.CompositeLit:
			element, ok := literalElementName(node)
			if !ok || len(node.Elts) == 0 {
				return true
			}
			named["type:"+element] = true
			for _, item := range node.Elts {
				pair, ok := item.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := pair.Key.(*ast.Ident); ok {
					named[element+"."+key.Name] = true
				}
			}
		case *ast.AssignStmt:
			for _, target := range node.Lhs {
				selector, ok := target.(*ast.SelectorExpr)
				if !ok {
					continue
				}
				receiver, ok := selector.X.(*ast.Ident)
				if !ok {
					continue
				}
				if element, ok := assembled[receiver.Name]; ok {
					named[element+"."+selector.Sel.Name] = true
				}
			}
		}
		return true
	})
}

// assembledLocals maps each local variable this function binds to a composite
// literal of a package-qualified type onto that type's name, so a later
// `local.Field = ...` can be credited to it.
func assembledLocals(function *ast.FuncDecl) map[string]string {
	locals := make(map[string]string)
	ast.Inspect(function, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, right := range assign.Rhs {
			literal, ok := right.(*ast.CompositeLit)
			if !ok || i >= len(assign.Lhs) {
				continue
			}
			element, ok := literalElementName(literal)
			if !ok {
				continue
			}
			if name, ok := assign.Lhs[i].(*ast.Ident); ok {
				locals[name.Name] = element
			}
		}
		return true
	})
	return locals
}

// literalElementName is the type name of a package-qualified composite literal.
func literalElementName(literal *ast.CompositeLit) (string, bool) {
	selector, ok := literal.Type.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	return selector.Sel.Name, true
}

// namedOrExempt reports whether a reversal builder names this field, or whether
// the tree records a reason it cannot.
func namedOrExempt(named map[string]bool, name string) bool {
	_, exempt := nestedCoverageExempt[name]
	return named[name] || exempt
}
