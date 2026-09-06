// Package diffreport turns a schema diff into the list of change categories it
// carries, for the CLI commands that report a comparison to an operator.
//
// The categories are discovered by reflecting over the fields of
// [difftypes.SchemaDiff] rather than read from a list maintained by hand. A hand-written
// list is what let "ptah schema compare" report nothing for row-level security
// changes the comparator had recorded (stokaro/ptah#1284): the field existed,
// the comparator filled it, and no reporting path ever read it. Reflection
// removes the second edit, so a category added to the diff model is reported
// by every command built on this package without one.
package diffreport

import (
	"fmt"
	"reflect"
	"strings"

	"ptah.run/migration/schemadiff/difftypes"
)

// maxObjectNameParts bounds how many string fields of one changed object are
// joined into its printed name. Small object references (an index and its
// table, a grant's role/privilege/object) fit inside the bound; the widest
// element type, a constraint definition, would otherwise print its whole
// definition — check expression, referenced columns, referential actions — on
// a line meant to identify it.
const maxObjectNameParts = 4

// Category is one non-empty change category of a schema diff.
type Category struct {
	// Name is the category's JSON field name, e.g. "rls_enabled_tables_added".
	// It is the same spelling the JSON serialization of the diff uses, so an
	// operator reading a report and a pipeline reading JSON name the category
	// identically.
	Name string

	// Objects names the changed objects, in the order the comparator recorded
	// them (the comparator sorts every category for deterministic output).
	Objects []string
}

// Count returns how many objects the category carries.
func (c Category) Count() int {
	return len(c.Objects)
}

// Categories returns every non-empty change category of diff, in the field
// order of the diff struct. A nil diff has no categories.
//
// The result is empty exactly when the diff carries no changes, so a caller
// can report "no differences" from len(Categories(diff)) == 0 without a second
// opinion about what counts as a change.
func Categories(diff *difftypes.SchemaDiff) []Category {
	if diff == nil {
		return nil
	}

	value := reflect.ValueOf(*diff)
	structType := value.Type()
	categories := make([]Category, 0, structType.NumField())
	for index := range structType.NumField() {
		field := structType.Field(index)
		if !IsChangeCategory(field) {
			continue
		}
		list := value.Field(index)
		if list.Len() == 0 {
			continue
		}
		categories = append(categories, Category{
			Name:    categoryName(field),
			Objects: describeAll(list),
		})
	}
	return categories
}

// Names returns the category names, for a caller that needs the shape of a
// report rather than its contents.
func Names(categories []Category) []string {
	names := make([]string, 0, len(categories))
	for _, category := range categories {
		names = append(names, category.Name)
	}
	return names
}

// IsChangeCategory reports whether field carries schema changes.
//
// Only slice fields do. Every category of the diff is a list of changed
// objects; the fields that are not lists record how the diff was produced
// rather than what differs — the live catalog identifier rules, for one — and
// reporting them as changes would make a synced schema look modified.
//
// A list the wire does not carry is not one either. `json:"-"` is how the diff
// says a field is an INPUT the planner reads rather than an observation about
// the two schemas: the declaration's tables, resolved so a foreign key can name
// the table it points at, and the policy identity collisions the planner
// refuses on. Both are populated on an ordinary comparison of two schemas that
// agree, so reporting them printed `DeclaredTables (2): ...` for a converged
// database and `ptah schema compare` answered that a synced schema differs
// (stokaro/ptah#2315).
//
// The test is the same one [categoryName] applies, and deliberately so: a field
// with a name on the wire is reported under that name, and a field with none is
// not reported at all.
//
// A list the diff declares a supplement is not one either, and that exclusion
// is read off the declaration rather than decided here. A supplement qualifies
// another list -- naming the removals a UNIQUE constraint backs, or the columns
// a foreign-key drop must be ordered by -- so every object it holds is already
// printed under the list it qualifies, and printing it again tells an operator
// that two things changed where one did. It stays on the wire, where a machine
// needs the qualifier; see [difftypes.SupplementLists].
//
// It is exported because the guard that asserts every category reaches a report
// has to ask the same question this does. A guard with its own copy of the
// predicate is a guard that can agree with itself while the report diverges,
// which is what stokaro/ptah#2476 measured.
func IsChangeCategory(field reflect.StructField) bool {
	if !field.IsExported() || field.Type.Kind() != reflect.Slice {
		return false
	}
	if _, supplement := difftypes.SupplementLists()[categoryName(field)]; supplement {
		return false
	}
	tag, ok := field.Tag.Lookup("json")
	if !ok {
		// No tag at all still serializes, under the Go field name.
		return true
	}
	name, _, _ := strings.Cut(tag, ",")
	return name != "-"
}

// categoryName prefers the field's JSON name so reports and serialized diffs
// use one spelling, and falls back to the Go field name for a field that
// carries no JSON tag.
func categoryName(field reflect.StructField) string {
	tag, ok := field.Tag.Lookup("json")
	if !ok {
		return field.Name
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" || name == "-" {
		return field.Name
	}
	return name
}

func describeAll(list reflect.Value) []string {
	objects := make([]string, 0, list.Len())
	for index := range list.Len() {
		objects = append(objects, describe(list.Index(index)))
	}
	return objects
}

// describe names one changed object.
//
// A category of plain names is its own description. A category of object
// references or per-object diffs is described by its string fields joined in
// declaration order, which puts the object's own name first and its qualifying
// context — the owning table, the granted privilege — after it. Anything else
// falls back to the value's default formatting, so a category is never dropped
// from a report because its element type is new.
func describe(element reflect.Value) string {
	element = dereference(element)
	if !element.IsValid() {
		return "<nil>"
	}
	if element.Kind() == reflect.String {
		return element.String()
	}
	if element.Kind() != reflect.Struct {
		return fmt.Sprint(element.Interface())
	}
	// An element that knows how to name itself says so. stringFields reads
	// TOP-LEVEL string fields, so an element carrying its object inside a
	// nested declaration -- an index addition, since stokaro/ptah#2315 --
	// would be reported by its context alone, without the name of the thing
	// that changed.
	if named, ok := reflect.TypeAssert[fmt.Stringer](element); ok {
		return named.String()
	}

	parts := stringFields(element)
	if len(parts) == 0 {
		return fmt.Sprint(element.Interface())
	}
	return strings.Join(parts, " ")
}

func stringFields(element reflect.Value) []string {
	structType := element.Type()
	parts := make([]string, 0, maxObjectNameParts)
	for index := range structType.NumField() {
		if len(parts) == maxObjectNameParts {
			break
		}
		if !structType.Field(index).IsExported() {
			continue
		}
		field := element.Field(index)
		if field.Kind() != reflect.String || field.String() == "" {
			continue
		}
		parts = append(parts, field.String())
	}
	return parts
}

func dereference(value reflect.Value) reflect.Value {
	for value.IsValid() && value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	return value
}
