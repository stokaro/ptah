package compare

// White-box testing required: this file guards the compare command's own
// reporting path, and that path is the unexported writeComparison. Driving the
// exported command instead would need a live database carrying an instance of
// every schema difference category, and would still not prove that the
// reporting reads the diff rather than only the planner's SQL -- which is the
// property stokaro/ptah#1284 was filed about.

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// nonCategoryFields names the SchemaDiff fields that do not carry schema
// changes, with the reason each is not reported as a difference. Everything
// that is not a list is one by construction, and a list can be one too when it
// qualifies another list rather than adding to it. This map is what forces that
// judgment to be made rather than assumed: a field missing from both the
// categories and this map fails TestSchemaDiffNonCategoryFieldsAreDocumented,
// and a list listed here is a claim that reporting it would say nothing the
// list it qualifies does not already say.
var nonCategoryFields = map[string]string{
	"IdentifierSemantics":           "records the live catalog identifier rules the diff was produced under, not a difference between the two schemas",
	"ConstraintBackedIndexRemovals": "a subset of IndexesRemoved, naming the removals whose object a UNIQUE constraint enforces so the planner spells the drop as a constraint drop; reporting it would print the same removed index a second time, and on its own it removes nothing",
}

// TestWriteComparisonReportsEveryDiffCategory is the guard for
// stokaro/ptah#1284. For every change category of SchemaDiff it asserts the
// three properties the issue found missing on RLSEnabledTablesAdded:
//
//   - the category appears in the report, so a difference is never printed as
//     an empty diff;
//   - a diff carrying only that category answers HasChanges, so --exit-code
//     fails a pipeline on it;
//   - a category the dialect planner rendered no SQL for is called out on
//     standard error instead of leaving the operator with an empty statement
//     list.
//
// The categories come from reflection over the struct, not from a list kept by
// hand here, so adding a field to SchemaDiff without teaching the report to
// read it fails this test rather than shipping a silently unreported category.
func TestWriteComparisonReportsEveryDiffCategory(t *testing.T) {
	c := qt.New(t)

	fields := diffCategoryFields()
	c.Assert(len(fields) > 20, qt.IsTrue, qt.Commentf("only %d categories discovered; reflection is not seeing SchemaDiff", len(fields)))

	for _, field := range fields {
		c.Run(field.Name, func(c *qt.C) {
			diff := diffWithOnlyCategory(field)
			stdout := &bytes.Buffer{}
			stderr := &bytes.Buffer{}

			writeComparison(stdout, stderr, diff, "", "postgres")

			c.Assert(stdout.String(), qt.Contains, diffCategoryJSONName(field)+" (1):")
			c.Assert(stdout.String(), qt.Contains, "Reconciling SQL: none.")
			c.Assert(stderr.String(), qt.Contains, diffCategoryJSONName(field))
			c.Assert(exitcode.Code(nonEmptyDiffExitCode(diff), 0), qt.Equals, 1)
		})
	}
}

// TestSchemaDiffNonCategoryFieldsAreDocumented keeps the exclusion from
// TestWriteComparisonReportsEveryDiffCategory honest: every SchemaDiff field
// that reflection does not treat as a category must carry a written reason.
func TestSchemaDiffNonCategoryFieldsAreDocumented(t *testing.T) {
	c := qt.New(t)

	names := diffNonCategoryFieldNames()
	c.Assert(names, qt.Not(qt.HasLen), 0)

	for _, name := range names {
		c.Run(name, func(c *qt.C) {
			c.Assert(
				nonCategoryFields[name],
				qt.Not(qt.Equals),
				"",
				qt.Commentf("SchemaDiff.%s carries no list of changes; record here why it is not a reported difference", name),
			)
		})
	}
}

func TestWriteComparisonReportsNoDifferences(t *testing.T) {
	c := qt.New(t)

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	writeComparison(stdout, stderr, &difftypes.SchemaDiff{}, "", "postgres")

	c.Assert(stdout.String(), qt.Equals, "No schema differences detected.\n")
	c.Assert(stderr.String(), qt.Equals, "")
}

func TestWriteComparisonPrintsCategoriesAndSQL(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		RLSEnabledTablesAdded: []string{"other.secured"},
		GrantsAdded: []difftypes.GrantRef{
			{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "other.granted"},
		},
	}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	writeComparison(stdout, stderr, diff, "ALTER TABLE \"other\".\"secured\" ENABLE ROW LEVEL SECURITY;\n", "postgres")

	c.Assert(stdout.String(), qt.Equals, `Differences detected (2 categories):
  rls_enabled_tables_added (1): other.secured
  grants_added (1): app SELECT TABLE other.granted

Reconciling SQL:
ALTER TABLE "other"."secured" ENABLE ROW LEVEL SECURITY;
`)
	c.Assert(stderr.String(), qt.Equals, "")
}

// diffCategoryFields returns the SchemaDiff fields that carry changes: the
// exported list fields, less the ones nonCategoryFields excludes with a stated
// reason. It is a helper rather than an inline loop so the test bodies above
// stay free of control flow.
func diffCategoryFields() []reflect.StructField {
	structType := reflect.TypeFor[difftypes.SchemaDiff]()
	var fields []reflect.StructField
	for field := range structType.Fields() {
		if !field.IsExported() || field.Type.Kind() != reflect.Slice {
			continue
		}
		if _, excluded := nonCategoryFields[field.Name]; excluded {
			continue
		}
		fields = append(fields, field)
	}
	return fields
}

// diffNonCategoryFieldNames returns every SchemaDiff field the report guard
// above does not exercise, which is exactly the set that has to carry a written
// reason.
func diffNonCategoryFieldNames() []string {
	categories := make(map[string]struct{})
	for _, field := range diffCategoryFields() {
		categories[field.Name] = struct{}{}
	}
	structType := reflect.TypeFor[difftypes.SchemaDiff]()
	var names []string
	for field := range structType.Fields() {
		if _, isCategory := categories[field.Name]; isCategory {
			continue
		}
		names = append(names, field.Name)
	}
	return names
}

// diffWithOnlyCategory builds a diff whose single non-empty category is field,
// holding one zero-valued object. The object's contents do not matter to the
// property under test -- that the category reaches the report at all -- and a
// zero value keeps the fixture from having to know each element type.
func diffWithOnlyCategory(field reflect.StructField) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	reflect.ValueOf(diff).Elem().FieldByIndex(field.Index).Set(reflect.MakeSlice(field.Type, 1, 1))
	return diff
}

func diffCategoryJSONName(field reflect.StructField) string {
	name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
	return name
}
