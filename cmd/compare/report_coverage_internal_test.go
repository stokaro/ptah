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

	"go.5x5.cz/ptah/cmd/internal/diffreport"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// nonCategoryFields names the SchemaDiff fields that carry no schema changes
// because of what they are, with the reason each is not reported as a
// difference: a field that is not a list, and a list the wire does not carry.
// This map is what forces that judgment to be made rather than assumed -- a
// field excluded from the report and missing from here fails
// TestSchemaDiffNonCategoryFieldsAreDocumented.
//
// A list that qualifies another list is NOT one of these. It says so at its own
// declaration, with a ptah:"supplement=..." tag the report reads, so its reason
// travels with the field: an entry here would be a second answer, and the two
// this replaces named fields that stokaro/ptah#2315 had already retired while
// the report went on printing the two it did name (stokaro/ptah#2476).
var nonCategoryFields = map[string]string{
	"IdentifierSemantics":        "records the live catalog identifier rules the diff was produced under, not a difference between the two schemas",
	"DeclaredTables":             "every table the declaration holds, carried so a foreign key can be resolved to the table it references; like the vocabulary below it is an input to rendering rather than a difference, and reporting it would print the whole document's tables as though they had changed (stokaro/ptah#2315)",
	"DeclaredUserTypes":          "the declaration's type vocabulary, carried so a planner can resolve a created column's type to the user type it names; it is an input to rendering rather than a difference between the two schemas, and reporting it would print the whole document's domains and enums as though they had changed (stokaro/ptah#2315)",
	"DeclaredViewLikes":          "every declared view and materialized view, carried so a DROP that cascades can be resolved to the views it reaches -- usually views this diff does not touch. Like the two above it is an input to rendering rather than a difference, and reporting it would print the whole document's views as though they had changed (stokaro/ptah#2315)",
	"DeclaredForeignKeys":        "every foreign key the schema the plan runs against holds, carried so a column type change can drop its keys and put them back -- keys this diff does not touch, under a column it does. Like the three above it is an input to rendering rather than a difference, and reporting it would print the whole document's foreign keys as though they had changed (stokaro/ptah#2315)",
	"DeclaredConstraintHosts":    "the whole declaration of every table a constraint change names, carried so a target that rebuilds a table to change one of its constraints can render the table entire. Like the four above it is an input to rendering rather than a difference, and reporting it would print those tables as though they had changed (stokaro/ptah#2315)",
	"DeclaredTableDependencies":  "the table dependency graph of the schema the plan runs against, carried so the removals can be ordered child-before-parent. Like the four above it is an input to rendering rather than a difference, and reporting it would print an edge for every table in the document as though something had changed (stokaro/ptah#2315)",
	"DeclaredFunctions":          "the declaration order and call graph of the declared functions, carried so the ones this diff creates can be ordered caller-after-callee. Like the five above it is an input to rendering rather than a difference, and reporting it would print every function in the document as though it had changed (stokaro/ptah#2315)",
	"DeclaredIndexes":            "every declared index paired with the relation it belongs to, carried so an addition -- which is a reference, a name and a table -- can be resolved to the definition it names. Like the six above it is an input to rendering rather than a difference, and reporting it would print every index in the document as though it had changed (stokaro/ptah#2315)",
	"RLSPolicyIdentityConflicts": "two declared policies that resolve to one identity, which is a defect in the declaration rather than a difference between the two schemas; the planner refuses the diff on it, so reporting it as a change would tell the operator the databases differ when what differs is the document they wrote (stokaro/ptah#2440)",
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
		t.Run(field.Name, func(t *testing.T) {
			c := qt.New(t)
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
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				nonCategoryFields[name],
				qt.Not(qt.Equals),
				"",
				qt.Commentf("SchemaDiff.%s carries no list of changes; record here why it is not a reported difference", name),
			)
		})
	}
}

// TestSchemaDiffNonCategoryReasonsNameALiveField reads the map the other
// direction. Without it a reason can outlive the field it was written for: two
// did, naming routine lists stokaro/ptah#2315 had folded away, and nothing
// noticed because every assertion ran from the struct towards the map and a
// key nothing looks up is a key nothing checks (stokaro/ptah#2476).
func TestSchemaDiffNonCategoryReasonsNameALiveField(t *testing.T) {
	c := qt.New(t)

	declared := make(map[string]struct{})
	for field := range reflect.TypeFor[difftypes.SchemaDiff]().Fields() {
		declared[field.Name] = struct{}{}
	}
	c.Assert(nonCategoryFields, qt.Not(qt.HasLen), 0)

	for name := range nonCategoryFields {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			_, exists := declared[name]
			c.Assert(exists, qt.IsTrue, qt.Commentf("SchemaDiff has no field %s; the reason recorded for it outlived the field", name))
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
		RLSEnabledTablesAdded: difftypes.RLSEnabledTableChanges{{Table: "other.secured"}},
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

// diffCategoryFields returns the SchemaDiff fields that carry changes, asking
// the predicate the report itself applies rather than deciding again here. A
// guard with its own copy of the rule agrees with itself while the report
// diverges, which is how two lists came to be printed as categories under a map
// that said they would not be (stokaro/ptah#2476). It is a helper rather than
// an inline loop so the test bodies above stay free of control flow.
func diffCategoryFields() []reflect.StructField {
	structType := reflect.TypeFor[difftypes.SchemaDiff]()
	var fields []reflect.StructField
	for field := range structType.Fields() {
		if !diffreport.IsChangeCategory(field) {
			continue
		}
		fields = append(fields, field)
	}
	return fields
}

// diffNonCategoryFieldNames returns every SchemaDiff field the report guard
// above does not exercise and that does not state its own reason for it, which
// is exactly the set nonCategoryFields has to carry a written reason for. A
// supplement states its reason at its declaration, so it is excluded here
// rather than needing an entry that could outlive it.
func diffNonCategoryFieldNames() []string {
	categories := make(map[string]struct{})
	for _, field := range diffCategoryFields() {
		categories[field.Name] = struct{}{}
	}
	supplements := difftypes.SupplementLists()
	structType := reflect.TypeFor[difftypes.SchemaDiff]()
	var names []string
	for field := range structType.Fields() {
		if _, isCategory := categories[field.Name]; isCategory {
			continue
		}
		if _, isSupplement := supplements[diffCategoryJSONName(field)]; isSupplement {
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
