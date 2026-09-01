package modelast_test

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/modelast"
)

// routedKind is one declared object kind, the AST node the converter must
// produce for it, and how many of that node a conversion of routingFixture must
// contain.
//
// Counting nodes rather than searching rendered SQL is deliberate: this package
// decides which objects exist, and the renderer decides what each one means for
// a concrete engine. A comparison over SQL would fail here for the renderer's
// reasons and hide this converter's own answer.
type routedKind struct {
	name  string
	want  int
	count func([]ast.Node) int
}

var routedKinds = []routedKind{
	{name: "sequence", want: 1, count: countNodes[*ast.CreateSequenceNode]},
	{name: "user type", want: 3, count: countNodes[*ast.CreateTypeNode]},
	{name: "role", want: 1, count: countNodes[*ast.CreateRoleNode]},
	{name: "table", want: 1, count: countNodes[*ast.CreateTableNode]},
	{name: "function", want: 1, count: countNodes[*ast.CreateFunctionNode]},
	{name: "view", want: 1, count: countNodes[*ast.CreateViewNode]},
	{name: "materialized view", want: 1, count: countNodes[*ast.CreateMaterializedViewNode]},
	{name: "trigger", want: 1, count: countNodes[*ast.CreateTriggerNode]},
	{name: "extension", want: 1, count: countNodes[*ast.ExtensionNode]},
	{name: "rls enable", want: 1, count: countNodes[*ast.AlterTableEnableRLSNode]},
	{name: "policy", want: 1, count: countNodes[*ast.CreatePolicyNode]},
	{name: "synonym", want: 1, count: countNodes[*ast.CreateSynonymNode]},
	{name: "hypertable", want: 1, count: countNodes[*ast.CreateHypertableNode]},
	{name: "continuous aggregate", want: 1, count: countNodes[*ast.CreateContinuousAggregateNode]},
	{name: "extended property", want: 1, count: countNodes[*ast.ExtendedPropertyNode]},
	{name: "grant", want: 1, count: countNodes[*ast.GrantPrivilegeNode]},
}

func countNodes[T ast.Node](statements []ast.Node) int {
	found := 0
	for _, statement := range statements {
		if _, matches := statement.(T); matches {
			found++
		}
	}
	return found
}

// routingFixture declares one object of every kind in routedKinds. The three
// user types are a domain, a composite and a range, which share one CREATE TYPE
// node kind and one ordering pass.
//
// Enums are deliberately absent. Four engines model an enum on the column
// instead of as a standalone type, so the enum node count legitimately differs
// by dialect -- the object is still emitted, inside the column -- and that is the
// one modeling difference this comparison must not mistake for a drop.
func routingFixture() schemamodel.Database {
	start := int64(1000)
	return schemamodel.Database{
		Extensions:     []schemamodel.Extension{{Name: "pgcrypto"}},
		Sequences:      []schemamodel.Sequence{{Name: "seq_probe", AsType: "bigint", Start: &start}},
		Domains:        []schemamodel.Domain{{Name: "domain_probe", BaseType: "TEXT"}},
		CompositeTypes: []schemamodel.CompositeType{{Name: "composite_probe", Fields: []schemamodel.CompositeField{{Name: "street", Type: "TEXT"}}}},
		Ranges:         []schemamodel.Range{{Name: "range_probe", Subtype: "float8"}},
		Roles:          []schemamodel.Role{{Name: "role_probe", Login: true, Inherit: true}},
		Tables:         []schemamodel.Table{{StructName: "T", Name: "table_probe"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", Name: "touched", Type: "TIMESTAMP", Nullable: true},
		},
		Functions:         []schemamodel.Function{{Name: "func_probe", Returns: "integer", Language: "sql", Body: "SELECT 1;"}},
		Views:             []schemamodel.View{{StructName: "V", Name: "view_probe", Body: "SELECT id FROM table_probe"}},
		MaterializedViews: []schemamodel.MaterializedView{{StructName: "MV", Name: "matview_probe", Body: "SELECT id FROM table_probe"}},
		Synonyms:          []schemamodel.Synonym{{StructName: "SY", Name: "synonym_probe", Target: "dbo.table_probe"}},
		Hypertables: []schemamodel.Hypertable{{
			StructName: "HY", Table: "table_probe", Column: "n",
		}},
		ContinuousAggregates: []schemamodel.ContinuousAggregate{{
			StructName: "CA", Name: "aggregate_probe",
			Body: "SELECT id FROM table_probe",
		}},
		ExtendedProperties: []schemamodel.ExtendedProperty{{
			StructName: "XP", Name: "property_probe", Schema: "dbo",
			Table: "table_probe", Value: "probe",
		}},
		Triggers: []schemamodel.Trigger{{
			StructName: "TR", Name: "trigger_probe", Table: "table_probe",
			Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "SELECT 1",
		}},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{StructName: "S", Table: "table_probe"}},
		RLSPolicies: []schemamodel.RLSPolicy{{
			StructName: "S", Name: "policy_probe", Table: "table_probe",
			PolicyFor: "SELECT", ToRoles: "role_probe", UsingExpression: "true",
		}},
		Grants: []schemamodel.Grant{{
			StructName: "G", Role: "role_probe", Privileges: []string{"SELECT"}, OnTable: "table_probe",
		}},
	}
}

// TestCollectDatabase_EveryDialectGetsEveryDeclaredObject is this converter's half
// of stokaro/ptah#929 item 5: whether an object is converted is not a question
// the dialect name answers.
//
// Emission used to be gated by three name lists -- emitsStandaloneSequences,
// reportsUnsupportedSchemaObjects and supportsStandaloneViewsAndTriggers -- and a
// kind not on the list for a target was deleted here, before any renderer could
// report it. Measured with `ptah schema render` over one fixture declaring a
// sequence, a domain, a role, a table, a view and a function: fifteen objects
// were absent at exit 0 with no comment and no warning, across clickhouse,
// mysql, mariadb, sqlserver and sqlite. Each earlier fix added one more name to
// one of those lists, which closed an instance and left the class.
//
// The assertion reports the missing (dialect, kind) pairs, so a regression names
// what disappeared where.
func TestCollectDatabase_EveryDialectGetsEveryDeclaredObject(t *testing.T) {
	c := qt.New(t)

	spellings := acceptedSpellings(c)
	c.Assert(len(spellings) > 0, qt.IsTrue, qt.Commentf("the spelling extractor is broken"))

	missing := missingRoutedObjects(spellings)

	c.Assert(missing, qt.HasLen, 0,
		qt.Commentf("%d declared objects never reach a renderer:\n%s",
			len(missing), strings.Join(missing, "\n")))
}

// missingRoutedObjects returns one line per (spelling, kind) pair whose node
// count differs from what the fixture declares.
func missingRoutedObjects(spellings []string) []string {
	var missing []string
	for _, spelling := range spellings {
		statements := modelast.CollectDatabase(routingFixture(), spelling).Statements
		for _, kind := range routedKinds {
			got := kind.count(statements)
			if got == kind.want {
				continue
			}
			missing = append(missing, fmt.Sprintf("%-14s %-18s want %d, got %d", spelling, kind.name, kind.want, got))
		}
	}
	return missing
}

// TestCollectDatabase_TheRoutingFixtureCoversEveryDeclaredCollection is the control
// for the test above.
//
// The grid is only as complete as the fixture. A kind the fixture does not
// declare is a kind the comparison cannot lose, so a gate reintroduced for it
// would pass unnoticed -- which is how the earlier per-instance fixes each left
// the class open. This reads the collections schemamodel.Database actually has and
// requires every one of them to be either declared by the fixture or named here
// with a reason, so a new object kind added to Database fails this test until
// someone decides which it is.
func TestCollectDatabase_TheRoutingFixtureCoversEveryDeclaredCollection(t *testing.T) {
	c := qt.New(t)

	// Collections that are not standalone declared objects, each with the
	// reason it is out of scope for the routing grid.
	notObjectCollections := []string{
		"Fields",         // columns, carried inside CREATE TABLE
		"Indexes",        // emitted, but by name-independent phases already covered elsewhere
		"Constraints",    // table-level, carried inside CREATE TABLE or ADD CONSTRAINT
		"EmbeddedFields", // expanded into Fields before conversion
		"EmbeddedSources",
		"Schemas",     // namespaces, appended unconditionally and always have been
		"Enums",       // four engines model an enum on the column; see routingFixture
		"ManagedData", // row data, not DDL
		"Dependencies",
		"FunctionDependencies",
		"SelfReferencingForeignKeys",
		"NotDescribed",
	}

	declared := declaredCollectionNames(routingFixture())
	all := allCollectionNames()

	uncovered := slices.DeleteFunc(all, func(name string) bool {
		return slices.Contains(declared, name) || slices.Contains(notObjectCollections, name)
	})

	c.Assert(uncovered, qt.HasLen, 0,
		qt.Commentf("schemamodel.Database collections the routing fixture neither declares nor excuses: %s",
			strings.Join(uncovered, ", ")))

	// Control on the control: the reflection really found the collections. An
	// extractor returning nothing would make the filter above trivially empty.
	c.Assert(len(all) > len(notObjectCollections), qt.IsTrue,
		qt.Commentf("reflection found %d fields on schemamodel.Database", len(all)))
	c.Assert(declared, qt.Contains, "Sequences")
	c.Assert(declared, qt.Contains, "Domains")
	c.Assert(declared, qt.Contains, "Grants")
}

// allCollectionNames lists every exported field of schemamodel.Database, read from
// the type rather than copied, so a new collection appears here the day it is
// added.
func allCollectionNames() []string {
	databaseType := reflect.TypeFor[schemamodel.Database]()
	names := make([]string, 0, databaseType.NumField())
	for field := range databaseType.Fields() {
		names = append(names, field.Name)
	}
	return names
}

// declaredCollectionNames lists the exported fields the routing fixture actually
// populates. A field left at its zero value declares no object, so naming it in
// routedKinds would assert nothing.
func declaredCollectionNames(database schemamodel.Database) []string {
	value := reflect.ValueOf(database)
	names := make([]string, 0, value.NumField())
	for i := range value.NumField() {
		if value.Field(i).IsZero() {
			continue
		}
		names = append(names, value.Type().Field(i).Name)
	}
	return names
}
