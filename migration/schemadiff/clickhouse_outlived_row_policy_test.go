package schemadiff_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// outlivedPolicySource declares the table orders and the named row policies
// on it, from a source that describes every row policy, so one it leaves out
// is one it does not want.
func outlivedPolicySource(policies ...string) *schemamodel.Database {
	var objects []schemaext.Object
	for _, name := range policies {
		objects = append(objects, must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("", "orders", name),
			chschema.DesiredRowPolicy{Filter: new("id > 100"), Roles: chschema.RoleSelection{All: true}})))
	}
	return &schemamodel.Database{
		Tables:          []schemamodel.Table{{Name: "orders", StructName: "Order"}},
		Fields:          []schemamodel.Field{{Name: "id", StructName: "Order", Type: "UInt64", Primary: true}},
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// outlivedPolicyDatabase holds no table, and the policy leak that ClickHouse
// kept on the name orders when the table was dropped.
func outlivedPolicyDatabase() *catalog.Database {
	leak := must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("", "orders", "leak"), chschema.ObservedRowPolicy{
		Filter: new("1"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true, Names: make([]string, 0), Except: make([]string, 0)}}))
	return &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(leak)),
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// TestClickHouseOutlivedRowPolicyIsPlannedBeforeItsTable plans a policy the
// server kept for a dropped table as a change of its own, ahead of the CREATE
// TABLE that gives the name a table again: the server applies the policy to
// that table at once, so a policy the declaration leaves out is dropped
// first, and a declared policy of the same name is altered first and left
// out of the table's creation, which the server refuses with code 493 for a
// policy it already holds. A policy only the declaration holds is still
// created with its table.
func TestClickHouseOutlivedRowPolicyIsPlannedBeforeItsTable(t *testing.T) {
	for _, test := range []struct {
		name       string
		declared   []string
		created    int
		statements []string
	}{
		{name: "an undeclared policy", statements: []string{
			"DROP ROW POLICY `leak` ON `orders`", "CREATE TABLE orders ("}},
		{name: "a declared policy of the same name", declared: []string{"leak"}, statements: []string{
			"ALTER ROW POLICY `leak` ON `orders` USING (id > 100) AS PERMISSIVE TO ALL", "CREATE TABLE orders ("}},
		{name: "an undeclared policy beside a declared one", declared: []string{"tenant"}, created: 1, statements: []string{
			"DROP ROW POLICY `leak` ON `orders`", "CREATE TABLE orders (", "CREATE ROW POLICY `tenant` ON `orders` USING (id > 100) AS PERMISSIVE TO ALL"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())

			diff, err := schemadiff.CompareWithDialect(t.Context(), outlivedPolicySource(test.declared...), outlivedPolicyDatabase(), "clickhouse", runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.FeatureChanges, qt.HasLen, 1)
			c.Assert(diff.FeatureChanges[0].Subject.Key(), qt.Equals, chschema.RowPolicyRef("", "orders", "leak").Key())
			c.Assert(diff.TablesAdded, qt.HasLen, 1)
			c.Assert(diff.TablesAdded[0].OwnedObjects.Len(), qt.Equals, test.created)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, "clickhouse", planext.Options{})
			c.Assert(err, qt.IsNil)
			var firstLines []string
			for _, statement := range statements {
				firstLines = append(firstLines, strings.TrimSuffix(strings.SplitN(statement, "\n", 2)[0], ";"))
			}
			c.Assert(firstLines, qt.DeepEquals, test.statements)
		})
	}
}

// tableBoundRuntime is the builtin runtime answering that no kind outlives its
// table, the answer of a runtime whose registration does not declare
// [engine.ObjectComparison.OutlivesParent]. The comparison it runs is the
// builtin one, so the change reaches the diff as it does there.
type tableBoundRuntime struct{ *engine.Runtime }

func (tableBoundRuntime) OutlivesParent(string, schemaext.Kind) bool { return false }

// noTableSource declares no table, from a source that describes every row
// policy.
func noTableSource() *schemamodel.Database {
	return &schemamodel.Database{
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// TestClickHouseOutlivedRowPolicyWithNoTableDeclared_HappyPath drops the
// policy a dropped table left behind when no table of the name is declared:
// the change is planned apart from any table.
func TestClickHouseOutlivedRowPolicyWithNoTableDeclared_HappyPath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDialect(t.Context(), noTableSource(), outlivedPolicyDatabase(), "clickhouse", must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.HasLen, 1)
	c.Assert(diff.FeatureChanges[0].Subject.Key(), qt.Equals, chschema.RowPolicyRef("", "orders", "leak").Key())
	c.Assert(diff.TablesModified, qt.HasLen, 0)
}

// TestClickHouseOutlivedRowPolicyWithNoTableDeclared_FailurePath compares the
// same states through a runtime whose kinds stay bound to their table: a
// change below a table neither side holds has no table to join, and the
// comparison refuses it rather than planning it alone.
func TestClickHouseOutlivedRowPolicyWithNoTableDeclared_FailurePath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDialect(t.Context(), noTableSource(), outlivedPolicyDatabase(), "clickhouse", tableBoundRuntime{must.Must(builtin.New())})

	c.Assert(err, qt.ErrorMatches, `(?s).*feature change has no desired table .*orders.*`)
	c.Assert(diff, qt.IsNil)
}

// viewPolicyDatabase holds the view v and a policy on it, which the read
// records as one it could not describe.
func viewPolicyDatabase() *catalog.Database {
	return &catalog.Database{
		Views: []catalog.View{{Name: "v", Body: "SELECT 1 AS id"}},
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{
			Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("", "v", "on_view"),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: `the policy is on "v" (engine View)`}}})),
	}
}

// viewPolicySource describes every row policy and declares the given views
// and tables.
func viewPolicySource(views []schemamodel.View, tables ...string) *schemamodel.Database {
	source := &schemamodel.Database{
		Views:           views,
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	for _, table := range tables {
		source.Tables = append(source.Tables, schemamodel.Table{Name: table, StructName: table})
		source.Fields = append(source.Fields, schemamodel.Field{Name: "id", StructName: table, Type: "UInt64", Primary: true})
	}
	return source
}

// TestClickHouseViewDropTakesItsRowPolicies drops the policies the read
// recorded on a view together with the view, ahead of it: ClickHouse keeps a
// policy bound to the name, so a policy left behind would filter the next
// relation of the name, a table the plan creates among them. The comparison
// then has nothing undecided to report about the policy.
func TestClickHouseViewDropTakesItsRowPolicies(t *testing.T) {
	for _, test := range []struct {
		name    string
		source  *schemamodel.Database
		drops   []string
		creates []string
	}{
		{name: "a view the declaration leaves out", source: viewPolicySource(nil),
			drops: []string{"DROP ROW POLICY `on_view` ON `v`", "DROP VIEW IF EXISTS `v`"}},
		{name: "a view replaced by a table", source: viewPolicySource(nil, "v"),
			drops: []string{"DROP ROW POLICY `on_view` ON `v`", "DROP VIEW IF EXISTS `v`"}, creates: []string{"CREATE TABLE v ("}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			semantics := identifier.ForDialect("clickhouse")

			diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), test.source, viewPolicyDatabase(),
				&config.CompareOptions{Dialect: "clickhouse", IdentifierSemantics: &semantics}, runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diagnostics.Features, qt.HasLen, 0)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, "clickhouse", planext.Options{})
			c.Assert(err, qt.IsNil)
			c.Assert(statementHeads(statements, "DROP "), qt.DeepEquals, test.drops)
			c.Assert(statementHeads(statements, "CREATE "), qt.DeepEquals, test.creates)
		})
	}
}

// TestClickHouseViewDropLeavesOtherRowPolicies drops the view v and only the
// policy on it: the policy on the view w the declaration keeps, and the one
// written ON the database, stay on the server, and the comparison reports both
// as undecided.
func TestClickHouseViewDropLeavesOtherRowPolicies(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	semantics := identifier.ForDialect("clickhouse")
	record := func(table, name string) schemaext.SubjectCoverage {
		return schemaext.SubjectCoverage{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("", table, name),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the read does not describe it"}}
	}
	current := &catalog.Database{
		Views: []catalog.View{{Name: "v", Body: "SELECT 1 AS id"}, {Name: "w", Body: "SELECT 2 AS id"}},
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
			[]schemaext.SubjectCoverage{record("v", "on_view"), record("w", "on_w"), record("*", "wide")})),
	}
	source := viewPolicySource([]schemamodel.View{{Name: "w", Body: "SELECT 2 AS id"}})

	diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), source, current,
		&config.CompareOptions{Dialect: "clickhouse", IdentifierSemantics: &semantics}, runtime)

	c.Assert(err, qt.IsNil)
	undecided := make([]string, 0, len(diagnostics.Features))
	for _, change := range diagnostics.Features {
		undecided = append(undecided, change.Subject.Parent.Source+"."+change.Subject.Name.Source)
	}
	slices.Sort(undecided)
	c.Assert(undecided, qt.DeepEquals, []string{"*.wide", "w.on_w"})
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, "clickhouse", planext.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(statementHeads(statements, "DROP "), qt.DeepEquals, []string{"DROP ROW POLICY `on_view` ON `v`", "DROP VIEW IF EXISTS `v`"})
}

// TestClickHouseViewDropReportsAKindBoundToItsTable drops the view v through a
// runtime that says no kind outlives its table. The comparison leaves out of
// its report only the objects a target keeps past their relation, which its
// planner drops with the relation; for any other kind the report stays.
func TestClickHouseViewDropReportsAKindBoundToItsTable(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("clickhouse")

	_, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), viewPolicySource(nil), viewPolicyDatabase(),
		&config.CompareOptions{Dialect: "clickhouse", IdentifierSemantics: &semantics}, tableBoundRuntime{must.Must(builtin.New())})

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.Features, qt.HasLen, 1)
	c.Assert(diagnostics.Features[0].Subject.Key(), qt.Equals, chschema.RowPolicyRef("", "v", "on_view").Key())
}

// TestClickHouseKeptViewKeepsItsRowPolicies keeps the policy on a view the
// declaration keeps, and reports it undecided: the read could not describe
// it, and the filter still applies.
func TestClickHouseKeptViewKeepsItsRowPolicies(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	semantics := identifier.ForDialect("clickhouse")

	diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), viewPolicySource([]schemamodel.View{{Name: "v", Body: "SELECT 1 AS id"}}),
		viewPolicyDatabase(), &config.CompareOptions{Dialect: "clickhouse", IdentifierSemantics: &semantics}, runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.Features, qt.HasLen, 1)
	c.Assert(diagnostics.Features[0].Subject.Key(), qt.Equals, chschema.RowPolicyRef("", "v", "on_view").Key())
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, "clickhouse", planext.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "ROW POLICY")
}

// TestClickHouseRebuiltViewKeepsItsRowPolicies plans a diff that removes the
// view d1.v and adds the view v: the shape of a view rebuilt under the same
// name, spelled once by the catalog and once by the declaration. No
// comparison is known to produce it, so the diff is built by hand. The plan
// drops and creates the view and leaves its policy alone, since ClickHouse
// keeps the policy for the view created under the name; dropping it would
// change which rows the view shows.
func TestClickHouseRebuiltViewKeepsItsRowPolicies(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	diff := &difftypes.SchemaDiff{
		ViewsRemoved: difftypes.ViewChanges{{Name: "d1.v"}},
		ViewsAdded:   difftypes.ViewChanges{{Name: "v", Body: "SELECT 1 AS id"}},
		Features:     difftypes.FeatureContext{CurrentCoverage: viewPolicyDatabase().FeatureCoverage},
	}

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, "clickhouse", planext.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(statementHeads(statements, "DROP "), qt.DeepEquals, []string{"DROP VIEW IF EXISTS `d1`.`v`"})
	c.Assert(statementHeads(statements, "CREATE "), qt.DeepEquals, []string{"CREATE VIEW `v` AS"})
}

// statementHeads is the first line of each statement that starts with verb,
// without its terminator, in plan order. A plan that creates nothing gives
// nil.
func statementHeads(statements []string, verb string) []string {
	var lines []string
	for _, statement := range statements {
		head := strings.TrimSuffix(strings.SplitN(statement, "\n", 2)[0], ";")
		if strings.HasPrefix(head, verb) {
			lines = append(lines, head)
		}
	}
	return lines
}
