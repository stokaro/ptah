package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
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
