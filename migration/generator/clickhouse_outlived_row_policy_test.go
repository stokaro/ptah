package generator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// outlivedDeclaration declares the table orders and the named row policies on
// it, from a source that describes every row policy.
func outlivedDeclaration(policies ...string) *schemamodel.Database {
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

// outlivedDatabase holds no table, and the permissive policy leak that
// ClickHouse kept on the name orders when the table was dropped.
func outlivedDatabase() *catalog.Database {
	leak := must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("", "orders", "leak"), chschema.ObservedRowPolicy{
		Filter: new("1"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true, Names: make([]string, 0), Except: make([]string, 0)}}))
	return &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(leak)),
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// statementHeads is the first line of each statement in sql, without its
// terminator, leaving out comment lines.
func statementHeads(sql string) []string {
	var heads []string
	for statement := range strings.SplitSeq(sql, ";\n") {
		for line := range strings.SplitSeq(strings.TrimSpace(statement), "\n") {
			if line != "" && !strings.HasPrefix(line, "--") {
				heads = append(heads, strings.TrimSuffix(line, ";"))
				break
			}
		}
	}
	return heads
}

// TestClickHouseOutlivedRowPolicyRollbackRunsAfterTheTableDrop plans both
// directions over a policy ClickHouse kept for the dropped table orders. The
// up migration changes the policy before it creates the table; the down
// migration changes it back only after it drops the table, so the permissive
// policy is never back in force on a table that still holds rows, which a
// failed DROP TABLE would otherwise leave readable in full.
func TestClickHouseOutlivedRowPolicyRollbackRunsAfterTheTableDrop(t *testing.T) {
	for _, test := range []struct {
		name     string
		declared []string
		up, down []string
	}{
		{name: "an undeclared policy",
			up:   []string{"DROP ROW POLICY `leak` ON `orders`", "CREATE TABLE orders ("},
			down: []string{"DROP TABLE IF EXISTS orders", "CREATE ROW POLICY `leak` ON `orders` USING (1) AS PERMISSIVE TO ALL"}},
		{name: "an undeclared policy beside a declared one", declared: []string{"tenant"},
			up: []string{"DROP ROW POLICY `leak` ON `orders`", "CREATE TABLE orders (",
				"CREATE ROW POLICY `tenant` ON `orders` USING (id > 100) AS PERMISSIVE TO ALL"},
			down: []string{"DROP ROW POLICY `tenant` ON `orders`", "DROP TABLE IF EXISTS orders",
				"CREATE ROW POLICY `leak` ON `orders` USING (1) AS PERMISSIVE TO ALL"}},
		{name: "a declared policy of the same name", declared: []string{"leak"},
			up:   []string{"ALTER ROW POLICY `leak` ON `orders` USING (id > 100) AS PERMISSIVE TO ALL", "CREATE TABLE orders ("},
			down: []string{"DROP TABLE IF EXISTS orders", "ALTER ROW POLICY `leak` ON `orders` USING (1) AS PERMISSIVE TO ALL"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired, current := outlivedDeclaration(test.declared...), outlivedDatabase()
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "clickhouse", runtime))

			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "clickhouse",
			})

			c.Assert(err, qt.IsNil)
			up, err := builtin.RenderSQL("clickhouse", plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(statementHeads(up), qt.DeepEquals, test.up)
			down, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(statementHeads(down), qt.DeepEquals, test.down)
		})
	}
}
