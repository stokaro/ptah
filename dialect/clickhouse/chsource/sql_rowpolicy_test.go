package chsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

func policySQL(c *qt.C, text string) schemamodel.Database {
	c.Helper()
	db, _, err := sqlschema.ReadWith(c.Context(), []byte(text), "clickhouse", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.IsNil)
	return db
}

func policyObject(c *qt.C, db schemamodel.Database) schemaext.Object {
	c.Helper()
	objects := must.Must(db.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	return objects[0]
}

func TestSQLRowPolicyDeclarationsAndRenderedReload(t *testing.T) {
	tests := []struct {
		name, text string
		want       schemaext.Object
	}{
		{"all clauses", "CREATE ROW POLICY tenant ON analytics.orders FOR SELECT USING tenant_id = 1 AS RESTRICTIVE TO ALL EXCEPT admin;", schemaext.Object{Ref: chschema.RowPolicyRef("analytics", "orders", "tenant"), Value: &chschema.DesiredRowPolicy{Filter: new("tenant_id = 1"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}}},
		{"defaults", "CREATE ROW POLICY p ON orders;", schemaext.Object{Ref: chschema.RowPolicyRef("", "orders", "p"), Value: &chschema.DesiredRowPolicy{}}},
		{"none", "CREATE ROW POLICY p ON orders USING NONE AS PERMISSIVE TO NONE;", schemaext.Object{Ref: chschema.RowPolicyRef("", "orders", "p"), Value: &chschema.DesiredRowPolicy{Composition: chschema.Permissive}}},
		{"all", "create row policy p on orders to all;", schemaext.Object{Ref: chschema.RowPolicyRef("", "orders", "p"), Value: &chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{All: true}}}},
		{"quoted names", "CREATE ROW POLICY ` p``q` ON `a.b`.\"Order\" TO `ALL`, \" my role \";", schemaext.Object{Ref: chschema.RowPolicyRef("a.b", "Order", " p`q"), Value: &chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{Names: []string{"ALL", " my role "}}}}},
		{"keywords in filter", "CREATE ROW POLICY p ON orders USING (if(`TO` = 'AS WITH CHECK', id IN [1,2], 0)) TO NONE;", schemaext.Object{Ref: chschema.RowPolicyRef("", "orders", "p"), Value: &chschema.DesiredRowPolicy{Filter: new("if(`TO` = 'AS WITH CHECK', id IN [1,2], 0)")}}},
		{"comments and clause order", "CREATE /* owner */ ROW POLICY p ON orders AS RESTRICTIVE TO ALL /* role */ EXCEPT admin USING (id > 0);", schemaext.Object{Ref: chschema.RowPolicyRef("", "orders", "p"), Value: &chschema.DesiredRowPolicy{Filter: new("id > 0"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := policySQL(c, test.text)
			c.Assert(policyObject(c, db), qt.DeepEquals, test.want)
			c.Assert(db.FeatureCoverage.Lookup(chschema.RowPolicyKind, test.want.Ref).State, qt.Equals, schemaext.Complete)
			syntax := must.Must(parser.NewParser(test.text, parser.WithDialect("clickhouse"), parser.WithSQL(builtintest.Runtime().SQL())).Parse())
			c.Assert(syntax.Statements, qt.HasLen, 1)
			_, ok := syntax.Statements[0].(*ast.ExtensionStatement)
			c.Assert(ok, qt.IsTrue)
			sql := must.Must(builtin.RenderSQL("clickhouse", syntax.Statements[0]))
			c.Assert(policyObject(c, policySQL(c, sql)), qt.DeepEquals, test.want)
		})
	}
}
