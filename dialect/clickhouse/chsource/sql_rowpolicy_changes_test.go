package chsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

const createPolicy = "CREATE ROW POLICY tenant ON app.orders USING tenant = 1 AS RESTRICTIVE TO ALL EXCEPT admin;"

type policyAlterCase struct {
	name, clause string
	want         chschema.DesiredRowPolicy
}

func policyAlterCases() []policyAlterCase {
	return []policyAlterCase{
		{"only recipients", "TO NONE", chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive}},
		{"remove filter", "USING NONE", chschema.DesiredRowPolicy{Composition: chschema.Restrictive, Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}},
		{"composition", "AS PERMISSIVE", chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}},
	}
}

func TestSQLRowPolicyAlterPreservesOmittedClausesInOneFile(t *testing.T) {
	for _, test := range policyAlterCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			text := createPolicy + "\nALTER ROW POLICY tenant ON app.orders " + test.clause + ";"
			c.Assert(policyObject(c, policySQL(c, text)).Value, qt.DeepEquals, &test.want)
		})
	}
}

func TestSQLRowPolicyAlterPreservesOmittedClausesAcrossFiles(t *testing.T) {
	for _, test := range policyAlterCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			text := "ALTER ROW POLICY tenant ON app.orders " + test.clause + ";"
			base := policySQL(c, createPolicy)
			before := policyObject(c, base)
			next, _, err := sqlschema.ReadWith(c.Context(), []byte(text), "clickhouse", sqlschema.NewDocument(&base), sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.IsNil)
			c.Assert(next.FeatureObjects.Len(), qt.Equals, 0)
			c.Assert(policyObject(c, base).Value, qt.DeepEquals, &test.want)
			c.Assert(before.Value, qt.DeepEquals, &chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}})
		})
	}
}

func TestSQLRowPolicyMigrationCommandsKeepSyntaxAndRefuseUndeclaredChanges(t *testing.T) {
	for _, test := range []struct{ text, reason string }{
		{"ALTER ROW POLICY p ON orders TO NONE;", `ALTER ROW POLICY "p" on "orders" requires an earlier CREATE ROW POLICY in this schema`},
		{"DROP ROW POLICY p ON orders;", "DROP ROW POLICY is a migration operation; a desired schema omits the policy declaration"},
	} {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)
			syntax, err := parser.NewParser(test.text, parser.WithDialect("clickhouse"), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(syntax.Statements, qt.HasLen, 1)
			command, ok := syntax.Statements[0].(*ast.ExtensionStatement).Payload.(*chast.RowPolicyCommand)
			c.Assert(ok, qt.IsTrue)
			c.Assert(command.Validate(), qt.IsNil)
			rendered := must.Must(builtin.RenderSQL("clickhouse", syntax.Statements[0]))
			again := must.Must(parser.NewParser(rendered, parser.WithDialect("clickhouse"), parser.WithSQL(builtintest.Runtime().SQL())).Parse())
			c.Assert(again.Statements[0].(*ast.ExtensionStatement).Payload, qt.DeepEquals, command)
			db, _, err := sqlschema.ReadWith(c.Context(), []byte(test.text), "clickhouse", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.reason)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

func TestSQLRowPolicySelectionDoesNotClaimUnreadableDocuments(t *testing.T) {
	c := qt.New(t)
	for _, target := range []string{"", "postgres", "mysql"} {
		known, _, err := builtintest.Runtime().SQL().Coverage(target, sqlext.CoverageRequest{})
		c.Assert(err, qt.IsNil)
		c.Assert(known.Lookup(chschema.RowPolicyKind, chschema.RowPolicyRef("", "orders", "tenant")).State, qt.Not(qt.Equals), schemaext.Complete)
		_, err = parser.NewParser(createPolicy, parser.WithDialect(target), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
		c.Assert(err, qt.IsNotNil)
	}
	_, err := parser.NewParser(createPolicy, parser.WithDialect("clickhouse"), parser.WithSQL(sqlext.None())).Parse()
	c.Assert(err, qt.ErrorMatches, `.*unsupported CREATE target: ROW.*`)
}
