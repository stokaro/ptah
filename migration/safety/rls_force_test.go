package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestClassify_RLSForce reads NO FORCE the way DISABLE is read: afterwards the
// table's owner, often the role the application connects as, reads and writes
// past every policy the table keeps. FORCE only adds protection.
func TestClassify_RLSForce(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want safety.Severity
	}{
		{name: "no force node", node: ast.NewAlterTableForceRLS("accounts").SetNoForce(), want: safety.Destructive},
		{name: "no force text", node: ast.NewRawSQL("ALTER TABLE accounts NO FORCE ROW LEVEL SECURITY"), want: safety.Destructive},
		{name: "force node", node: ast.NewAlterTableForceRLS("accounts"), want: safety.Safe},
		{name: "force text", node: ast.NewRawSQL("ALTER TABLE accounts FORCE ROW LEVEL SECURITY"), want: safety.Safe},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(safety.Classify(test.node), qt.Equals, test.want)
		})
	}
}

// TestClassifySchemaDiff_RLSForce counts each direction under its own category,
// so a plan that turns FORCE off does not pass a destructive gate as a plan
// that turns it on.
func TestClassifySchemaDiff_RLSForce(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{RLSForceChanged: difftypes.RLSForceChanges{
		{Table: "public.a", Forced: true},
		{Table: "public.b", Forced: false},
		{Table: "public.c", Forced: false},
	}}

	findings := safety.ClassifySchemaDiff(diff)

	c.Assert(findings, qt.DeepEquals, []safety.Finding{
		{Category: "rls_force_removed", Count: 2, Severity: safety.Destructive},
		{Category: "rls_force_added", Count: 1, Severity: safety.Safe},
	})
	c.Assert(safety.HasDestructive(findings), qt.IsTrue)
}
