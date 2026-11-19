package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// deferredCheckSchema is a table whose named CHECK defers its check to the
// end of the transaction.
func deferredCheckSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Slot", Name: "slots", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Slot", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Slot", Name: "position", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Slot", Table: "slots", Name: "slots_position_ck", Type: "CHECK",
			CheckExpression: "position > 0", Deferrable: true, Initially: "deferred",
		}},
	}
}

// deferredCheckAddition is ALTER TABLE ... ADD of a deferred CHECK, the
// statement a plan writes when a CHECK starts deferring its check.
func deferredCheckAddition() *ast.AlterTableNode {
	check := &ast.ConstraintNode{
		Type: ast.CheckConstraint, Name: "slots_position_ck", Expression: "position > 0",
		Deferrable: true, Initially: "deferred",
	}
	return &ast.AlterTableNode{
		Name:       "slots",
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: check}},
	}
}

// TestRenderCheckDeferral_HappyPath writes the deferral of a CHECK on Oracle,
// in the table and in an added constraint. Oracle Free 23.26 creates both
// statements and reports the CHECK DEFERRABLE DEFERRED in USER_CONSTRAINTS.
func TestRenderCheckDeferral_HappyPath(t *testing.T) {
	c := qt.New(t)

	statements, createErr := builtin.GetOrderedCreateStatementsWithCapabilities(deferredCheckSchema(), "oracle", capability.ForDialect("oracle"))
	added, addErr := builtin.RenderSQLWithCapabilities("oracle", capability.ForDialect("oracle"), deferredCheckAddition())

	c.Assert(createErr, qt.IsNil)
	c.Assert(addErr, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "CONSTRAINT slots_position_ck CHECK (position > 0) DEFERRABLE INITIALLY DEFERRED")
	c.Assert(added, qt.Contains, "ALTER TABLE slots ADD CONSTRAINT slots_position_ck CHECK (position > 0) DEFERRABLE INITIALLY DEFERRED;")
}

// TestRenderCheckDeferral_UnsupportedTarget refuses a deferred CHECK on a
// target that cannot defer one rather than write it plain. PostgreSQL 18.6
// answers `CHECK constraints cannot be marked DEFERRABLE`, though it defers a
// key.
func TestRenderCheckDeferral_UnsupportedTarget(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{name: "PostgreSQL", dialect: "postgres", caps: capability.Postgres18()},
		{name: "MySQL", dialect: "mysql", caps: capability.ForDialect("mysql")},
		{name: "SQL Server", dialect: "sqlserver", caps: capability.ForDialect("sqlserver")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			statements, createErr := builtin.GetOrderedCreateStatementsWithCapabilities(deferredCheckSchema(), tt.dialect, tt.caps)
			added, addErr := builtin.RenderSQLWithCapabilities(tt.dialect, tt.caps, deferredCheckAddition())

			c.Assert(createErr, qt.ErrorMatches, tt.dialect+` does not support a DEFERRABLE CHECK; constraint "slots_position_ck" declares one`)
			c.Assert(createErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
			c.Assert(addErr, qt.ErrorMatches, `.*`+tt.dialect+` does not support a DEFERRABLE CHECK; constraint "slots_position_ck" declares one`)
			c.Assert(addErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(added, qt.Equals, "")
		})
	}
}
