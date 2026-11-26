package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// sqlServerRow is a row of r on dbo.t, which dbo owns, that dbo gave, as the
// SQL Server read reports one: state is G, W or D.
func sqlServerRow(state, privilege, objectType, object, column string) catalog.Grant {
	schema := "dbo"
	if objectType == "SCHEMA" {
		schema = ""
	}
	return catalog.Grant{
		Role: "r", Privilege: privilege, ObjectType: objectType, Schema: schema, ObjectName: object, Column: column,
		WithOption: state == "W", IsPartialRevoke: state == "D", Denied: state == "D", GrantedBy: "dbo", ObjectOwner: "dbo",
	}
}

// planSQLServerRows plans both directions of desired against a database that
// holds dbo.t(id, a, b) and the rows held, read as deployer, which owns
// nothing and runs the plan with CONTROL, and renders them.
func planSQLServerRows(c *qt.C, desired *schemamodel.Database, held []catalog.Grant) (up, down []string) {
	c.Helper()
	runtime := must.Must(builtin.New())
	desired.Tables = []schemamodel.Table{{Name: "t", Schema: "dbo", StructName: "T"}}
	desired.Fields = []schemamodel.Field{
		{Name: "id", StructName: "T", Type: "INT", Primary: true},
		{Name: "a", StructName: "T", Type: "INT", Nullable: true},
		{Name: "b", StructName: "T", Type: "INT", Nullable: true},
	}
	current, err := goschematodb.ToDBSchema(c.Context(), &schemamodel.Database{Tables: desired.Tables, Fields: desired.Fields}, "sqlserver", runtime)
	c.Assert(err, qt.IsNil)
	current.Grants = held
	current.ConnectedRole = "deployer"
	caps := capability.SQLServer2022()
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, catalog.ServerInfo{Dialect: "sqlserver", Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)

	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "sqlserver", Capabilities: caps,
	})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQL("sqlserver", plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQL("sqlserver", plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	return statementHeads(forward), statementHeads(reverse)
}

// TestPlanBidirectionalSchemaDiff_SQLServerRollbackGivesBackReplacedRows_HappyPath
// pins the down migration of a change that replaces a row of a grantee the
// schema does not manage. SQL Server keeps one row for a permission of a
// grantee on a securable, so a DENY takes the place of a GRANT and a GRANT
// takes the place of a DENY. A REVOKE alone takes the new row away and leaves
// the grantee holding nothing; the down migration grants or denies the
// replaced row again, with its grant option, after the REVOKE.
func TestPlanBidirectionalSchemaDiff_SQLServerRollbackGivesBackReplacedRows_HappyPath(t *testing.T) {
	deny := func(privilege, table string, columns ...string) schemamodel.Grant {
		return schemamodel.Grant{Role: "r", Privileges: []string{privilege}, OnTable: table, Columns: columns}
	}
	tests := []struct {
		name     string
		denied   []schemamodel.Grant
		granted  []schemamodel.Grant
		held     []catalog.Grant
		up, down []string
	}{
		{
			name:   "a grant a deny replaces",
			denied: []schemamodel.Grant{deny("SELECT", "dbo.t")},
			held:   []catalog.Grant{sqlServerRow("G", "SELECT", "TABLE", "t", "")},
			up:     []string{"DENY SELECT ON [dbo].[t] TO [r]"},
			down:   []string{"REVOKE SELECT ON [dbo].[t] FROM [r]", "GRANT SELECT ON [dbo].[t] TO [r]"},
		},
		{
			name:   "a grant with the grant option a deny replaces",
			denied: []schemamodel.Grant{deny("SELECT", "dbo.t")},
			held:   []catalog.Grant{sqlServerRow("W", "SELECT", "TABLE", "t", "")},
			up:     []string{"DENY SELECT ON [dbo].[t] TO [r] CASCADE"},
			down:   []string{"REVOKE SELECT ON [dbo].[t] FROM [r]", "GRANT SELECT ON [dbo].[t] TO [r] WITH GRANT OPTION"},
		},
		{
			name:    "a deny a grant replaces",
			granted: []schemamodel.Grant{deny("SELECT", "dbo.t")},
			held:    []catalog.Grant{sqlServerRow("D", "SELECT", "TABLE", "t", "")},
			up:      []string{"GRANT SELECT ON [dbo].[t] TO [r]"},
			down:    []string{"REVOKE SELECT ON [dbo].[t] FROM [r]", "DENY SELECT ON [dbo].[t] TO [r]"},
		},
		{
			name: "a deny a grant with the grant option replaces",
			granted: []schemamodel.Grant{
				{Role: "r", Privileges: []string{"SELECT"}, OnTable: "dbo.t", WithOption: true},
			},
			held: []catalog.Grant{sqlServerRow("D", "SELECT", "TABLE", "t", "")},
			up:   []string{"GRANT SELECT ON [dbo].[t] TO [r] WITH GRANT OPTION"},
			down: []string{
				"REVOKE GRANT OPTION FOR SELECT ON [dbo].[t] FROM [r] CASCADE",
				"REVOKE SELECT ON [dbo].[t] FROM [r]",
				"DENY SELECT ON [dbo].[t] TO [r]",
			},
		},
		{
			name:   "a column grant a column deny replaces",
			denied: []schemamodel.Grant{deny("SELECT", "dbo.t", "a")},
			held:   []catalog.Grant{sqlServerRow("G", "SELECT", "TABLE", "t", "a")},
			up:     []string{"DENY SELECT ([a]) ON [dbo].[t] TO [r]"},
			down:   []string{"REVOKE SELECT ([a]) ON [dbo].[t] FROM [r]", "GRANT SELECT ([a]) ON [dbo].[t] TO [r]"},
		},
		{
			name:    "a column deny a column grant replaces",
			granted: []schemamodel.Grant{deny("SELECT", "dbo.t", "a")},
			held:    []catalog.Grant{sqlServerRow("D", "SELECT", "TABLE", "t", "a")},
			up:      []string{"GRANT SELECT ([a]) ON [dbo].[t] TO [r]"},
			down:    []string{"REVOKE SELECT ([a]) ON [dbo].[t] FROM [r]", "DENY SELECT ([a]) ON [dbo].[t] TO [r]"},
		},
		{
			name:   "a column grant and a column deny a deny on the table takes",
			denied: []schemamodel.Grant{deny("SELECT", "dbo.t")},
			held: []catalog.Grant{
				sqlServerRow("G", "SELECT", "TABLE", "t", "a"),
				sqlServerRow("D", "SELECT", "TABLE", "t", "b"),
			},
			up: []string{"DENY SELECT ON [dbo].[t] TO [r]"},
			down: []string{
				"REVOKE SELECT ON [dbo].[t] FROM [r]",
				"GRANT SELECT ([a]) ON [dbo].[t] TO [r]",
				"DENY SELECT ([b]) ON [dbo].[t] TO [r]",
			},
		},
		{
			name:   "a table grant and a column deny a deny on the table takes",
			denied: []schemamodel.Grant{deny("SELECT", "dbo.t")},
			held: []catalog.Grant{
				sqlServerRow("G", "SELECT", "TABLE", "t", ""),
				sqlServerRow("D", "SELECT", "TABLE", "t", "a"),
			},
			up: []string{"DENY SELECT ON [dbo].[t] TO [r]"},
			down: []string{
				"REVOKE SELECT ON [dbo].[t] FROM [r]",
				"GRANT SELECT ON [dbo].[t] TO [r]",
				"DENY SELECT ([a]) ON [dbo].[t] TO [r]",
			},
		},
		{
			name:   "a table grant with the grant option and a column deny a deny on the table takes",
			denied: []schemamodel.Grant{deny("SELECT", "dbo.t")},
			held: []catalog.Grant{
				sqlServerRow("W", "SELECT", "TABLE", "t", ""),
				sqlServerRow("D", "SELECT", "TABLE", "t", "a"),
			},
			up: []string{"DENY SELECT ON [dbo].[t] TO [r] CASCADE"},
			down: []string{
				"REVOKE SELECT ON [dbo].[t] FROM [r]",
				"GRANT SELECT ON [dbo].[t] TO [r] WITH GRANT OPTION",
				"DENY SELECT ([a]) ON [dbo].[t] TO [r] CASCADE",
			},
		},
		{
			name:   "a schema grant a schema deny replaces",
			denied: []schemamodel.Grant{{Role: "r", Privileges: []string{"SELECT"}, OnSchema: "dbo"}},
			held:   []catalog.Grant{sqlServerRow("G", "SELECT", "SCHEMA", "dbo", "")},
			up:     []string{"DENY SELECT ON SCHEMA::[dbo] TO [r]"},
			down:   []string{"REVOKE SELECT ON SCHEMA::[dbo] FROM [r]", "GRANT SELECT ON SCHEMA::[dbo] TO [r]"},
		},
		{
			name:    "a schema deny a schema grant replaces",
			granted: []schemamodel.Grant{{Role: "r", Privileges: []string{"SELECT"}, OnSchema: "dbo"}},
			held:    []catalog.Grant{sqlServerRow("D", "SELECT", "SCHEMA", "dbo", "")},
			up:      []string{"GRANT SELECT ON SCHEMA::[dbo] TO [r]"},
			down:    []string{"REVOKE SELECT ON SCHEMA::[dbo] FROM [r]", "DENY SELECT ON SCHEMA::[dbo] TO [r]"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			up, down := planSQLServerRows(c, &schemamodel.Database{DeniedGrants: test.denied, Grants: test.granted}, test.held)

			c.Assert(up, qt.DeepEquals, test.up)
			c.Assert(down, qt.DeepEquals, test.down)
		})
	}
}

// TestPlanBidirectionalSchemaDiff_SQLServerRollbackLeavesAnotherGrantorsRow_HappyPath
// pins that a row another principal gave, from its own grant option, is not
// granted again: the DENY leaves it in place, measured on SQL Server 2025, so
// the REVOKE of the DENY is the whole rollback.
func TestPlanBidirectionalSchemaDiff_SQLServerRollbackLeavesAnotherGrantorsRow_HappyPath(t *testing.T) {
	c := qt.New(t)
	held := sqlServerRow("G", "SELECT", "TABLE", "t", "")
	held.GrantedBy = "u"
	denied := []schemamodel.Grant{{Role: "r", Privileges: []string{"SELECT"}, OnTable: "dbo.t"}}

	up, down := planSQLServerRows(c, &schemamodel.Database{DeniedGrants: denied}, []catalog.Grant{held})

	c.Assert(up, qt.DeepEquals, []string{"DENY SELECT ON [dbo].[t] TO [r]"})
	c.Assert(down, qt.DeepEquals, []string{"REVOKE SELECT ON [dbo].[t] FROM [r]"})
}
