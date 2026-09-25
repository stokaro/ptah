package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// TestParser_ParseGrantOnRoutine_HappyPath pins what a GRANT on a function,
// procedure or routine parses into. The argument types are part of the node
// because PostgreSQL overloads a routine name by them, and a node that dropped
// them could not say which overload the privilege is on.
func TestParser_ParseGrantOnRoutine_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want ast.GrantPrivilegeNode
	}{
		{
			name: "a function with one argument",
			sql:  "GRANT EXECUTE ON FUNCTION purge_workspace(uuid) TO wpmgr_app;",
			want: ast.GrantPrivilegeNode{
				Role: "wpmgr_app", Privileges: []string{"EXECUTE"},
				ObjectType: "FUNCTION", ObjectName: "purge_workspace", Arguments: "uuid",
			},
		},
		{
			name: "a qualified procedure with no arguments",
			sql:  "GRANT EXECUTE ON PROCEDURE app.archive() TO app_role;",
			want: ast.GrantPrivilegeNode{
				Role: "app_role", Privileges: []string{"EXECUTE"},
				ObjectType: "PROCEDURE", ObjectName: "app.archive",
			},
		},
		{
			name: "ROUTINE and ALL PRIVILEGES",
			sql:  "GRANT ALL PRIVILEGES ON ROUTINE f(integer, text) TO app_role WITH GRANT OPTION;",
			want: ast.GrantPrivilegeNode{
				Role: "app_role", Privileges: []string{"ALL"},
				ObjectType: "ROUTINE", ObjectName: "f", Arguments: "integer, text", WithOption: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			node, ok := statements.Statements[0].(*ast.GrantPrivilegeNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(*node, qt.DeepEquals, test.want)
		})
	}
}

// TestParser_ParseRevoke_HappyPath pins the REVOKE forms a schema file writes.
// The first two rows are the statements mosamlife/wpmgr's schema carries: the
// PUBLIC revoke on a SECURITY DEFINER function, and a table revoke taking back
// what ALTER DEFAULT PRIVILEGES handed out.
func TestParser_ParseRevoke_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want ast.RevokePrivilegeNode
	}{
		{
			name: "REVOKE ALL on a function from PUBLIC",
			sql:  "REVOKE ALL ON FUNCTION purge_workspace(uuid) FROM PUBLIC;",
			want: ast.RevokePrivilegeNode{
				Role: "PUBLIC", Privileges: []string{"ALL"},
				ObjectType: "FUNCTION", ObjectName: "purge_workspace", Arguments: "uuid",
			},
		},
		{
			name: "several privileges on a table named without the keyword",
			sql:  "REVOKE INSERT, UPDATE, DELETE ON plugin_signatures FROM wpmgr_app;",
			want: ast.RevokePrivilegeNode{
				Role: "wpmgr_app", Privileges: []string{"INSERT", "UPDATE", "DELETE"},
				ObjectType: "TABLE", ObjectName: "plugin_signatures",
			},
		},
		{
			name: "GRANT OPTION FOR keeps the privilege and takes the option",
			sql:  "REVOKE GRANT OPTION FOR SELECT ON TABLE t FROM r;",
			want: ast.RevokePrivilegeNode{
				Role: "r", Privileges: []string{"SELECT"},
				ObjectType: "TABLE", ObjectName: "t", GrantOptionFor: true,
			},
		},
		{
			name: "RESTRICT is the default and changes nothing",
			sql:  "revoke usage on schema app from r restrict;",
			want: ast.RevokePrivilegeNode{
				Role: "r", Privileges: []string{"USAGE"}, ObjectType: "SCHEMA", ObjectName: "app",
			},
		},
		{
			name: "a sequence",
			sql:  "REVOKE USAGE, SELECT ON SEQUENCE order_seq FROM r;",
			want: ast.RevokePrivilegeNode{
				Role: "r", Privileges: []string{"USAGE", "SELECT"}, ObjectType: "SEQUENCE", ObjectName: "order_seq",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			node, ok := statements.Statements[0].(*ast.RevokePrivilegeNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(*node, qt.DeepEquals, test.want)
		})
	}
}

// TestParser_ParseObjectPrivileges_FailurePath pins the forms GRANT and REVOKE
// refuse by name. Each one would otherwise be read as a different statement: a
// wider privilege, a routine the file cannot tie to an overload, or a grant on
// objects nobody can list offline.
func TestParser_ParseObjectPrivileges_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "a routine named without its argument types",
			sql:     "GRANT EXECUTE ON FUNCTION purge_workspace TO r;",
			wantErr: `GRANT \.\.\. ON FUNCTION purge_workspace needs the argument types at position \d+, as in purge_workspace\(uuid\): a routine's identity includes them`,
		},
		{
			name:    "a REVOKE on a routine named without its argument types",
			sql:     "REVOKE ALL ON PROCEDURE archive FROM PUBLIC;",
			wantErr: `REVOKE \.\.\. ON PROCEDURE archive needs the argument types .*`,
		},
		{
			name:    "a column privilege in a GRANT",
			sql:     "GRANT UPDATE (state, decided_at) ON assistant_update_proposals TO wpmgr_app;",
			wantErr: `column privileges are not supported: GRANT UPDATE \(\.\.\.\) at position \d+ grants on columns, and a grant here covers a whole object`,
		},
		{
			name:    "a column privilege in a REVOKE",
			sql:     "REVOKE SELECT (secret) ON accounts FROM r;",
			wantErr: `column privileges are not supported: REVOKE SELECT \(\.\.\.\) .*`,
		},
		{
			name:    "ALL ... IN SCHEMA",
			sql:     "GRANT SELECT ON ALL TABLES IN SCHEMA app TO r;",
			wantErr: `GRANT \.\.\. ON ALL \.\.\. IN SCHEMA is not supported at position \d+: it applies to the objects that exist when it runs, so name each object instead`,
		},
		{
			name:    "a list of grantees in a REVOKE",
			sql:     "REVOKE SELECT ON t FROM a, b;",
			wantErr: `REVOKE names one grantee here, not a list, at position \d+: write one statement per role`,
		},
		{
			name:    "GRANTED BY",
			sql:     "REVOKE SELECT ON t FROM a GRANTED BY owner;",
			wantErr: `REVOKE \.\.\. GRANTED BY is not supported at position \d+: grants are not keyed by grantor here`,
		},
		{
			name:    "CASCADE",
			sql:     "REVOKE SELECT ON t FROM a CASCADE;",
			wantErr: `REVOKE \.\.\. CASCADE is not supported at position \d+: it also revokes what the role granted onward, which a desired-state file cannot see`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.wantErr+`.*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}
