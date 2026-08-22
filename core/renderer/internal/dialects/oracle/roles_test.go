package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// TestOracleRoleStatements pins every role and grant shape against what a live
// server accepts.
//
// Each rendered row below was executed on 23.26.2.0.0 and answered ok; each
// refusal carries the server's own error number for the statement it declines
// to write. The renderer used to emit a comment for all of them
// (stokaro/ptah#1920).
func TestOracleRoleStatements(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		// want is the rendered statement, empty where the renderer refuses.
		want string
		// wantErr is the substring the refusal carries.
		wantErr string
	}{
		{
			name: "a plain role renders unguarded",
			node: &ast.CreateRoleNode{Name: "app_reader"},
			// No IF NOT EXISTS: a second CREATE ROLE answers ORA-01921 and the
			// clause is not accepted.
			want: "CREATE ROLE app_reader;",
		},
		{
			// In Oracle the thing that logs in is a USER. Rendering CREATE ROLE
			// here would be accepted by the server and would not create what
			// was declared.
			name:    "a login role is refused rather than turned into a role",
			node:    &ast.CreateRoleNode{Name: "app_reader", Login: true},
			wantErr: "declares LOGIN, which in Oracle describes a USER",
		},
		{
			name:    "a password is the same refusal",
			node:    &ast.CreateRoleNode{Name: "app_reader", Password: "secret"},
			wantErr: "declares a password",
		},
		{
			name:    "and so is a capability flag a role cannot carry",
			node:    &ast.CreateRoleNode{Name: "app_reader", Superuser: true},
			wantErr: "declares SUPERUSER",
		},
		{
			// Dropping an absent role answers ORA-01919, and Oracle has no
			// IF EXISTS here.
			name: "the drop is unguarded too",
			node: &ast.DropRoleNode{Name: "app_reader"},
			want: "DROP ROLE app_reader;",
		},
		{
			name: "an object privilege carries its ON clause",
			node: &ast.GrantPrivilegeNode{
				Role: "app_reader", Privileges: []string{"SELECT", "INSERT"}, ObjectName: "users",
			},
			want: "GRANT SELECT, INSERT ON users TO app_reader;",
		},
		{
			// A system privilege names no object, and an empty ON clause is a
			// syntax error rather than a harmless extra space.
			name: "a system privilege carries none",
			node: &ast.GrantPrivilegeNode{Role: "app_reader", Privileges: []string{"CREATE SESSION"}},
			want: "GRANT CREATE SESSION TO app_reader;",
		},
		{
			// The engine's own refusal: ORA-01926, measured. Emitting it would
			// render a statement the server rejects, so the plan would fail
			// halfway through instead of before it started.
			name: "WITH GRANT OPTION to a role is refused by the engine",
			node: &ast.GrantPrivilegeNode{
				Role: "app_reader", Privileges: []string{"SELECT"}, ObjectName: "users", WithOption: true,
			},
			wantErr: "ORA-01926",
		},
		{
			name: "the revoke mirrors the grant",
			node: &ast.RevokePrivilegeNode{
				Role: "app_reader", Privileges: []string{"SELECT"}, ObjectName: "users",
			},
			want: "REVOKE SELECT ON users FROM app_reader;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rendered, err := renderer.RenderSQL(platform.Oracle, test.node)

			c.Assert(renderErrorText(err), qt.Contains, test.wantErr)
			c.Assert(err != nil, qt.Equals, test.wantErr != "")
			c.Assert(renderedStatement(rendered, err), qt.Equals, test.want)
		})
	}
}

func renderErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// renderedStatement trims the renderer's trailing newlines so a row states the
// statement rather than its whitespace, and reports nothing for a refusal.
func renderedStatement(rendered string, err error) string {
	if err != nil {
		return ""
	}
	return trimTrailingNewlines(rendered)
}

func trimTrailingNewlines(text string) string {
	for len(text) > 0 && (text[len(text)-1] == '\n' || text[len(text)-1] == '\r') {
		text = text[:len(text)-1]
	}
	return text
}
