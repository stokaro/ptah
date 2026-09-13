package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// TestParser_ParseAlterDefaultPrivilegesGrant_HappyPath pins what an
// ALTER DEFAULT PRIVILEGES ... GRANT statement parses into.
//
// The whole node is compared rather than one field at a time. The five strings
// it carries are four identity components and an object class, all of them
// plain strings, so a parser that filed the schema as the grantee would pass
// every per-field assertion an author is likely to write.
//
// Each row is a statement the PostgreSQL renderer emits, because reading them
// back is what the grammar exists for: `ptah db read` renders the statement,
// and a frontend that cannot read it cannot read Ptah's own output.
func TestParser_ParseAlterDefaultPrivilegesGrant_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want ast.DefaultPrivilegeNode
	}{
		{
			name: "one privilege on tables",
			sql:  "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;",
			want: ast.DefaultPrivilegeNode{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []ast.DefaultPrivilege{{Privilege: "SELECT"}},
			},
		},
		{
			name: "several privileges keep their order",
			sql:  "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT SELECT, INSERT, UPDATE ON TABLES TO app_writer;",
			want: ast.DefaultPrivilegeNode{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_writer",
				Privileges: []ast.DefaultPrivilege{
					{Privilege: "SELECT"},
					{Privilege: "INSERT"},
					{Privilege: "UPDATE"},
				},
			},
		},
		{
			name: "with grant option reaches every privilege named",
			sql:  "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT SELECT, INSERT ON TABLES TO app_writer WITH GRANT OPTION;",
			want: ast.DefaultPrivilegeNode{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_writer",
				Privileges: []ast.DefaultPrivilege{
					{Privilege: "SELECT", WithOption: true},
					{Privilege: "INSERT", WithOption: true},
				},
			},
		},
		{
			name: "sequences",
			sql:  "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT USAGE ON SEQUENCES TO app_writer;",
			want: ast.DefaultPrivilegeNode{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "SEQUENCES",
				Grantee:    "app_writer",
				Privileges: []ast.DefaultPrivilege{{Privilege: "USAGE"}},
			},
		},
		{
			name: "functions",
			sql:  "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT EXECUTE ON FUNCTIONS TO app_reader;",
			want: ast.DefaultPrivilegeNode{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "FUNCTIONS",
				Grantee:    "app_reader",
				Privileges: []ast.DefaultPrivilege{{Privilege: "EXECUTE"}},
			},
		},
		{
			name: "types granted to public",
			sql:  "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT USAGE ON TYPES TO PUBLIC;",
			want: ast.DefaultPrivilegeNode{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TYPES",
				Grantee:    "PUBLIC",
				Privileges: []ast.DefaultPrivilege{{Privilege: "USAGE"}},
			},
		},
		{
			name: "quoted identifiers keep the spelling the statement used",
			sql:  `ALTER DEFAULT PRIVILEGES FOR ROLE "App Owner" IN SCHEMA "App" GRANT SELECT ON TABLES TO "App Reader";`,
			want: ast.DefaultPrivilegeNode{
				Grantor:    `"App Owner"`,
				Schema:     `"App"`,
				ObjectType: "TABLES",
				Grantee:    `"App Reader"`,
				Privileges: []ast.DefaultPrivilege{{Privilege: "SELECT"}},
			},
		},
		{
			name: "lower case keywords",
			sql:  "alter default privileges for role app_owner in schema app grant select on tables to app_reader;",
			want: ast.DefaultPrivilegeNode{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []ast.DefaultPrivilege{{Privilege: "SELECT"}},
			},
		},
		{
			name: "FOR USER names the same catalog entry as FOR ROLE",
			sql:  "ALTER DEFAULT PRIVILEGES FOR USER app_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;",
			want: ast.DefaultPrivilegeNode{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []ast.DefaultPrivilege{{Privilege: "SELECT"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			node, ok := statements.Statements[0].(*ast.DefaultPrivilegeNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(*node, qt.DeepEquals, test.want)
		})
	}
}

// TestParser_ParseAlterDefaultPrivilegesRevoke_HappyPath pins the REVOKE form,
// which is a sibling node rather than a flag on the grant.
//
// The GRANT OPTION FOR row is the one that carries weight: it leaves the
// privilege in place and takes back the right to pass it on, so reading it as a
// plain revoke would report the grantee losing an access it still has.
func TestParser_ParseAlterDefaultPrivilegesRevoke_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want ast.RevokeDefaultPrivilegeNode
	}{
		{
			name: "plain revoke",
			sql:  "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app REVOKE SELECT ON TABLES FROM app_reader;",
			want: ast.RevokeDefaultPrivilegeNode{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []string{"SELECT"},
			},
		},
		{
			name: "grant option for",
			sql:  "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app REVOKE GRANT OPTION FOR INSERT ON TABLES FROM app_writer;",
			want: ast.RevokeDefaultPrivilegeNode{
				Grantor:        "app_owner",
				Schema:         "app",
				ObjectType:     "TABLES",
				Grantee:        "app_writer",
				Privileges:     []string{"INSERT"},
				GrantOptionFor: true,
			},
		},
		{
			name: "several privileges from public",
			sql:  "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app REVOKE SELECT, UPDATE ON SEQUENCES FROM PUBLIC;",
			want: ast.RevokeDefaultPrivilegeNode{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "SEQUENCES",
				Grantee:    "PUBLIC",
				Privileges: []string{"SELECT", "UPDATE"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			node, ok := statements.Statements[0].(*ast.RevokeDefaultPrivilegeNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(*node, qt.DeepEquals, test.want)
		})
	}
}

// TestParser_ParseAlterDefaultPrivileges_FailurePath pins the statements this
// grammar declines.
//
// Every one of them is valid PostgreSQL that the node cannot hold. Accepting
// any would build a node with an empty identity component or a dropped name,
// and the PostgreSQL renderer would then decline to spell it -- so the
// statement would be read and lost rather than read and refused, which is a
// desired schema quietly missing the access control its document declared.
func TestParser_ParseAlterDefaultPrivileges_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "no IN SCHEMA sets the cluster-wide default",
			sql:     "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner GRANT SELECT ON TABLES TO app_reader;",
			wantErr: `ALTER DEFAULT PRIVILEGES requires IN SCHEMA: the cluster-wide default has no representation here`,
		},
		{
			name:    "no FOR ROLE leaves the grantor to the session",
			sql:     "ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;",
			wantErr: `ALTER DEFAULT PRIVILEGES requires FOR ROLE: the grantor is part of the default's identity`,
		},
		{
			name:    "an object class the model has no spelling for",
			sql:     "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT USAGE ON SCHEMAS TO app_reader;",
			wantErr: `unsupported ALTER DEFAULT PRIVILEGES object class: SCHEMAS at position \d+`,
		},
		{
			name:    "a list of grantor roles",
			sql:     "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner, other_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;",
			wantErr: `ALTER DEFAULT PRIVILEGES names one grantor role here, not a list, at position \d+`,
		},
		{
			name:    "a list of schemas",
			sql:     "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app, other GRANT SELECT ON TABLES TO app_reader;",
			wantErr: `ALTER DEFAULT PRIVILEGES names one schema here, not a list, at position \d+`,
		},
		{
			name:    "a list of grantees",
			sql:     "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader, other_reader;",
			wantErr: `ALTER DEFAULT PRIVILEGES names one grantee here, not a list, at position \d+`,
		},
		{
			name:    "neither GRANT nor REVOKE follows the scope",
			sql:     "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app SELECT ON TABLES TO app_reader;",
			wantErr: `expected GRANT or REVOKE in ALTER DEFAULT PRIVILEGES at position \d+`,
		},
		{
			name:    "FOR names something other than a role",
			sql:     "ALTER DEFAULT PRIVILEGES FOR GROUP app_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;",
			wantErr: `expected ROLE or USER after FOR in ALTER DEFAULT PRIVILEGES at position \d+`,
		},
		{
			name:    "PRIVILEGES misspelled",
			sql:     "ALTER DEFAULT PRIVILEGE FOR ROLE app_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;",
			wantErr: `expected PRIVILEGES after ALTER DEFAULT: expected 'PRIVILEGES', got 'PRIVILEGE' at position \d+`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestParser_ParseAlterTargetDispatch pins that branching on the keyword after
// ALTER left ALTER TABLE where it was, and that a target nobody wired is named
// rather than reported as a missing TABLE keyword.
//
// The ALTER TABLE row is the control. Reading the target before the TABLE
// expectation is the change that made ALTER DEFAULT PRIVILEGES reachable, and
// nothing else in this file would notice if it had swallowed the statement the
// rest of the parser is built around.
func TestParser_ParseAlterTargetDispatch(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("ALTER TABLE users ADD COLUMN email TEXT;").Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	alter, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alter.Name, qt.Equals, "users")

	_, err = parser.NewParser("ALTER SEQUENCE app.counter RESTART;").Parse()
	c.Assert(err, qt.ErrorMatches, `unsupported ALTER target: SEQUENCE at position \d+`)
}
