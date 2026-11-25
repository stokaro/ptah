package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// TestRead_Deny_HappyPath pins how a SQL Server schema file's DENY composes
// with the GRANT and REVOKE statements around it. The server keeps one row for
// a permission of one grantee on one securable, and a statement on a whole
// table reaches the rows of its columns, measured on SQL Server 2025: a DENY
// takes the place of the GRANT before it, a GRANT or a REVOKE takes the DENY,
// and a DENY on the table takes the column grants and denies of the
// permission. A column grant after a DENY on its table keeps both rows, and
// so does a column DENY after a grant on its table.
func TestRead_Deny_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantDenied  []privilege
		wantGrants  []privilege
		wantRevoked []privilege
	}{
		{
			name:       "a deny on a table",
			sql:        "DENY SELECT ON dbo.t TO app;",
			wantDenied: []privilege{{Role: "app", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}},
		},
		{
			name:       "a deny on columns",
			sql:        "DENY SELECT ([secret], [pin]) ON dbo.t TO app;",
			wantDenied: []privilege{{Role: "app", Target: "TABLE dbo.t (secret)", Privileges: []string{"SELECT"}}, {Role: "app", Target: "TABLE dbo.t (pin)", Privileges: []string{"SELECT"}}},
		},
		{
			name:       "a deny on a schema",
			sql:        "DENY DELETE ON SCHEMA::app TO app;",
			wantDenied: []privilege{{Role: "app", Target: "SCHEMA app", Privileges: []string{"DELETE"}}},
		},
		{
			name:       "a deny after a grant takes its place",
			sql:        "GRANT SELECT ON dbo.t TO app;\nDENY SELECT ON dbo.t TO app;",
			wantDenied: []privilege{{Role: "app", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}},
		},
		{
			name:       "a grant after a deny takes its place",
			sql:        "DENY SELECT ON dbo.t TO app;\nGRANT SELECT ON dbo.t TO app;",
			wantGrants: []privilege{{Role: "app", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}},
		},
		{
			name:        "a revoke after a deny takes it",
			sql:         "DENY SELECT ON dbo.t TO app;\nREVOKE SELECT ON dbo.t FROM app;",
			wantRevoked: []privilege{{Role: "app", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}},
		},
		{
			name:       "a deny on a table takes the column grants and denies",
			sql:        "GRANT SELECT (name) ON dbo.t TO app;\nDENY SELECT (secret) ON dbo.t TO app;\nDENY SELECT ON dbo.t TO app;",
			wantDenied: []privilege{{Role: "app", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}},
		},
		{
			name:       "a column grant after a deny on its table",
			sql:        "DENY SELECT ON dbo.t TO app;\nGRANT SELECT (name) ON dbo.t TO app;",
			wantDenied: []privilege{{Role: "app", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}},
			wantGrants: []privilege{{Role: "app", Target: "TABLE dbo.t (name)", Privileges: []string{"SELECT"}}},
		},
		{
			name:       "a column deny under a deny on its table adds nothing",
			sql:        "DENY SELECT ON dbo.t TO app;\nDENY SELECT (secret) ON dbo.t TO app;",
			wantDenied: []privilege{{Role: "app", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}},
		},
		{
			name:       "a column deny after a grant on its table",
			sql:        "GRANT SELECT ON dbo.t TO app;\nDENY SELECT (secret) ON dbo.t TO app;",
			wantDenied: []privilege{{Role: "app", Target: "TABLE dbo.t (secret)", Privileges: []string{"SELECT"}}},
			wantGrants: []privilege{{Role: "app", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}},
		},
		{
			name:       "a grant on a table takes the column denies",
			sql:        "DENY SELECT (secret) ON dbo.t TO app;\nGRANT SELECT ON dbo.t TO app;",
			wantGrants: []privilege{{Role: "app", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.SQLServer)

			c.Assert(err, qt.IsNil)
			c.Assert(privileges(database.DeniedGrants), qt.DeepEquals, test.wantDenied)
			c.Assert(privileges(database.Grants), qt.DeepEquals, test.wantGrants)
			c.Assert(privileges(database.RevokedGrants), qt.DeepEquals, test.wantRevoked)
		})
	}
}

// TestRead_Deny_FailurePath refuses a DENY the schema cannot keep: on a target
// with no DENY, one whose CASCADE takes grants the file does not name, one
// keyed by a grantor, and one of ALL, which names different permissions on
// each kind of object.
func TestRead_Deny_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "a deny on PostgreSQL",
			dialect: platform.Postgres,
			sql:     "DENY SELECT ON t TO app;",
			wantErr: `line 1, column 1: unsupported SQL statement: DENY`,
		},
		{
			name:    "a deny with cascade",
			dialect: platform.SQLServer,
			sql:     "DENY SELECT ON dbo.t TO app CASCADE;",
			wantErr: `line 1, column 29: DENY \.\.\. CASCADE is not supported: it also denies the privileges to whoever the grantee granted them to, which a desired-state file cannot see`,
		},
		{
			name:    "a deny as a grantor",
			dialect: platform.SQLServer,
			sql:     "DENY SELECT ON dbo.t TO app AS dbo;",
			wantErr: `line 1, column 29: DENY \.\.\. AS is not supported: grants are not keyed by grantor here`,
		},
		{
			name:    "a deny of all",
			dialect: platform.SQLServer,
			sql:     "DENY ALL ON dbo.t TO app;",
			wantErr: `line 1, column 1: DENY ALL ON TABLE dbo\.t TO app: ALL names different permissions on each kind of object; name the permissions to deny`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.DeniedGrants, qt.IsNil)
		})
	}
}
