package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
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

// TestRead_SQLServerPublicGrantee_HappyPath pins which grantee spellings of a
// SQL Server schema file name the fixed role public, recorded as PUBLIC, and
// which name a role of their own. Measured on SQL Server 2025, PUBLIC without
// brackets in any case and [public] reach the fixed role on a case-sensitive
// database, which also accepts CREATE ROLE [Public] and CREATE ROLE [PUBLIC]
// beside it. [PUBLIC] stays PUBLIC: the read refuses a database holding a
// role of that name.
func TestRead_SQLServerPublicGrantee_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []privilege
	}{
		{name: "PUBLIC without brackets", sql: "DENY SELECT ON dbo.t TO PUBLIC;", want: []privilege{{Role: "PUBLIC", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}}},
		{name: "mixed case without brackets", sql: "DENY SELECT ON dbo.t TO Public;", want: []privilege{{Role: "PUBLIC", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}}},
		{name: "bracketed in lower case", sql: "DENY SELECT ON dbo.t TO [public];", want: []privilege{{Role: "PUBLIC", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}}},
		{name: "bracketed in upper case", sql: "DENY SELECT ON dbo.t TO [PUBLIC];", want: []privilege{{Role: "PUBLIC", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}}},
		{name: "bracketed in mixed case", sql: "DENY SELECT ON dbo.t TO [Public];", want: []privilege{{Role: "Public", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.SQLServer)

			c.Assert(err, qt.IsNil)
			c.Assert(privileges(database.DeniedGrants), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_PaddedGrantee_HappyPath pins how a quoted grantee with white space
// around its name is read where the read accepts it. SQL Server compares names
// with trailing spaces ignored (measured on SQL Server 2025, [a ] is [a]), so
// [r ] is the role r and [public ] the fixed role public. A server that keeps
// trailing spaces keeps the grantee's complete name.
func TestRead_PaddedGrantee_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		platform string
		want     []privilege
	}{
		{
			name: "SQL Server trailing spaces", sql: "GRANT SELECT ON dbo.t TO [r  ];", platform: platform.SQLServer,
			want: []privilege{{Role: "r", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}},
		},
		{
			name: "SQL Server public with a trailing space", sql: "GRANT SELECT ON dbo.t TO [public ];", platform: platform.SQLServer,
			want: []privilege{{Role: "PUBLIC", Target: "TABLE dbo.t", Privileges: []string{"SELECT"}}},
		},
		{
			name: "PostgreSQL leading space", sql: "GRANT SELECT ON t TO \" y\";", platform: platform.Postgres,
			want: []privilege{{Role: " y", Target: "TABLE t", Privileges: []string{"SELECT"}}},
		},
		{
			name: "MariaDB leading space", sql: "GRANT INSERT ON t TO ` rr`;", platform: platform.MariaDB,
			want: []privilege{{Role: " rr", Target: "TABLE t", Privileges: []string{"INSERT"}}},
		},
		{
			name: "Oracle leading space", sql: "GRANT SELECT ON t TO \" RR\";", platform: platform.Oracle,
			want: []privilege{{Role: " RR", Target: "TABLE t", Privileges: []string{"SELECT"}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.platform)

			c.Assert(err, qt.IsNil)
			c.Assert(privileges(database.Grants), qt.DeepEquals, test.want)
		})
	}
}

func TestRead_SQLServerTrailingSpaceRoleMatchesItsGrantee(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte("CREATE ROLE [reader ]; GRANT SELECT ON dbo.t TO [reader ];"), platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Roles, qt.HasLen, 1)
	c.Assert(database.Roles[0].Name, qt.Equals, "reader")
	c.Assert(database.Grants, qt.HasLen, 1)
	c.Assert(database.Grants[0].Role, qt.Equals, "reader")
}

// TestRead_SQLServerPaddedGrantee_FailurePath keeps the SQL Server schema
// reader's principal-name restriction aligned with its catalog reader.
func TestRead_SQLServerPaddedGrantee_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name: "a deny to a name after a space", sql: "DENY SELECT ON dbo.t TO [ public];",
			wantErr: `.*the grantee \[ public\] uses surrounding white space that Ptah does not support on this target; rename the principal`,
		},
		{
			name: "a grant to a name after a space", sql: "GRANT SELECT ON dbo.t TO [ r];",
			wantErr: `.*the grantee \[ r\] uses surrounding white space that Ptah does not support on this target; .*`,
		},
		{
			name: "a revoke from a name after a space", sql: "REVOKE SELECT ON dbo.t FROM [ r];",
			wantErr: `.*the grantee \[ r\] uses surrounding white space that Ptah does not support on this target; .*`,
		},
		{
			name: "a grant to a name before a tab", sql: "GRANT SELECT ON dbo.t TO [r\t];",
			wantErr: `(?s).*the grantee \[r\s\] uses surrounding white space that Ptah does not support on this target; .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.SQLServer)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Grants, qt.IsNil)
			c.Assert(database.RevokedGrants, qt.IsNil)
			c.Assert(database.DeniedGrants, qt.IsNil)
		})
	}
}
