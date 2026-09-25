package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// TestRead_ColumnPrivileges_HappyPath pins how a column list composes with the
// statements around it. A column grant is kept one column at a time, and a
// revoke of the table privilege also takes it off every column, as PostgreSQL
// 18 does: REVOKE UPDATE ON t after GRANT UPDATE (a) ON t empties the column's
// ACL. The first row is the pair mosamlife/wpmgr's schema file writes.
func TestRead_ColumnPrivileges_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantGrants  []privilege
		wantRevoked []privilege
	}{
		{
			name: "a table revoke followed by a column grant",
			sql: "REVOKE UPDATE ON proposals FROM app;\n" +
				"GRANT UPDATE (state, decided_at) ON proposals TO app;",
			wantGrants: []privilege{
				{Role: "app", Target: "TABLE proposals (state)", Privileges: []string{"UPDATE"}},
				{Role: "app", Target: "TABLE proposals (decided_at)", Privileges: []string{"UPDATE"}},
			},
			wantRevoked: []privilege{{Role: "app", Target: "TABLE proposals", Privileges: []string{"UPDATE"}}},
		},
		{
			name: "a table revoke after a column grant takes the column grant too",
			sql: "GRANT UPDATE (state) ON proposals TO app;\n" +
				"REVOKE UPDATE ON proposals FROM app;",
			wantRevoked: []privilege{{Role: "app", Target: "TABLE proposals", Privileges: []string{"UPDATE"}}},
		},
		{
			name: "a column revoke after a column grant",
			sql: "GRANT SELECT (a, b) ON t TO app;\n" +
				"REVOKE SELECT (b) ON t FROM app;",
			wantGrants:  []privilege{{Role: "app", Target: "TABLE t (a)", Privileges: []string{"SELECT"}}},
			wantRevoked: []privilege{{Role: "app", Target: "TABLE t (b)", Privileges: []string{"SELECT"}}},
		},
		{
			name: "a table grant after a column revoke clears the column revoke",
			sql: "REVOKE SELECT (secret) ON t FROM app;\n" +
				"GRANT SELECT ON t TO app;",
			wantGrants: []privilege{{Role: "app", Target: "TABLE t", Privileges: []string{"SELECT"}}},
		},
		{
			name:       "a quoted column is unquoted",
			sql:        `GRANT SELECT ("Label") ON t TO app;`,
			wantGrants: []privilege{{Role: "app", Target: "TABLE t (Label)", Privileges: []string{"SELECT"}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(privileges(database.Grants), qt.DeepEquals, test.wantGrants)
			c.Assert(privileges(database.RevokedGrants), qt.DeepEquals, test.wantRevoked)
		})
	}
}

// TestRead_ColumnPrivileges_FailurePath pins the column revoke refused: under a
// grant of the same privilege on the whole table it takes nothing away.
// Measured on PostgreSQL 18, has_column_privilege still answers true after
// GRANT UPDATE ON t and REVOKE UPDATE (a) ON t.
func TestRead_ColumnPrivileges_FailurePath(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte("GRANT UPDATE ON t TO app;\nREVOKE UPDATE (a) ON t FROM app;"), platform.Postgres)

	c.Assert(err, qt.ErrorMatches, `(?s).*REVOKE UPDATE \(a\) ON t FROM app follows a GRANT of UPDATE on the whole table, which covers every column, so revoking it on columns takes nothing away; grant it on the columns that keep it instead`)
	c.Assert(database.Grants, qt.IsNil)
}
