//go:build integration

package integration_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A desired database read can remove DENY rows. Every catalog grantor must
// be cleared, including a column denial beneath its own grantor's table
// grant. Both the catalog rows and actual access are checked after applying
// the public database-to-database schema operation.
func TestSchemaApplySQLServerRevokesEveryDenialGrantorE2E(t *testing.T) {
	common := []string{
		"CREATE USER cp_grantor WITHOUT LOGIN",
		"GRANT CONTROL ON SCHEMA::dbo TO cp_grantor",
	}
	for _, test := range []struct {
		name    string
		held    []string
		desired []string
		want    []string
	}{
		{
			name: "object denials from two grantors",
			held: []string{"DENY SELECT ON dbo.t TO cp_reader", "DENY SELECT ON dbo.t TO cp_reader AS cp_grantor"},
		},
		{
			name: "schema denials from two grantors",
			held: []string{"DENY SELECT ON SCHEMA::dbo TO cp_reader", "DENY SELECT ON SCHEMA::dbo TO cp_reader AS cp_grantor"},
		},
		{
			name: "column denials from two grantors",
			held: []string{"DENY SELECT (id) ON dbo.t TO cp_reader", "DENY SELECT (id) ON dbo.t TO cp_reader AS cp_grantor"},
		},
		{
			name:    "column denial under the same grantor's table grant",
			held:    []string{"GRANT SELECT ON dbo.t TO cp_reader AS cp_grantor", "DENY SELECT (id) ON dbo.t TO cp_reader AS cp_grantor"},
			desired: []string{"GRANT SELECT ON dbo.t TO cp_reader AS cp_grantor"},
			want:    []string{"G SELECT dbo.t - cp_grantor"},
		},
		{
			name:    "column denial under a different grantor's table grant",
			held:    []string{"GRANT SELECT ON dbo.t TO cp_reader", "DENY SELECT (id) ON dbo.t TO cp_reader AS cp_grantor"},
			desired: []string{"GRANT SELECT ON dbo.t TO cp_reader"},
			want:    []string{"G SELECT dbo.t - dbo"},
		},
		{
			name: "column denials under both grantors' table grants",
			held: []string{
				"GRANT SELECT ON dbo.t TO cp_reader", "GRANT SELECT ON dbo.t TO cp_reader AS cp_grantor",
				"DENY SELECT (id) ON dbo.t TO cp_reader", "DENY SELECT (id) ON dbo.t TO cp_reader AS cp_grantor",
			},
			desired: []string{"GRANT SELECT ON dbo.t TO cp_reader", "GRANT SELECT ON dbo.t TO cp_reader AS cp_grantor"},
			want:    []string{"G SELECT dbo.t - cp_grantor", "G SELECT dbo.t - dbo"},
		},
		{
			name: "column denial under its grantor's grantable table grant",
			held: []string{
				"GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION AS cp_grantor",
				"DENY SELECT (id) ON dbo.t TO cp_reader CASCADE AS cp_grantor",
			},
			desired: []string{"GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION AS cp_grantor"},
			want:    []string{"W SELECT dbo.t - cp_grantor"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := sqlServerColumnDatabase(c, slices.Concat(common, test.held)...)
			desired, _ := sqlServerColumnDatabase(c, slices.Concat(common, test.desired)...)
			want := append([]string{}, test.want...)

			applied, err := runCompatVerb("schema", "apply", "--url", target, "--to", desired, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(applied, qt.Contains, " AS [cp_grantor]")
			c.Assert(sqlServerRowsOf(c, db, "cp_reader"), qt.DeepEquals, want)
			settled, err := runCompatVerb("schema", "diff", "--from", target, "--to", desired)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", settled))
			c.Assert(changeLines(settled), qt.HasLen, 0)
			// Give the role read access after checking the exact result of the
			// migration. A leftover DENY or column REVOKE row would still block
			// this query even with a schema-level SELECT grant.
			declaredSQLServerExec(c, db, "GRANT SELECT ON SCHEMA::dbo TO cp_reader")
			c.Assert(sqlServerAsMember(c, db, "SELECT id, name, secret FROM dbo.t"), qt.IsNil)
		})
	}
}
