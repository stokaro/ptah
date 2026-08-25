package mysql

// White-box testing required: the projection is a SQL string built inside the
// reader, and the four defects it carried were all invisible from outside --
// each produced a description that parsed, named real roles, and was wrong
// about what they hold.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestReadGrants_ReadsThePrivilegeViewsRatherThanTheGrantTables pins where the
// answer comes from.
//
// mysql.db has a column per privilege, so a query naming Select_priv reports
// SELECT and nothing else: every other schema-scope grant is invisible.
// mysql.tables_priv keeps its privileges in one SET column whose members
// include `Grant`, so splitting it on commas produces a privilege called GRANT.
// Neither table separates the grant option from the privilege.
//
// Measured on MySQL 9.7.2, one source with three grants
// (stokaro/ptah#2204):
//
//	mysql.tables_priv   r_upd  mysrc  t  Update,Grant
//	TABLE_PRIVILEGES    'r_upd'@'%'  mysrc  t  UPDATE  IS_GRANTABLE=YES
//
// The views are the reason all four defects close together, so a later change
// back to the grant tables should fail here rather than in a replay somebody
// runs a month later.
func TestReadGrants_ReadsThePrivilegeViewsRatherThanTheGrantTables(t *testing.T) {
	tests := []struct {
		name     string
		fragment string
	}{
		{name: "schema scope comes from the view", fragment: "information_schema.SCHEMA_PRIVILEGES"},
		{name: "table scope comes from the view", fragment: "information_schema.TABLE_PRIVILEGES"},
		{name: "the privilege is the view's own column", fragment: "PRIVILEGE_TYPE"},
		{name: "the grant option is read", fragment: "IS_GRANTABLE"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(grantQuery("u.is_role = 'Y'"), qt.Contains, test.fragment)
		})
	}
}

func TestReadGrants_NeitherGrantTableIsConsultedAnyMore(t *testing.T) {
	// The other direction, because a query can name a view and still fall back
	// to the tables that were wrong.
	tests := []struct {
		name    string
		absent  string
		because string
	}{
		{
			name:    "mysql.db",
			absent:  "mysql.db",
			because: "a column per privilege, so a predicate on one hides the rest",
		},
		{
			name:    "mysql.tables_priv",
			absent:  "mysql.tables_priv",
			because: "one SET column whose members include Grant",
		},
		{
			name:    "the hard-coded privilege",
			absent:  "'SELECT' AS privilege",
			because: "it made every schema grant SELECT whatever it was",
		},
		{
			name:    "the joined object name",
			absent:  "CONCAT(tp.db",
			because: "a pre-joined name is quoted as one identifier the server refuses",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(grantQuery("u.is_role = 'Y'"), qt.Not(qt.Contains), test.absent,
				qt.Commentf("%s", test.because))
		})
	}
}
