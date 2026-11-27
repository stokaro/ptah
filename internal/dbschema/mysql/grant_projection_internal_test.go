package mysql

// White-box testing required: the projection is a SQL string built inside the
// reader, and the four defects it carried were all invisible from outside --
// each produced a description that parsed, named real roles, and was wrong
// about what they hold.

import (
	"database/sql/driver"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
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
// The views are the reason all four defects close together on MySQL, so a
// change back to the grant tables there should fail here rather than in a
// replay somebody runs a month later. MariaDB reads the grant tables, one arm
// per privilege, as [TestMariaDBGrantQuery_ReadsEachPrivilegeOfTheGrantTables]
// pins.
func TestReadGrants_ReadsThePrivilegeViewsRatherThanTheGrantTables(t *testing.T) {
	tests := []struct {
		name     string
		fragment string
	}{
		{name: "schema scope comes from the view", fragment: "information_schema.SCHEMA_PRIVILEGES"},
		{name: "table scope comes from the view", fragment: "information_schema.TABLE_PRIVILEGES"},
		{name: "column scope comes from the view", fragment: "information_schema.COLUMN_PRIVILEGES"},
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

// TestMariaDBGrantQuery_ReadsEachPrivilegeOfTheGrantTables pins the MariaDB
// projection to the grant tables, which hold each grantee's own rows, where
// the privilege views also list what a role holds through another
// ([grantSource] says why). It reads them so that none of the defects the views
// closed comes back: each privilege column of mysql.db is an arm of its own,
// each member of the SET columns is looked up by name, and Grant is the grant
// option, never a privilege.
func TestMariaDBGrantQuery_ReadsEachPrivilegeOfTheGrantTables(t *testing.T) {
	tests := []struct {
		name     string
		fragment string
	}{
		{name: "a database privilege is its column", fragment: "'CREATE TEMPORARY TABLES' AS privilege"},
		{name: "the column the privilege comes from", fragment: "FROM mysql.db AS d WHERE d.Create_tmp_table_priv = 'Y'"},
		{name: "the optional column the server has", fragment: "d.Show_create_routine_priv = 'Y'"},
		{name: "the grant option on a database", fragment: "IF(d.Grant_priv = 'Y', 'YES', 'NO')"},
		{name: "a table privilege is a member of the SET", fragment: "FIND_IN_SET('Delete versioning rows', tp.Table_priv) > 0"},
		{name: "the name a table privilege is read by", fragment: "'DELETE HISTORY'"},
		{name: "a column privilege is a member of the SET", fragment: "FIND_IN_SET('References', cp.Column_priv) > 0"},
		{name: "a column takes the grant option of its table", fragment: "FIND_IN_SET('Grant', COALESCE(tg.Table_priv, ''))"},
		{name: "a database row holding the grant option alone", fragment: "'USAGE', 'YES', 'SCHEMA'"},
		{name: "the grant option and no privilege", fragment: "FROM mysql.db AS d WHERE d.Grant_priv = 'Y' AND d.Select_priv <> 'Y' AND d.Insert_priv <> 'Y'"},
		{name: "a table row holding the grant option alone", fragment: "tp.Table_priv = 'Grant' AND tp.Column_priv = ''"},
		{name: "the grantee is the account", fragment: "u.user = granted.grantee_user AND u.host = granted.grantee_host"},
		{name: "the database compares as the views compare it", fragment: "CONVERT(d.Db USING utf8mb3) COLLATE utf8mb3_general_ci"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mariaDBGrantQuery("u.is_role = 'Y'", mariaDBDatabasePrivileges), qt.Contains, test.fragment)
		})
	}
}

func TestMariaDBGrantQuery_ReadsNoPrivilegeView(t *testing.T) {
	tests := []struct {
		name    string
		absent  string
		because string
	}{
		{name: "SCHEMA_PRIVILEGES", absent: "SCHEMA_PRIVILEGES", because: "it lists a role's inherited privileges as its own"},
		{name: "TABLE_PRIVILEGES", absent: "TABLE_PRIVILEGES", because: "it lists a role's inherited privileges as its own"},
		{name: "COLUMN_PRIVILEGES", absent: "COLUMN_PRIVILEGES", because: "it lists a role's inherited privileges as its own"},
		{name: "the grant option as a privilege", absent: "'GRANT'", because: "Grant is the grant option, not a privilege"},
		{name: "a column the server lacks", absent: "Show_create_routine_priv", because: "MariaDB 10.11 has no such column"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			columns := slices.DeleteFunc(slices.Clone(mariaDBDatabasePrivileges), func(held globalTablePrivilege) bool { return held.optional })
			c.Assert(mariaDBGrantQuery("u.is_role = 'Y'", columns), qt.Not(qt.Contains), test.absent, qt.Commentf("%s", test.because))
		})
	}
}

// TestGrantSourceOf reads the grants from the privilege views on MySQL and
// from the grant tables on MariaDB, naming an optional column of mysql.db
// only where the server has it.
func TestGrantSourceOf(t *testing.T) {
	tests := []struct {
		name        string
		mariaDB     bool
		columns     []string
		wantTables  bool
		wantColumns int
	}{
		{name: "MySQL", mariaDB: false, columns: []string{"show_create_routine_priv"}},
		{name: "MariaDB 11.8", mariaDB: true, columns: []string{"Select_priv", "Show_create_routine_priv"}, wantTables: true, wantColumns: 20},
		{name: "MariaDB 10.11", mariaDB: true, columns: []string{"Select_priv"}, wantTables: true, wantColumns: 19},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rows := make([][]driver.Value, 0, len(test.columns))
			for _, column := range test.columns {
				rows = append(rows, []driver.Value{column})
			}
			db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
				return dbtest.QueryResult{Columns: []string{"column_name"}, Rows: rows}, nil
			})

			source, err := NewMySQLReader(db.SQL, "app").grantSourceOf(t.Context(), sessionAccount{mariaDB: test.mariaDB})

			c.Assert(err, qt.IsNil)
			c.Assert(source.tables, qt.Equals, test.wantTables)
			c.Assert(source.databaseColumns, qt.HasLen, test.wantColumns)
		})
	}
}
