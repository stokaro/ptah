package mysqlsystemdb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlsystemdb"
)

// TestIsMySQLSystemDatabase names the databases a MySQL-family server or a
// clustering layer beside it owns, in any case, and no user database.
func TestIsMySQLSystemDatabase(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{name: "mysql", want: true},
		{name: "information_schema", want: true},
		{name: "performance_schema", want: true},
		{name: "sys", want: true},
		{name: "SYS", want: true},
		{name: "mysql_innodb_cluster_metadata", want: true},
		{name: "ndbinfo", want: true},
		{name: "app", want: false},
		{name: "sysadmin", want: false},
		{name: "mysql2", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlsystemdb.IsMySQLSystemDatabase(test.name), qt.Equals, test.want)
		})
	}
}

// TestMySQLUserDatabasesPredicate keeps the user databases over the column it
// is given, folding case the way [mysqlsystemdb.IsMySQLSystemDatabase] does.
func TestMySQLUserDatabasesPredicate(t *testing.T) {
	c := qt.New(t)

	got := mysqlsystemdb.MySQLUserDatabasesPredicate("SCHEMA_NAME")

	c.Assert(got, qt.Matches, `LOWER\(SCHEMA_NAME\) NOT IN \('information_schema', .*'mysql', .*'performance_schema', 'sys'\)`)
}
