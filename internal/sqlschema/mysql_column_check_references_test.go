package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlcheck"
	"ptah.run/internal/sqlschema"
)

// TestRead_MySQLColumnCheckNamingAnotherColumn_FailurePath refuses a CHECK
// written on a column that names another column when the document is read for
// MySQL. Each row is refused by MySQL 8.4.11 with `ERROR 3813 (HY000): Column
// check constraint ... references other column.` (stokaro/ptah#3791).
func TestRead_MySQLColumnCheckNamingAnotherColumn_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "a column declared before it",
			sql:     "CREATE TABLE c (a int, b int CHECK (b > a));",
			wantErr: `(?s)column CHECK at position \d+: .*the CHECK on column b names column a, and MySQL answers ERROR 3813.*`,
		},
		{
			name:    "a column declared after it",
			sql:     "CREATE TABLE c (a int CHECK (a < b), b int);",
			wantErr: `(?s)column CHECK at position \d+: .*the CHECK on column a names column b, and MySQL answers ERROR 3813.*`,
		},
		{
			name:    "a second CHECK on the column",
			sql:     "CREATE TABLE p1 (a int CHECK (a > 0) CHECK (b > 0), b int);",
			wantErr: `(?s)column CHECK at position \d+: .*the CHECK on column a names column b.*`,
		},
		{
			name:    "a named CHECK",
			sql:     "CREATE TABLE p9 (a int, b int CONSTRAINT p9_named CHECK (b > a));",
			wantErr: `(?s)column CHECK at position \d+: .*the CHECK on column b names column a.*`,
		},
		{
			name:    "another column qualified by its table",
			sql:     "CREATE TABLE p3 (a int, b int CHECK (p3.a > 0));",
			wantErr: `(?s)column CHECK at position \d+: .*the CHECK on column b names column a.*`,
		},
		{
			name:    "ALTER TABLE adds the column",
			sql:     "CREATE TABLE q (a int, b int);\nALTER TABLE q ADD COLUMN c int CHECK (c > a);",
			wantErr: `(?s).*ALTER TABLE q: .*the CHECK on column c names column a.*`,
		},
		{
			name:    "ALTER TABLE modifies the column",
			sql:     "CREATE TABLE q (a int, b int);\nALTER TABLE q MODIFY COLUMN b int CHECK (b > a);",
			wantErr: `(?s).*ALTER TABLE q: .*the CHECK on column b names column a.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "mysql")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

// TestRead_MySQLColumnCheckNamingAnotherColumn_ErrorClass pins the sentinel a
// caller can test for, on both paths that refuse the CHECK.
func TestRead_MySQLColumnCheckNamingAnotherColumn_ErrorClass(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE c (a int, b int CHECK (b > a));",
		"CREATE TABLE q (a int, b int);\nALTER TABLE q ADD COLUMN c int CHECK (c > a);",
	} {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)

			_, _, err := sqlschema.Read([]byte(sql), "mysql")

			c.Assert(err, qt.ErrorIs, mysqlcheck.ErrNamesOtherColumn)
		})
	}
}

// TestRead_MySQLColumnCheckNamingAnotherColumn_HappyPath reads the CHECKs
// MySQL 8.4.11 accepts on a column, and the same CHECK read for MariaDB, which
// accepts it (measured on 11.8.9).
func TestRead_MySQLColumnCheckNamingAnotherColumn_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{name: "its own column", dialect: "mysql", sql: "CREATE TABLE c2 (a int, b int CHECK (b > 0 AND c2.b < 9));"},
		{name: "a string spelling another column", dialect: "mysql", sql: "CREATE TABLE p4 (a int, b varchar(10) CHECK (b <> 'a'));"},
		{name: "another column at table level", dialect: "mysql", sql: "CREATE TABLE p10 (a int, b int, CHECK (b > a));"},
		{name: "another column read for MariaDB", dialect: "mariadb", sql: "CREATE TABLE e1 (a int, b int CHECK (b > a));"},
		{name: "ALTER TABLE adds a column naming its own", dialect: "mysql", sql: "CREATE TABLE q (a int);\nALTER TABLE q ADD COLUMN d int CHECK (d > 0);"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
		})
	}
}
