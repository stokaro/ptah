package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// A MODIFY rewrites a column's definition and leaves its keys alone, and
// UNIQUE in it asks for a key of its own. Measured on MySQL 8.4.11 and 26.7.0
// and MariaDB 11.8.9 and 12.3.3: `x int UNIQUE` keeps the key `x` through
// `MODIFY x bigint`, and `MODIFY x bigint UNIQUE` adds `x_2` beside it, under
// the next name no key of the table holds. Atlas CE v1.3.0 reports each
// database synced with the file that built it (stokaro/ptah#3875). The
// column's own key is the column's UNIQUE, and each key the MODIFY adds is a
// UNIQUE constraint under the server's name.
func TestRead_ModifyColumn_KeepsTheColumnKey(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		sql        string
		wantUnique bool
		wantKeys   []string
	}{
		{
			name: "MySQL, a MODIFY that omits UNIQUE", dialect: "mysql",
			sql:        "CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);\nALTER TABLE c MODIFY COLUMN x bigint;",
			wantUnique: true,
		},
		{
			name: "MariaDB, a MODIFY that omits UNIQUE", dialect: "mariadb",
			sql:        "CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);\nALTER TABLE c MODIFY COLUMN x bigint;",
			wantUnique: true,
		},
		{
			name: "MySQL, a MODIFY that restates UNIQUE", dialect: "mysql",
			sql:        "CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);\nALTER TABLE c MODIFY COLUMN x bigint UNIQUE;",
			wantUnique: true, wantKeys: []string{"x_2(x)"},
		},
		{
			name: "MariaDB, a MODIFY that restates UNIQUE", dialect: "mariadb",
			sql:        "CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);\nALTER TABLE c MODIFY COLUMN x bigint UNIQUE;",
			wantUnique: true, wantKeys: []string{"x_2(x)"},
		},
		{
			name: "MySQL, the next name where another column's key holds x_2", dialect: "mysql",
			sql:        "CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE, x_2 int UNIQUE);\nALTER TABLE c MODIFY COLUMN x bigint UNIQUE;",
			wantUnique: true, wantKeys: []string{"x_3(x)"},
		},
		{
			name: "MySQL, UNIQUE restated twice", dialect: "mysql",
			sql: "CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);\nALTER TABLE c MODIFY COLUMN x bigint UNIQUE;\n" +
				"ALTER TABLE c MODIFY COLUMN x bigint UNIQUE;",
			wantUnique: true, wantKeys: []string{"x_2(x)", "x_3(x)"},
		},
		{
			name: "MySQL, UNIQUE on a column without a key", dialect: "mysql",
			sql:        "CREATE TABLE c (id bigint PRIMARY KEY, x int);\nALTER TABLE c MODIFY COLUMN x bigint UNIQUE;",
			wantUnique: true,
		},
		{
			name: "MySQL, a named key over the column stays named", dialect: "mysql",
			sql:      "CREATE TABLE c (id bigint PRIMARY KEY, x int, UNIQUE KEY ux (x));\nALTER TABLE c MODIFY COLUMN x bigint;",
			wantKeys: []string{"ux(x)"},
		},
		{
			name: "MySQL, UNIQUE beside a named key over the column", dialect: "mysql",
			sql:        "CREATE TABLE c (id bigint PRIMARY KEY, x int, UNIQUE KEY ux (x));\nALTER TABLE c MODIFY COLUMN x bigint UNIQUE;",
			wantUnique: true, wantKeys: []string{"ux(x)"},
		},
		{
			name: "MySQL, the added key dropped by its name", dialect: "mysql",
			sql: "CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);\nALTER TABLE c MODIFY COLUMN x bigint UNIQUE;\n" +
				"ALTER TABLE c DROP INDEX x_2;",
			wantUnique: true,
		},
		{
			name: "MySQL, the column's own key dropped beside the added one", dialect: "mysql",
			sql: "CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);\nALTER TABLE c MODIFY COLUMN x bigint UNIQUE;\n" +
				"ALTER TABLE c DROP INDEX x;",
			wantKeys: []string{"x_2(x)"},
		},
		{
			name: "MySQL, the kept key dropped after the MODIFY", dialect: "mysql",
			sql: "CREATE TABLE c (id bigint PRIMARY KEY, x int UNIQUE);\nALTER TABLE c MODIFY COLUMN x bigint;\n" +
				"ALTER TABLE c DROP INDEX x;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			field := columnOfC(c, database, "x")
			c.Assert(field.Type, qt.Equals, "bigint")
			c.Assert(field.Unique, qt.Equals, test.wantUnique)
			c.Assert(uniqueConstraintsOf(database), qt.DeepEquals, test.wantKeys)
		})
	}
}
