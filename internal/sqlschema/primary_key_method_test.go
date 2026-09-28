package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// TestRead_PrimaryKeyMethod_HappyPath reads the access method a MySQL-family
// primary key asks for, before its parts or after them, on CREATE TABLE and
// ALTER TABLE. MariaDB 11.8.9 keeps USING HASH and prints it back; BTREE is
// the default and is carried as none (stokaro/ptah#3853).
func TestRead_PrimaryKeyMethod_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    string
	}{
		{name: "after the parts", dialect: "mariadb", sql: "CREATE TABLE t (id int, PRIMARY KEY (id) USING HASH);", want: "HASH"},
		{name: "before the parts", dialect: "mariadb", sql: "CREATE TABLE t (id int, PRIMARY KEY USING HASH (id));", want: "HASH"},
		{name: "a named key", dialect: "mysql", sql: "CREATE TABLE t (id int, CONSTRAINT pk PRIMARY KEY (id) USING HASH);", want: "HASH"},
		{name: "BTREE is the default", dialect: "mariadb", sql: "CREATE TABLE t (id int, PRIMARY KEY (id) USING BTREE);"},
		{name: "ALTER TABLE adds it", dialect: "mariadb",
			sql: "CREATE TABLE t (id int NOT NULL);\nALTER TABLE t ADD PRIMARY KEY (id) USING HASH;", want: "HASH"},
		{name: "DROP PRIMARY KEY takes it", dialect: "mariadb",
			sql: "CREATE TABLE t (id int, a int, PRIMARY KEY (id) USING HASH);\nALTER TABLE t DROP PRIMARY KEY;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
			c.Assert(database.Tables[0].PrimaryKeyMethod, qt.Equals, test.want)
		})
	}
}
