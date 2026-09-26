package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// tenKeys is a CREATE TABLE of table with ten unnamed foreign keys, so the
// tenth derived name is the table's length plus eight characters.
func tenKeys(table string) string {
	var columns, keys strings.Builder
	for _, n := range []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"} {
		columns.WriteString(", x" + n + " int")
		keys.WriteString(", FOREIGN KEY (x" + n + ") REFERENCES p(id)")
	}
	return "CREATE TABLE " + table + " (id int PRIMARY KEY" + columns.String() + keys.String() + ");"
}

// addedKey is an ALTER TABLE that adds one unnamed foreign key to a new table,
// so the derived name is the table's length plus seven characters.
func addedKey(table string) string {
	return "CREATE TABLE " + table + " (id int PRIMARY KEY, a int);\n" +
		"ALTER TABLE " + table + " ADD FOREIGN KEY (a) REFERENCES p(id);"
}

// TestRead_MySQLFamilyForeignKeyNameLength_HappyPath keeps the longest name
// each server keeps as derived. Every row was run on MySQL 8.4.11 and 26.7.0
// and MariaDB 11.8.9, and the catalog holds the name the row expects.
func TestRead_MySQLFamilyForeignKeyNameLength_HappyPath(t *testing.T) {
	t56, t55, t57 := strings.Repeat("t", 56), strings.Repeat("t", 55), strings.Repeat("t", 57)
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    string
	}{
		{
			name:    "MySQL, 64 characters in CREATE TABLE",
			dialect: platform.MySQL,
			sql:     tenKeys(t56),
			want:    t56 + " " + t56 + "_ibfk_10(x10)",
		},
		{
			name:    "MariaDB, 63 characters in CREATE TABLE",
			dialect: platform.MariaDB,
			sql:     tenKeys(t55),
			want:    t55 + " " + t55 + "_ibfk_10(x10)",
		},
		{
			name:    "MySQL, 64 characters in ALTER TABLE",
			dialect: platform.MySQL,
			sql:     addedKey(t57),
			want:    t57 + " " + t57 + "_ibfk_1(a)",
		},
		{
			name:    "MariaDB, 64 characters in ALTER TABLE",
			dialect: platform.MariaDB,
			sql:     addedKey(t57),
			want:    t57 + " " + t57 + "_ibfk_1(a)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(foreignKeysOf(database), qt.Contains, test.want)
		})
	}
}

// TestRead_MySQLFamilyForeignKeyNameLength_FailurePath refuses a document
// whose unnamed key would take a name the server does not keep as derived
// (stokaro/ptah#3762). Measured over a utf8mb4 connection: MySQL 8.4.11 and
// 26.7.0 answer a 65-character name with `ERROR 1059 (42000): Identifier name
// ... is too long`; MariaDB 11.8.9 answers a 64-character name in CREATE TABLE
// the same way, and in ALTER TABLE cuts a 65-character name to 64. MariaDB
// 12.1 and later name each key by its number and keep every row.
func TestRead_MySQLFamilyForeignKeyNameLength_FailurePath(t *testing.T) {
	t56, t57, t58 := strings.Repeat("t", 56), strings.Repeat("t", 57), strings.Repeat("t", 58)
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{
			name:    "MySQL, 65 characters in CREATE TABLE",
			dialect: platform.MySQL,
			sql:     tenKeys(t57),
		},
		{
			name:    "MariaDB, 64 characters in CREATE TABLE",
			dialect: platform.MariaDB,
			sql:     tenKeys(t56),
		},
		{
			name:    "MariaDB, 64 characters from a column's REFERENCES",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE " + t57 + " (id int PRIMARY KEY, a int REFERENCES p(id));",
		},
		{
			name:    "MySQL, 65 characters in ALTER TABLE",
			dialect: platform.MySQL,
			sql:     addedKey(t58),
		},
		{
			name:    "MariaDB, 65 characters in ALTER TABLE",
			dialect: platform.MariaDB,
			sql:     addedKey(t58),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.sql), test.dialect)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrForeignKeyNameTooLong)
			c.Assert(database.Constraints, qt.HasLen, 0)
		})
	}
}

// TestRead_MySQLFamilyForeignKeyNameLength_Message says which name, which
// statement and which limit, so the author can see what to name.
func TestRead_MySQLFamilyForeignKeyNameLength_Message(t *testing.T) {
	c := qt.New(t)
	t56 := strings.Repeat("t", 56)

	_, _, err := sqlschema.Read([]byte(mysqlNamingParents+tenKeys(t56)), platform.MariaDB)

	c.Assert(err, qt.ErrorMatches, `.*: `+t56+`_ibfk_10, the name mariadb gives an unnamed foreign key of `+t56+
		` in CREATE TABLE, is 64 characters, and the server keeps at most 63 there as derived; name the key`)
}
