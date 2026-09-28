package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// columnOfC answers the column name of the table c.
func columnOfC(c *qt.C, database schemamodel.Database, name string) schemamodel.Field {
	c.Helper()
	var structName string
	for _, table := range database.Tables {
		if table.Name == "c" {
			structName = table.StructName
		}
	}
	var found []schemamodel.Field
	for _, field := range database.Fields {
		if field.StructName == structName && field.Name == name {
			found = append(found, field)
		}
	}
	c.Assert(found, qt.HasLen, 1, qt.Commentf("column %s", name))
	return found[0]
}

// uniqueConstraintsOf lists the UNIQUE constraints of the database as
// `name(columns)`, in the order the model holds them.
func uniqueConstraintsOf(database schemamodel.Database) []string {
	var keys []string
	for _, constraint := range database.Constraints {
		if constraint.Type != "UNIQUE" {
			continue
		}
		key := constraint.Name + "("
		for i, column := range constraint.Columns {
			if i > 0 {
				key += ","
			}
			key += column
		}
		keys = append(keys, key+")")
	}
	return keys
}

// A column's own UNIQUE is dropped by the name the server gives it: the
// column's name on MySQL and MariaDB, with `_2` and on where another key of
// the table holds it, and `<table>_<column>_key` on PostgreSQL, with `1` and
// on where a relation or a constraint of the schema holds it. Measured on
// MySQL 8.4.11, MariaDB 11.8.9 and PostgreSQL 18.6, each statement applies,
// and Atlas CE v1.3.0 reports the database it builds synced with the file.
// The column is no longer UNIQUE, and a key the table declares by name stays
// (stokaro/ptah#3860).
func TestRead_DropsAColumnKeyByItsServerName(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		sql      string
		wantKeys []string
	}{
		{name: "PostgreSQL DROP CONSTRAINT", dialect: "postgres", sql: "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c DROP CONSTRAINT c_b_key;"},
		{name: "PostgreSQL DROP CONSTRAINT IF EXISTS", dialect: "postgres", sql: "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c DROP CONSTRAINT IF EXISTS c_b_key;"},
		{
			name: "PostgreSQL, the next name where an index holds the first", dialect: "postgres",
			sql: "CREATE TABLE c (id int PRIMARY KEY, y int);\nCREATE UNIQUE INDEX c_b_key ON c (y);\n" +
				"ALTER TABLE c ADD COLUMN b int UNIQUE;\nALTER TABLE c DROP CONSTRAINT c_b_key1;",
		},
		{
			name: "PostgreSQL, beside a named key over the column", dialect: "postgres",
			sql:      "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c ADD CONSTRAINT uq UNIQUE (b);\nALTER TABLE c DROP CONSTRAINT c_b_key;",
			wantKeys: []string{"uq(b)"},
		},
		{name: "MySQL DROP INDEX", dialect: "mysql", sql: "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c DROP INDEX b;"},
		{name: "MySQL DROP KEY", dialect: "mysql", sql: "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c DROP KEY b;"},
		{name: "MySQL DROP CONSTRAINT", dialect: "mysql", sql: "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c DROP CONSTRAINT b;"},
		{
			// Both engines compare index names without case.
			name: "MySQL DROP INDEX in another case", dialect: "mysql",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c DROP INDEX B;",
		},
		{
			name: "MySQL, the next name where an index holds the first", dialect: "mysql",
			sql: "CREATE TABLE c (id int PRIMARY KEY, y int, KEY b (y));\nALTER TABLE c ADD COLUMN b int UNIQUE;\nALTER TABLE c DROP INDEX b_2;",
		},
		{
			name: "MySQL, the next name where a UNIQUE holds the first", dialect: "mysql",
			sql:      "CREATE TABLE c (id int PRIMARY KEY, y int, CONSTRAINT b UNIQUE (y));\nALTER TABLE c ADD COLUMN b int UNIQUE;\nALTER TABLE c DROP INDEX b_2;",
			wantKeys: []string{"b(y)"},
		},
		{
			name: "MySQL, beside a named key over the column", dialect: "mysql",
			sql:      "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE, CONSTRAINT uq UNIQUE (b));\nALTER TABLE c DROP INDEX b;",
			wantKeys: []string{"uq(b)"},
		},
		{name: "MariaDB DROP INDEX IF EXISTS", dialect: "mariadb", sql: "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c DROP INDEX IF EXISTS b;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(columnOfC(c, database, "b").Unique, qt.IsFalse)
			c.Assert(uniqueConstraintsOf(database), qt.DeepEquals, test.wantKeys)
		})
	}
}

// A column's own UNIQUE renamed by the name the server gives it becomes a
// UNIQUE of the new name over the column, which is what the server holds: the
// model keeps no name on the column. Measured as above, Atlas CE v1.3.0
// reports the database each statement builds synced with the file
// (stokaro/ptah#3860).
func TestRead_RenamesAColumnKeyByItsServerName(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		sql      string
		wantKeys []string
	}{
		{
			name: "PostgreSQL RENAME CONSTRAINT", dialect: "postgres",
			sql:      "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c RENAME CONSTRAINT c_b_key TO other;",
			wantKeys: []string{"other(b)"},
		},
		{
			name: "PostgreSQL RENAME CONSTRAINT to a quoted name", dialect: "postgres",
			sql:      "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c RENAME CONSTRAINT c_b_key TO \"Other\";",
			wantKeys: []string{"Other(b)"},
		},
		{
			name: "PostgreSQL, the next name where an index holds the first", dialect: "postgres",
			sql: "CREATE TABLE c (id int PRIMARY KEY, y int);\nCREATE UNIQUE INDEX c_b_key ON c (y);\n" +
				"ALTER TABLE c ADD COLUMN b int UNIQUE;\nALTER TABLE c RENAME CONSTRAINT c_b_key1 TO other;",
			wantKeys: []string{"other(b)"},
		},
		{
			name: "MySQL RENAME INDEX", dialect: "mysql",
			sql:      "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c RENAME INDEX b TO other;",
			wantKeys: []string{"other(b)"},
		},
		{
			name: "MariaDB RENAME KEY", dialect: "mariadb",
			sql:      "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c RENAME KEY b TO other;",
			wantKeys: []string{"other(b)"},
		},
		{
			name: "MySQL, the next name where an index holds the first", dialect: "mysql",
			sql:      "CREATE TABLE c (id int PRIMARY KEY, y int, KEY b (y));\nALTER TABLE c ADD COLUMN b int UNIQUE;\nALTER TABLE c RENAME INDEX b_2 TO other;",
			wantKeys: []string{"other(b)"},
		},
		{
			name: "MySQL, renamed and then dropped", dialect: "mysql",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c RENAME INDEX b TO other;\nALTER TABLE c DROP INDEX other;",
		},
		{
			name: "PostgreSQL, renamed and then dropped", dialect: "postgres",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c RENAME CONSTRAINT c_b_key TO other;\nALTER TABLE c DROP CONSTRAINT other;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(columnOfC(c, database, "b").Unique, qt.IsFalse)
			c.Assert(uniqueConstraintsOf(database), qt.DeepEquals, test.wantKeys)
		})
	}
}

// RENAME INDEX renames an index the table declares, and a UNIQUE constraint,
// which is an index on MySQL and MariaDB.
func TestRead_RenameIndex(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte("CREATE TABLE c (id int PRIMARY KEY, y int, z int, KEY k (y), CONSTRAINT uq UNIQUE (z));\n"+
		"ALTER TABLE c RENAME INDEX k TO k2;\nALTER TABLE c RENAME KEY uq TO uq2;"), "mysql")

	c.Assert(err, qt.IsNil)
	var indexes []string
	for _, index := range database.Indexes {
		indexes = append(indexes, index.Name)
	}
	c.Assert(indexes, qt.DeepEquals, []string{"k2"})
	c.Assert(uniqueConstraintsOf(database), qt.DeepEquals, []string{"uq2(z)"})
}

// A statement the server refuses is refused, and names what it could not
// find or what holds the name. Measured on MySQL 8.4.11 and PostgreSQL 18.6.
func TestRead_ColumnKeyByName_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			// ERROR 1091: Can't DROP 'b_2'.
			name: "MySQL, a name the column's key does not have", dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c DROP INDEX b_2;",
			wantErr: `ALTER TABLE c DROP CONSTRAINT b_2 names a constraint this schema does not declare by that name`,
		},
		{
			// constraint "c_b_key1" of relation "c" does not exist.
			name: "PostgreSQL, a name the column's key does not have", dialect: "postgres",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c DROP CONSTRAINT c_b_key1;",
			wantErr: `ALTER TABLE c DROP CONSTRAINT c_b_key1 names a constraint this schema does not declare by that name`,
		},
		{
			// ERROR 1091: Can't DROP 'b'.
			name: "MySQL DROP FOREIGN KEY under the key's name", dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c DROP FOREIGN KEY b;",
			wantErr: `ALTER TABLE c DROP CONSTRAINT b names a constraint this schema does not declare by that name`,
		},
		{
			// ERROR 3821: Check constraint 'b' is not found in the table.
			name: "MySQL DROP CHECK under the key's name", dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER TABLE c DROP CHECK b;",
			wantErr: `ALTER TABLE c DROP CONSTRAINT b names a constraint this schema does not declare by that name`,
		},
		{
			// ERROR 1061: Duplicate key name 'a'.
			name: "MySQL, renamed onto another column's key", dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int UNIQUE);\nALTER TABLE c RENAME INDEX b TO a;",
			wantErr: `ALTER TABLE c RENAME INDEX b TO a: the table already holds a key named a`,
		},
		{
			// ERROR 1061: Duplicate key name 'k'.
			name: "MySQL, renamed onto an index", dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE, y int, KEY k (y));\nALTER TABLE c RENAME INDEX b TO k;",
			wantErr: `ALTER TABLE c RENAME INDEX b TO k: the table already holds a key named k`,
		},
		{
			// ERROR 1176: Key 'nope' doesn't exist in table 'c'.
			name: "MySQL, a name no index has", dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, y int);\nALTER TABLE c RENAME INDEX nope TO k2;",
			wantErr: `ALTER TABLE c RENAME INDEX nope names an index this schema does not declare by that name`,
		},
		{
			// relation "c_a_key" already exists.
			name: "PostgreSQL, renamed onto another column's key", dialect: "postgres",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int UNIQUE);\nALTER TABLE c RENAME CONSTRAINT c_b_key TO c_a_key;",
			wantErr: `ALTER TABLE c RENAME CONSTRAINT c_b_key TO c_a_key: the table already holds a key named c_a_key`,
		},
		{
			// relation "other" already exists.
			name: "PostgreSQL, renamed onto a relation", dialect: "postgres",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nCREATE TABLE other (id int);\nALTER TABLE c RENAME CONSTRAINT c_b_key TO other;",
			wantErr: `ALTER TABLE c RENAME CONSTRAINT c_b_key TO other: the table already holds a key named other`,
		},
		{
			// constraint "ck" for relation "c" already exists.
			name: "PostgreSQL, renamed onto a CHECK", dialect: "postgres",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE, CONSTRAINT ck CHECK (b > 0));\nALTER TABLE c RENAME CONSTRAINT c_b_key TO ck;",
			wantErr: `ALTER TABLE c RENAME CONSTRAINT c_b_key TO ck: the table already holds a key named ck`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}
