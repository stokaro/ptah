package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// fkParent is the table the foreign keys of these tests reference.
const fkParent = "CREATE TABLE p (id int PRIMARY KEY);\n"

// MySQL and MariaDB keep an index for every foreign key, and a drop that leaves
// another index beginning with the key's columns is allowed: the server checks
// the key against that one. Measured on MySQL 8.4.11 and MariaDB 11.8.9, each
// file applies (stokaro/ptah#3913).
func TestRead_DropIndexAForeignKeyCanDoWithout_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		sql         string
		wantIndexes []string
	}{
		{
			name: "another index begins with the key's columns", dialect: "mysql",
			sql:         fkParent + "CREATE TABLE c (id int PRIMARY KEY, x int, KEY ix (x), KEY ix2 (x, id), CONSTRAINT fk FOREIGN KEY (x) REFERENCES p (id));\nDROP INDEX ix ON c;",
			wantIndexes: []string{"ix2"},
		},
		{
			name: "the primary key begins with the key's columns", dialect: "mariadb",
			sql: fkParent + "CREATE TABLE c (x int, y int, PRIMARY KEY (x, y), KEY ix (x), CONSTRAINT fk FOREIGN KEY (x) REFERENCES p (id));\nALTER TABLE c DROP INDEX ix;",
		},
		{
			name: "the column's own key", dialect: "mysql",
			sql: fkParent + "CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE, KEY ix (x), CONSTRAINT fk FOREIGN KEY (x) REFERENCES p (id));\nDROP INDEX ix ON c;",
		},
		{
			name: "an index of the table no key needs", dialect: "mysql",
			sql: fkParent + "CREATE TABLE c (id int PRIMARY KEY, x int, y int, KEY iy (y), CONSTRAINT fk FOREIGN KEY (x) REFERENCES p (id));\nDROP INDEX iy ON c;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(indexNamesOf(database), qt.DeepEquals, test.wantIndexes)
		})
	}
}

// A drop that takes away the last index a foreign key can be checked against
// is refused, as MySQL and MariaDB refuse it: measured on MySQL 8.4.11 and
// MariaDB 11.8.9, each answers `ERROR 1553: Cannot drop index 'ix': needed in
// a foreign key constraint` (stokaro/ptah#3913). Read as a drop, the plan would
// drop an index the server refuses to drop.
func TestRead_DropIndexAForeignKeyNeeds_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name: "DROP INDEX ... ON", dialect: "mysql",
			sql:     fkParent + "CREATE TABLE c (id int PRIMARY KEY, x int, KEY ix (x), CONSTRAINT fk FOREIGN KEY (x) REFERENCES p (id));\nDROP INDEX ix ON c;",
			wantErr: `.*ALTER TABLE c DROP INDEX ix: foreign key fk needs an index that begins with \(x\), and no other index of the table does; MySQL and MariaDB refuse the drop \(ERROR 1553: needed in a foreign key constraint\)`,
		},
		{
			name: "ALTER TABLE ... DROP INDEX", dialect: "mariadb",
			sql:     fkParent + "CREATE TABLE c (id int PRIMARY KEY, x int, KEY ix (x), CONSTRAINT fk FOREIGN KEY (x) REFERENCES p (id));\nALTER TABLE c DROP INDEX ix;",
			wantErr: `.*ALTER TABLE c DROP INDEX ix: foreign key fk needs an index .*ERROR 1553.*`,
		},
		{
			// MySQL refuses a column's REFERENCES before this; MariaDB builds
			// the key and names it c_ibfk_1.
			name: "a column's REFERENCES", dialect: "mariadb",
			sql:     fkParent + "CREATE TABLE c (id int PRIMARY KEY, x int REFERENCES p (id), KEY ix (x));\nDROP INDEX ix ON c;",
			wantErr: `.*foreign key c_ibfk_1 needs an index that begins with \(x\).*ERROR 1553.*`,
		},
		{
			name: "a UNIQUE the key is checked against", dialect: "mysql",
			sql:     fkParent + "CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT uq UNIQUE (x), CONSTRAINT fk FOREIGN KEY (x) REFERENCES p (id));\nDROP INDEX uq ON c;",
			wantErr: `.*ALTER TABLE c DROP INDEX uq: foreign key fk needs an index .*ERROR 1553.*`,
		},
		{
			name: "a composite key and an index over its first column only", dialect: "mysql",
			sql:     fkParent + "CREATE TABLE q (a int, b int, PRIMARY KEY (a, b));\nCREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY iab (a, b), KEY ia (a), CONSTRAINT fk FOREIGN KEY (a, b) REFERENCES q (a, b));\nDROP INDEX iab ON c;",
			wantErr: `.*foreign key fk needs an index that begins with \(a, b\).*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
