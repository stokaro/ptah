package sqlschema_test

import (
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// indexOptionsOf lists each index as `<name> unique=<bool> comment=<text>
// invisible=<bool>`, sorted, and each UNIQUE constraint as `constraint <name>`.
func indexOptionsOf(database schemamodel.Database) []string {
	var described []string
	for _, index := range database.Indexes {
		described = append(described, fmt.Sprintf("%s unique=%t comment=%s invisible=%t",
			index.Name, index.Unique, index.Comment, index.Invisible))
	}
	for _, constraint := range database.Constraints {
		if constraint.Type == "UNIQUE" {
			described = append(described, "constraint "+constraint.Name)
		}
	}
	slices.Sort(described)
	return described
}

// TestRead_IndexOptions_HappyPath reads an index's COMMENT and whether the
// optimizer uses it (stokaro/ptah#3853). Each row's SQL was run on its server,
// MySQL 8.4.11 or MariaDB 11.8.9, which kept the comment in
// STATISTICS.INDEX_COMMENT and the visibility in IS_VISIBLE or IGNORED. A
// UNIQUE key carrying either is read as the unique index the server builds,
// because a UNIQUE constraint has no place for them.
func TestRead_IndexOptions_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		{
			name:    "MySQL keys in CREATE TABLE",
			dialect: "mysql",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, d int, KEY k_a (a) COMMENT 'lookup', " +
				"KEY k_b (b) INVISIBLE, UNIQUE KEY u_d (d) INVISIBLE COMMENT 'u', UNIQUE KEY u_a (a));",
			want: []string{
				"constraint u_a",
				"k_a unique=false comment=lookup invisible=false",
				"k_b unique=false comment= invisible=true",
				"u_d unique=true comment=u invisible=true",
			},
		},
		{
			name:    "MariaDB IGNORED",
			dialect: "mariadb",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY k_a (a) IGNORED, KEY k_b (b) NOT IGNORED);",
			want:    []string{"k_a unique=false comment= invisible=true", "k_b unique=false comment= invisible=false"},
		},
		{
			name:    "CREATE INDEX",
			dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int);\nCREATE INDEX k_a ON c (a) COMMENT 'x' INVISIBLE;",
			want:    []string{"k_a unique=false comment=x invisible=true"},
		},
		{
			name:    "ALTER TABLE ADD INDEX",
			dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int);\nALTER TABLE c ADD INDEX k_a (a) INVISIBLE;",
			want:    []string{"k_a unique=false comment= invisible=true"},
		},
		{
			name:    "ALTER INDEX hides an index",
			dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, KEY k_a (a));\nALTER TABLE c ALTER INDEX k_a INVISIBLE;",
			want:    []string{"k_a unique=false comment= invisible=true"},
		},
		{
			name:    "ALTER INDEX shows a MariaDB index",
			dialect: "mariadb",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, KEY k_a (a) IGNORED);\nALTER TABLE c ALTER INDEX k_a NOT IGNORED;",
			want:    []string{"k_a unique=false comment= invisible=false"},
		},
		{
			name:    "ALTER INDEX hides a UNIQUE key, which becomes the unique index it is",
			dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, UNIQUE KEY u_a (a));\nALTER TABLE c ALTER INDEX u_a INVISIBLE;",
			want:    []string{"u_a unique=true comment= invisible=true"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(indexOptionsOf(database), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_IndexOptions_FailurePath refuses an ALTER INDEX that names no index
// of the table, and a hidden index on a table with no such index, as the
// servers refuse them.
func TestRead_IndexOptions_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "a name the table does not hold",
			dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int);\nALTER TABLE c ALTER INDEX k_missing INVISIBLE;",
			wantErr: `.*ALTER TABLE c ALTER INDEX k_missing names an index this schema does not declare by that name`,
		},
		{
			name:    "a word that is not a visibility",
			dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, KEY k_a (a));\nALTER TABLE c ALTER INDEX k_a HIDDEN;",
			wantErr: `(?s).*expected VISIBLE, INVISIBLE, IGNORED or NOT IGNORED after ALTER INDEX k_a at position \d+.*`,
		},
		{
			name:    "MariaDB's word on MySQL",
			dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, KEY k_a (a));\nALTER TABLE c ALTER INDEX k_a IGNORED;",
			wantErr: `(?s).*IGNORED at position \d+: it is MariaDB's clause, and MySQL 8.4 answers ERROR 1064.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Indexes, qt.HasLen, 0)
		})
	}
}
