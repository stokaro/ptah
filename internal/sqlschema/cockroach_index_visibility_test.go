package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// cockroachVisibilityTable is the table each row indexes. visible is a column
// name as well as a word of the clause, so a condition can use it.
const cockroachVisibilityTable = "CREATE TABLE t (id int PRIMARY KEY, a int, visible bool);\n"

// TestRead_CockroachDBIndexVisibility_HappyPath reads the visibility clause
// CockroachDB takes last on an index, and keeps it out of the condition it
// follows. Each row is how CockroachDB v26.3.2 read the same statement.
func TestRead_CockroachDBIndexVisibility_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantInvisible bool
		wantCondition string
	}{
		{name: "NOT VISIBLE", sql: "CREATE INDEX k ON t (a) NOT VISIBLE;", wantInvisible: true},
		{name: "MySQL's INVISIBLE", sql: "CREATE INDEX k ON t (a) INVISIBLE;", wantInvisible: true},
		{name: "VISIBLE", sql: "CREATE INDEX k ON t (a) VISIBLE;"},
		{name: "VISIBILITY 0.0", sql: "CREATE INDEX k ON t (a) VISIBILITY 0.0;", wantInvisible: true},
		{
			name: "after a condition", sql: "CREATE INDEX k ON t (a) WHERE a > 0 NOT VISIBLE;",
			wantInvisible: true, wantCondition: "a > 0",
		},
		{
			name: "after a parenthesized condition on a column named visible",
			sql:  "CREATE INDEX k ON t (a) WHERE (visible) NOT VISIBLE;", wantInvisible: true, wantCondition: "(visible)",
		},
		{
			name: "VISIBILITY 1.0 after a condition", sql: "CREATE INDEX k ON t (a) WHERE a > 0 VISIBILITY 1.0;",
			wantCondition: "a > 0",
		},
		{
			name: "a condition that negates a column named visible",
			sql:  "CREATE INDEX k ON t (a) WHERE NOT visible;", wantCondition: "NOT visible",
		},
		{
			name: "a condition that ends in a column named visible",
			sql:  "CREATE INDEX k ON t (a) WHERE a > 0 AND visible;", wantCondition: "a > 0 AND visible",
		},
		{
			name: "an index of CREATE TABLE",
			sql:  "DROP TABLE t; CREATE TABLE t (id int PRIMARY KEY, a int, INDEX k (a) NOT VISIBLE);", wantInvisible: true,
		},
		{
			name: "ALTER INDEX hides it", sql: "CREATE INDEX k ON t (a); ALTER INDEX t@k NOT VISIBLE;",
			wantInvisible: true,
		},
		{name: "ALTER INDEX shows it", sql: "CREATE INDEX k ON t (a) NOT VISIBLE; ALTER INDEX t@k VISIBLE;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(cockroachVisibilityTable+test.sql), "cockroachdb")

			c.Assert(err, qt.IsNil)
			c.Assert(database.Indexes, qt.HasLen, 1)
			c.Assert(database.Indexes[0].Invisible, qt.Equals, test.wantInvisible)
			c.Assert(database.Indexes[0].Condition, qt.Equals, test.wantCondition)
		})
	}
}

// TestRead_CockroachDBIndexVisibility_FailurePath refuses what CockroachDB
// refuses, and what the model cannot hold.
func TestRead_CockroachDBIndexVisibility_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "the clause before WHERE, a syntax error on v26.3.2",
			sql:     "CREATE INDEX k ON t (a) NOT VISIBLE WHERE a > 0;",
			wantErr: `unsupported SQL statement: WHERE at position \d+`,
		},
		{
			name:    "a partially visible index",
			sql:     "CREATE INDEX k ON t (a) VISIBILITY 0.5;",
			wantErr: `VISIBILITY 0\.5 at position \d+: a partially visible index is not modeled; write VISIBLE or NOT VISIBLE`,
		},
		{
			name:    "a partially visible index after a condition",
			sql:     "CREATE INDEX k ON t (a) WHERE a > 0 VISIBILITY 0.25;",
			wantErr: `VISIBILITY 0\.25 at position \d+: a partially visible index is not modeled; .*`,
		},
		{
			name:    "an index named without its table",
			sql:     "CREATE INDEX k ON t (a); ALTER INDEX k NOT VISIBLE;",
			wantErr: `ALTER INDEX k at position \d+: name the index through its table, as table@index`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(cockroachVisibilityTable+test.sql), "cockroachdb")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Indexes, qt.HasLen, 0)
		})
	}
}

// TestRead_PostgreSQLIndexVisibility_FailurePath keeps PostgreSQL's grammar:
// it has no visibility clause, so the words after a condition belong to it,
// and ALTER INDEX reads a rename only.
func TestRead_PostgreSQLIndexVisibility_FailurePath(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read(
		[]byte(cockroachVisibilityTable+"CREATE INDEX k ON t (a); ALTER INDEX k NOT VISIBLE;"), "postgres")

	c.Assert(err, qt.ErrorMatches, `.*ALTER INDEX k NOT at position \d+: only RENAME TO is read; .*`)
	c.Assert(database.Indexes, qt.HasLen, 0)
}
