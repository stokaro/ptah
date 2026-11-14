package sourcestatement_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sourcestatement"
)

// The body's own text is quoted where it is the statement that was sent, and
// the statement as sent everywhere else.
func TestWritten(t *testing.T) {
	for _, test := range []struct {
		name, dialect, body string
		index               int
		sent, want          string
	}{
		{
			name:    "the terminator the migrator set aside",
			dialect: "sqlite",
			body:    "CREATE TABLE users (id int);\n\nALTER TABLE users RENAME COLUMN nope TO x;\n",
			index:   2,
			sent:    "ALTER TABLE users RENAME COLUMN nope TO x",
			want:    "ALTER TABLE users RENAME COLUMN nope TO x;",
		},
		{
			name:    "a terminator on a line of its own stays where the file put it",
			dialect: "sqlite",
			body:    "ALTER TABLE users\n  RENAME COLUMN nope TO x\n;\n",
			index:   1,
			sent:    "ALTER TABLE users\n  RENAME COLUMN nope TO x",
			want:    "ALTER TABLE users\n  RENAME COLUMN nope TO x\n;",
		},
		{
			name:    "a comment inside the statement stays and the one above it does not",
			dialect: "sqlite",
			body:    "-- above\nINSERT INTO t /* inside */ VALUES (1); -- after\n",
			index:   1,
			sent:    "INSERT INTO t /* inside */ VALUES (1)",
			want:    "INSERT INTO t /* inside */ VALUES (1);",
		},
		{
			name:    "the dialect decides where a statement ends",
			dialect: "postgres",
			body:    "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;\nSELECT nope;\n",
			index:   2,
			sent:    "SELECT nope",
			want:    "SELECT nope;",
		},
		{
			name:    "a position that names another statement quotes the statement as sent",
			dialect: "sqlite",
			body:    "CREATE TABLE users (id int);\n\nALTER TABLE users RENAME COLUMN nope TO x;\n",
			index:   1,
			sent:    "ALTER TABLE users RENAME COLUMN nope TO x",
			want:    "ALTER TABLE users RENAME COLUMN nope TO x",
		},
		{
			name:    "a last statement with no terminator is quoted as sent",
			dialect: "sqlite",
			body:    "CREATE TABLE users (id int);\nALTER TABLE users RENAME COLUMN nope TO x\n",
			index:   2,
			sent:    "ALTER TABLE users RENAME COLUMN nope TO x",
			want:    "ALTER TABLE users RENAME COLUMN nope TO x",
		},
		{
			name:    "no position",
			dialect: "sqlite",
			body:    "SELECT 1;\n",
			index:   0,
			sent:    "SELECT 1",
			want:    "SELECT 1",
		},
		{
			name:    "a position past the last statement",
			dialect: "sqlite",
			body:    "SELECT 1;\n",
			index:   2,
			sent:    "SELECT 1",
			want:    "SELECT 1",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sourcestatement.Written(test.body, test.dialect, test.index, test.sent), qt.Equals, test.want)
		})
	}
}
