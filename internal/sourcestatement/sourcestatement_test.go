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
			name:    "a statement sent without its comments still names the body's text",
			dialect: "sqlite",
			body:    "INSERT INTO t (id)\n  -- inside\n  VALUES (1);\n",
			index:   1,
			sent:    "INSERT INTO t (id)\n  \n  VALUES (1)",
			want:    "INSERT INTO t (id)\n  -- inside\n  VALUES (1);",
		},
		{
			name:    "a declared delimiter is not part of the statement",
			dialect: "sqlite",
			body:    "-- atlas:delimiter $$\nCREATE TABLE a (id int)$$\nINSERT INTO nosuch VALUES (1)$$\n",
			index:   2,
			sent:    "INSERT INTO nosuch VALUES (1)",
			want:    "INSERT INTO nosuch VALUES (1)",
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

// Every statement is listed as the body spells it, the way the community
// binary lists the statements a file executed. The rows are its measured
// output for the same bodies on SQLite.
func TestEach(t *testing.T) {
	for _, test := range []struct {
		name, dialect, body string
		sent, want          []string
	}{
		{
			name:    "terminators stay and the comments above each statement go",
			dialect: "sqlite",
			body: "-- comment above\nCREATE TABLE b (id int);\nINSERT INTO b (id)\n  -- comment inside\n  VALUES (1);\n" +
				"INSERT INTO b VALUES (2)\n",
			sent: []string{"CREATE TABLE b (id int)", "INSERT INTO b (id)\n  \n  VALUES (1)", "INSERT INTO b VALUES (2)"},
			want: []string{"CREATE TABLE b (id int);", "INSERT INTO b (id)\n  -- comment inside\n  VALUES (1);", "INSERT INTO b VALUES (2)"},
		},
		{
			name:    "a comment after a terminator belongs to no statement",
			dialect: "sqlite",
			body:    "CREATE TABLE x (id int); -- after x\n/* block above */ CREATE TABLE z (id int /* in z */);\n",
			sent:    []string{"CREATE TABLE x (id int)", "CREATE TABLE z (id int )"},
			want:    []string{"CREATE TABLE x (id int);", "CREATE TABLE z (id int /* in z */);"},
		},
		{
			name:    "a terminator on a line of its own stays where the file put it",
			dialect: "sqlite",
			body:    "CREATE TABLE y (id int)\n;\n",
			sent:    []string{"CREATE TABLE y (id int)"},
			want:    []string{"CREATE TABLE y (id int)\n;"},
		},
		{
			name:    "a last statement with no terminator keeps the comment after it",
			dialect: "sqlite",
			body:    "INSERT INTO x (id)\n  -- in\n  VALUES (2) -- trailing\n\n",
			sent:    []string{"INSERT INTO x (id)\n  \n  VALUES (2)"},
			want:    []string{"INSERT INTO x (id)\n  -- in\n  VALUES (2) -- trailing"},
		},
		{
			name:    "a directive above the first statement is a comment",
			dialect: "sqlite",
			body:    "-- atlas:txmode none\n\nCREATE TABLE c (id int);\nINSERT INTO c VALUES (1);\n",
			sent:    []string{"CREATE TABLE c (id int)", "INSERT INTO c VALUES (1)"},
			want:    []string{"CREATE TABLE c (id int);", "INSERT INTO c VALUES (1);"},
		},
		{
			name:    "the dialect decides where a statement ends",
			dialect: "postgres",
			body:    "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;\nSELECT nope;\n",
			sent:    []string{"CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql", "SELECT nope"},
			want:    []string{"CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;", "SELECT nope;"},
		},
		{
			name:    "a declared delimiter goes and the text before it is trimmed",
			dialect: "sqlite",
			body: "-- atlas:delimiter $$\nCREATE TABLE a (id int) $$\nCREATE TABLE b (id int)\n$$\n" +
				"CREATE TABLE c (id int)\n  -- inside\n  $$\n",
			sent: []string{"CREATE TABLE a (id int)", "CREATE TABLE b (id int)", "CREATE TABLE c (id int)"},
			want: []string{"CREATE TABLE a (id int)", "CREATE TABLE b (id int)", "CREATE TABLE c (id int)\n  -- inside"},
		},
		{
			name:    "a semicolon the file wrote stays under a declared delimiter",
			dialect: "sqlite",
			body:    "-- atlas:delimiter $$\nCREATE TABLE a (id int);$$\nCREATE TABLE b (id int)$$\n",
			sent:    []string{"CREATE TABLE a (id int)", "CREATE TABLE b (id int)"},
			want:    []string{"CREATE TABLE a (id int);", "CREATE TABLE b (id int)"},
		},
		{
			name:    "a semicolon stays once the delimiter is set back",
			dialect: "sqlite",
			body:    "DELIMITER //\nCREATE TABLE c (id int)//\nDELIMITER ;\nCREATE TABLE d (id int);\n",
			sent:    []string{"CREATE TABLE c (id int)", "CREATE TABLE d (id int)"},
			want:    []string{"CREATE TABLE c (id int)", "CREATE TABLE d (id int);"},
		},
		{
			name:    "a trigger body keeps its own semicolons",
			dialect: "sqlite",
			body: "-- atlas:delimiter //\nCREATE TABLE a (id int)//\n" +
				"CREATE TRIGGER t AFTER INSERT ON a BEGIN INSERT INTO a VALUES (2); END//\n",
			sent: []string{"CREATE TABLE a (id int)", "CREATE TRIGGER t AFTER INSERT ON a BEGIN INSERT INTO a VALUES (2); END"},
			want: []string{"CREATE TABLE a (id int)", "CREATE TRIGGER t AFTER INSERT ON a BEGIN INSERT INTO a VALUES (2); END"},
		},
		{
			name:    "nothing sent",
			dialect: "sqlite",
			body:    "SELECT 1;\n",
			sent:    nil,
			want:    make([]string, 0),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sourcestatement.Each(test.body, test.dialect, test.sent), qt.DeepEquals, test.want)
		})
	}
}

// A statement whose position in the body holds another statement is listed as
// it was sent, so a body the caller split differently never borrows a
// neighbor's text.
func TestEach_ListsSentWhereTheBodyHoldsAnotherStatement(t *testing.T) {
	for _, test := range []struct {
		name, dialect, body string
		sent, want          []string
	}{
		{
			name:    "a split that disagrees with the body",
			dialect: "sqlite",
			body:    "CREATE TABLE users (id int);\n\nALTER TABLE users RENAME COLUMN nope TO x;\n",
			sent:    []string{"ALTER TABLE users RENAME COLUMN nope TO x"},
			want:    []string{"ALTER TABLE users RENAME COLUMN nope TO x"},
		},
		{
			name:    "more statements sent than the body holds",
			dialect: "sqlite",
			body:    "SELECT 1;\n",
			sent:    []string{"SELECT 1", "SELECT 2"},
			want:    []string{"SELECT 1;", "SELECT 2"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sourcestatement.Each(test.body, test.dialect, test.sent), qt.DeepEquals, test.want)
		})
	}
}
