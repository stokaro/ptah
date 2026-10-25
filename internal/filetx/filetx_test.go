package filetx_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/filetx"
)

func TestRead_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		want       []int
	}{
		{
			name:       "no transaction control",
			statements: []string{"CREATE TABLE a (id int)", "INSERT INTO a VALUES (1)"},
			want:       nil,
		},
		{
			name:       "file wrapped in one transaction",
			statements: []string{"BEGIN", "CREATE TABLE a (id int)", "COMMIT"},
			want:       []int{0, 2},
		},
		{
			name:       "start transaction and end",
			statements: []string{"START TRANSACTION", "CREATE TABLE a (id int)", "END"},
			want:       []int{0, 2},
		},
		{
			name:       "work and transaction noise words",
			statements: []string{"BEGIN WORK", "SELECT 1", "COMMIT TRANSACTION"},
			want:       []int{0, 2},
		},
		{
			name:       "modes",
			statements: []string{"BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY, NOT DEFERRABLE", "SELECT 1", "COMMIT WORK"},
			want:       []int{0, 2},
		},
		{
			name:       "serializable deferrable",
			statements: []string{"START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE", "SELECT 1", "COMMIT"},
			want:       []int{0, 2},
		},
		{
			name:       "two transactions in one file with work between",
			statements: []string{"BEGIN", "SELECT 1", "COMMIT", "CREATE TABLE a (id int)", "BEGIN", "SELECT 2", "COMMIT"},
			want:       []int{0, 2, 4, 6},
		},
		{
			name:       "and no chain is a plain commit",
			statements: []string{"BEGIN", "SELECT 1", "COMMIT AND NO CHAIN", "BEGIN", "END TRANSACTION AND NO CHAIN"},
			want:       []int{0, 2, 3, 4},
		},
		{
			name:       "lower case and a terminator",
			statements: []string{"begin;", "select 1;", "commit;"},
			want:       []int{0, 2},
		},
		{
			name:       "a comment before the boundary",
			statements: []string{"-- open\nBEGIN", "SELECT 1", "/* close */ COMMIT"},
			want:       []int{0, 2},
		},
		{
			name:       "begin inside a routine body is not a boundary",
			statements: []string{"CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$"},
			want:       nil,
		},
		{
			name:       "a quoted name is not a keyword",
			statements: []string{`"BEGIN"`},
			want:       nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := filetx.Read(test.statements)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Boundaries(), qt.DeepEquals, test.want)
			c.Assert(got.Empty(), qt.Equals, test.want == nil)
		})
	}
}

func TestRead_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		wantIndex  int
		wantReason string
	}{
		{name: "transaction never closed", statements: []string{"BEGIN", "CREATE TABLE a (id int)"}, wantIndex: 0, wantReason: "never closed"},
		{name: "commit without begin", statements: []string{"CREATE TABLE a (id int)", "COMMIT"}, wantIndex: 1, wantReason: "no transaction is open"},
		{name: "end without begin", statements: []string{"END"}, wantIndex: 0, wantReason: "no transaction is open"},
		{name: "nested begin", statements: []string{"BEGIN", "BEGIN", "COMMIT", "COMMIT"}, wantIndex: 1, wantReason: "already open since statement 1"},
		{name: "second commit", statements: []string{"BEGIN", "COMMIT", "COMMIT"}, wantIndex: 2, wantReason: "no transaction is open"},
		{name: "rollback", statements: []string{"BEGIN", "CREATE TABLE a (id int)", "ROLLBACK"}, wantIndex: 2, wantReason: "ROLLBACK takes the file's own work back"},
		{name: "abort", statements: []string{"BEGIN", "ABORT"}, wantIndex: 1, wantReason: "ABORT takes the file's own work back"},
		{name: "rollback outside a transaction", statements: []string{"ROLLBACK"}, wantIndex: 0, wantReason: "ROLLBACK takes the file's own work back"},
		{name: "savepoint", statements: []string{"BEGIN", "SAVEPOINT s", "COMMIT"}, wantIndex: 1, wantReason: "SAVEPOINT is partial transaction state"},
		{name: "release savepoint", statements: []string{"BEGIN", "RELEASE SAVEPOINT s", "COMMIT"}, wantIndex: 1, wantReason: "RELEASE is partial transaction state"},
		{name: "rollback to savepoint", statements: []string{"BEGIN", "SAVEPOINT s", "ROLLBACK TO SAVEPOINT s", "COMMIT"}, wantIndex: 1, wantReason: "SAVEPOINT is partial transaction state"},
		{name: "commit and chain", statements: []string{"BEGIN", "COMMIT AND CHAIN"}, wantIndex: 1, wantReason: "COMMIT AND CHAIN starts a transaction"},
		{name: "end and chain", statements: []string{"BEGIN", "END WORK AND CHAIN"}, wantIndex: 1, wantReason: "END AND CHAIN starts a transaction"},
		{name: "prepare transaction", statements: []string{"BEGIN", "PREPARE TRANSACTION 'x'"}, wantIndex: 1, wantReason: "PREPARE TRANSACTION leaves a prepared transaction"},
		{name: "commit prepared", statements: []string{"COMMIT PREPARED 'x'"}, wantIndex: 0, wantReason: "COMMIT PREPARED leaves a prepared transaction"},
		{name: "unknown begin mode", statements: []string{"BEGIN SNAPSHOT 'abc'", "COMMIT"}, wantIndex: 0, wantReason: "not a transaction mode"},
		{name: "trailing words after commit", statements: []string{"BEGIN", "COMMIT garbage"}, wantIndex: 1, wantReason: "which Ptah does not read"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := filetx.Read(test.statements)
			c.Assert(got.Empty(), qt.IsTrue)
			var structure *filetx.StructureError
			c.Assert(err, qt.ErrorAs, &structure)
			c.Assert(structure.Index, qt.Equals, test.wantIndex)
			c.Assert(structure.StatementIndex(), qt.Equals, test.wantIndex)
			c.Assert(structure.Reason, qt.Contains, test.wantReason)
		})
	}
}

// The structure error names the statement, counted from one.
func TestStructureError_Error(t *testing.T) {
	c := qt.New(t)
	_, err := filetx.Read([]string{"SELECT 1", "COMMIT"})
	c.Assert(err, qt.ErrorMatches,
		`the file controls its own transactions in a form Ptah does not run, at statement 2: no transaction is open to close`)
}

// RunsAlone counts the statements that hold something to run: a comment or an
// empty statement beside the one statement leaves the file one that runs
// alone, and a second statement does not.
func TestRunsAlone(t *testing.T) {
	for _, test := range []struct {
		name       string
		statements []string
		want       bool
	}{
		{"one statement", []string{"CREATE INDEX CONCURRENTLY i ON t (id)"}, true},
		{"one statement after a comment", []string{"-- build it\nCREATE INDEX CONCURRENTLY i ON t (id)"}, true},
		{"one statement beside empty and comment-only ones", []string{"", "CREATE INDEX CONCURRENTLY i ON t (id)", "-- done", ";"}, true},
		{"two statements", []string{"CREATE TABLE t (id int)", "CREATE INDEX CONCURRENTLY i ON t (id)"}, false},
		{"no statement", []string{"-- nothing"}, false},
		{"none at all", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(filetx.RunsAlone(test.statements), qt.Equals, test.want)
		})
	}
}

// Each statement of a file with a statement before, inside, between and after
// its two transactions:
//
//	0 CREATE   outside
//	1 BEGIN    opens the first
//	2 INSERT   inside the first
//	3 COMMIT   closes the first
//	4 UPDATE   outside
//	5 BEGIN    opens the second
//	6 COMMIT   closes the second
//	7 DROP     outside
func TestBlocks_Positions(t *testing.T) {
	c := qt.New(t)
	blocks, err := filetx.Read([]string{
		"CREATE TABLE a (id int)", "BEGIN", "INSERT INTO a VALUES (1)", "COMMIT",
		"UPDATE a SET id = 2", "BEGIN", "COMMIT", "DROP TABLE b",
	})
	c.Assert(err, qt.IsNil)
	for _, test := range []struct {
		index     int
		inside    bool
		closes    bool
		opening   int
		opened    bool
		committed int
	}{
		{index: 0, inside: false, closes: false, opening: 0, opened: false, committed: 0},
		{index: 1, inside: true, closes: false, opening: 1, opened: true, committed: 1},
		{index: 2, inside: true, closes: false, opening: 1, opened: true, committed: 1},
		{index: 3, inside: false, closes: true, opening: 1, opened: true, committed: 1},
		{index: 4, inside: false, closes: false, opening: 0, opened: false, committed: 4},
		{index: 5, inside: true, closes: false, opening: 5, opened: true, committed: 5},
		{index: 6, inside: false, closes: true, opening: 5, opened: true, committed: 5},
		{index: 7, inside: false, closes: false, opening: 0, opened: false, committed: 7},
	} {
		t.Run(blocksStatementName(test.index), func(t *testing.T) {
			c := qt.New(t)
			opening, opened := blocks.Opening(test.index)
			c.Assert(blocks.Inside(test.index), qt.Equals, test.inside)
			c.Assert(blocks.Closes(test.index), qt.Equals, test.closes)
			c.Assert(opening, qt.Equals, test.opening)
			c.Assert(opened, qt.Equals, test.opened)
			c.Assert(blocks.Committed(test.index), qt.Equals, test.committed)
		})
	}
}

// The zero value is a file with no transaction of its own: every statement is
// outside and commits on its own.
func TestBlocks_ZeroValue(t *testing.T) {
	c := qt.New(t)
	var blocks filetx.Blocks
	opening, opened := blocks.Opening(3)
	c.Assert(blocks.Empty(), qt.IsTrue)
	c.Assert(blocks.Boundaries(), qt.IsNil)
	c.Assert(blocks.Inside(3), qt.IsFalse)
	c.Assert(blocks.Closes(3), qt.IsFalse)
	c.Assert(opening, qt.Equals, 0)
	c.Assert(opened, qt.IsFalse)
	c.Assert(blocks.Committed(3), qt.Equals, 3)
}

func blocksStatementName(index int) string {
	return []string{"create", "first begin", "insert", "first commit", "update", "second begin", "second commit", "drop"}[index]
}
