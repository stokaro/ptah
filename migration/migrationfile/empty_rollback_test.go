package migrationfile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

func TestDeclaresEmptyRollback_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{name: "the bare directive", sql: "-- +ptah empty_rollback\n", want: true},
		{name: "an explicit true", sql: "-- +ptah empty_rollback=true\n", want: true},
		{name: "an explicit false", sql: "-- +ptah empty_rollback=false\n", want: false},
		{name: "beside a comment", sql: "-- nothing to undo\n-- +ptah empty_rollback\n-- see the ticket\n", want: true},
		{name: "beside another directive", sql: "-- +ptah no_transaction empty_rollback\n", want: true},
		{name: "above a statement", sql: "-- +ptah empty_rollback\nSELECT 1;\n", want: true},
		{name: "a file with no directive", sql: "-- No rollback was provided.\n", want: false},
		{name: "an empty file", sql: "", want: false},
		{name: "another bare word", sql: "-- +ptah empty_rollbacks\n", want: false},
		{name: "below a statement", sql: "SELECT 1;\n-- +ptah empty_rollback\n", want: false},
		{name: "inside a string", sql: "SELECT '\n-- +ptah empty_rollback\n';\n", want: false},
		{name: "an ordinary comment naming it", sql: "-- use +ptah empty_rollback here\n", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrationfile.DeclaresEmptyRollback(test.sql, "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestDeclaresEmptyRollback_FailurePath(t *testing.T) {
	c := qt.New(t)
	got, err := migrationfile.DeclaresEmptyRollback("-- +ptah empty_rollback=maybe\n", "postgres")
	c.Assert(err, qt.ErrorMatches, `invalid \+ptah empty_rollback value "maybe": expected true or false`)
	c.Assert(got, qt.IsFalse)
}

// TestDeclaresEmptyRollback_ReadsTheTargetsComments covers where the directive header
// ends. `#` opens a comment on MySQL and is SQL on SQL Server, where a directive below
// it stands below a statement.
func TestDeclaresEmptyRollback_ReadsTheTargetsComments(t *testing.T) {
	tests := []struct {
		dialect string
		want    bool
	}{
		{dialect: "mysql", want: true},
		{dialect: "sqlserver", want: false},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrationfile.DeclaresEmptyRollback("# a note\n-- +ptah empty_rollback\n", test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestParseTimeouts_TakesTheBareDirective covers the parser that refuses a bare
// word it does not know, which reads every directive line of a file the migrator
// loads. A down file carrying the directive has to load.
func TestParseTimeouts_TakesTheBareDirective(t *testing.T) {
	c := qt.New(t)
	timeouts, err := migrationfile.ParseTimeouts("-- +ptah empty_rollback lock_timeout=5s\n")
	c.Assert(err, qt.IsNil)
	c.Assert(timeouts.HasLockTimeout, qt.IsTrue)
}

// TestMisplacedDirectives_ReportsTheDirectiveBelowAStatement covers the line that
// does nothing where it stands. Its value is held to the same grammar there.
func TestMisplacedDirectives_ReportsTheDirectiveBelowAStatement(t *testing.T) {
	c := qt.New(t)
	misplaced := migrationfile.MisplacedDirectives("SELECT 1;\n-- +ptah empty_rollback\n", "postgres")
	c.Assert(misplaced, qt.HasLen, 1)
	c.Assert(misplaced[0].Line, qt.Equals, 2)
	c.Assert(misplaced[0].Err, qt.IsNil)

	malformed := migrationfile.MisplacedDirectives("SELECT 1;\n-- +ptah empty_rollback=maybe\n", "postgres")
	c.Assert(malformed, qt.HasLen, 1)
	c.Assert(malformed[0].Err, qt.ErrorMatches, `invalid \+ptah empty_rollback value "maybe": expected true or false`)
}
