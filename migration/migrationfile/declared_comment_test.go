package migrationfile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/internal/clientdelimiter"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// declaredDirectives are directive lines, as a declared comment can spell them,
// for the readers readsAnything drives. The checkpoint, txtar and nolint readers
// are unexported or live in packages that import this one, so their words are
// covered by the unit test of sqlutil.NeutralizeDirectives only.
var declaredDirectives = []string{
	"+ptah no_transaction",
	"+ptah lock_timeout=1s",
	"+ptah statement_timeout=1s",
	"+ptah check assert=\"SELECT 1\"",
	"atlas:txmode none",
	"atlas:txmode file",
	"atlas:delimiter $$",
}

// readsAnything reports what the file readers find in sql: a transaction mode,
// a directive, a timeout, a check or a delimiter.
func readsAnything(c *qt.C, sql string) bool {
	c.Helper()
	mode := migrationfile.ParseFileTxMode("x.up.sql", sql)
	timeouts, timeoutErr := migrationfile.ParseTimeouts(sql)
	checks, checkErr := migrator.ParseChecks(sql, "postgres")
	_, _, delimited := clientdelimiter.Parse(sql[:len(sql)-len("\nCREATE TABLE t (id int);\n")])
	return mode.Mode != migrationfile.FileTxModeUnspecified || mode.Err != nil ||
		len(migrationfile.ParseDirectives(sql)) > 0 ||
		timeouts != (migrationfile.Timeouts{}) || timeoutErr != nil ||
		len(checks) > 0 || checkErr != nil || delimited
}

// TestDeclaredComment_IsNotADirective writes each directive as declared text
// through the comment writer and reads the file back. No reader finds an
// instruction in it.
func TestDeclaredComment_IsNotADirective(t *testing.T) {
	for _, directive := range declaredDirectives {
		t.Run(directive, func(t *testing.T) {
			c := qt.New(t)

			sql := sqlutil.LineComment(directive) + "\nCREATE TABLE t (id int);\n"

			c.Assert(readsAnything(c, sql), qt.IsFalse, qt.Commentf("file:\n%s", sql))
		})
	}
}

// TestDeclaredComment_ControlReadsTheDirectiveWhenWrittenRaw is the control: the
// same text written as a bare comment is an instruction to at least one
// reader, so the test above is not reading nothing.
func TestDeclaredComment_ControlReadsTheDirectiveWhenWrittenRaw(t *testing.T) {
	for _, directive := range declaredDirectives {
		t.Run(directive, func(t *testing.T) {
			c := qt.New(t)

			sql := "-- " + directive + "\nCREATE TABLE t (id int);\n"

			c.Assert(readsAnything(c, sql), qt.IsTrue, qt.Commentf("file:\n%s", sql))
		})
	}
}
