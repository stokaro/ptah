package migrator

import (
	"strings"

	"ptah.run/internal/dbanswer"
	"ptah.run/internal/revisiontext"
)

// atlasFailureError is what the Atlas revision table's `error` column records
// for a failed migration.
//
// The pinned Atlas community binary v1.3.0 records the database's own message
// and nothing else. Ptah records the chain it built on the way up, which on
// SQLite reads
//
//	failed to execute migration SQL: SQL logic error: no such table: t (1)
//	SQL: INSERT INTO t (id) VALUES (1)
//
// where that binary records `no such table: t`. Two of those three parts are
// Ptah's own: the wrapping prefix, and a `SQL:` line repeating a statement the
// adjacent `error_stmt` column already holds in full.
//
// Stepping down the chain past the wrappers that only add context drops both.
// What is left is the driver's message, which still differs from that
// binary's — ours is a different SQLite driver and spells the same condition
// `SQL logic error: … (1)`. That residue is a driver difference rather than a
// Ptah one and is recorded in the gap register; no amount of string surgery
// here would close it without inventing per-driver rewrites
// (stokaro/ptah#1196 item 1).
//
// The walk is [dbanswer.Of], the one a replayed migration reads its answer
// through, and it stops at a wrapper that rewrote the text it wraps. The
// PostgreSQL writer takes a role password out of the server's message that
// way, and the server's own error below it still quotes the password, so the
// innermost error would put the password in a table anyone who reads the
// revisions can read (stokaro/ptah#5993).
//
// The statement error is the one wrapper the walk can stop at that adds
// nothing worth keeping: when the writer did not end its message with the
// statement, [MigrationExecutionError] appends it, and `error_stmt` holds it
// already. The walk goes on below it.
//
// The native revision format is untouched. This runs only where the revision
// table is Atlas-shaped, and Ptah's own surface keeps the context it added.
func atlasFailureError(failure error) string {
	if failure == nil {
		return ""
	}
	answer := dbanswer.Of(failure)
	for {
		statement, ok := answer.(*MigrationExecutionError)
		if !ok || statement.Err == nil {
			break
		}
		answer = dbanswer.Of(statement.Err)
	}
	return revisiontext.ValidUTF8(strings.TrimSpace(answer.Error()))
}

// atlasFailureStatement is what the `error_stmt` column records: the failing
// statement as it was written, terminator included.
//
// The executor carries the normalized statement, which has had its terminating
// semicolon stripped, so recording it loses a character that binary keeps. The
// statement is recovered from the migration source instead, at the 1-based
// index the failure reports.
//
// That index is not derivable from the applied count. A transaction that rolled
// the body back reports zero applied while the statement that failed is still
// the one it was, so indexing by applied would record the file's first
// statement for every tx-mode-all failure.
//
// Falling back to the executor's text keeps a shape this cannot resolve — a
// source that splits differently, or a failure with no statement — reporting
// the statement it always did rather than nothing at all.
func atlasFailureStatement(sqlText string, grammar bodyGrammar, failedIndex int, stmt string) string {
	statements := grammar.sourceStatements(sqlText)
	if failedIndex < 1 || failedIndex > len(statements) {
		return revisiontext.ValidUTF8(stmt)
	}
	source := strings.TrimSpace(statements[failedIndex-1].Text)
	if source == "" {
		return revisiontext.ValidUTF8(stmt)
	}
	return revisiontext.ValidUTF8(source)
}
