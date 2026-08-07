package migrator

import (
	"errors"
	"strings"

	"go.5x5.cz/ptah/core/sqlutil"
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
// Unwrapping to the innermost error drops both. What is left is the driver's
// message, which still differs from that binary's — ours is a different SQLite
// driver and spells the same condition `SQL logic error: … (1)`. That residue
// is a driver difference rather than a Ptah one and is recorded in the gap
// register; no amount of string surgery here would close it without inventing
// per-driver rewrites (stokaro/ptah#1196 item 1).
//
// The native revision format is untouched. This runs only where the revision
// table is Atlas-shaped, and Ptah's own surface keeps the context it added.
func atlasFailureError(failure error) string {
	if failure == nil {
		return ""
	}
	innermost := failure
	for {
		unwrapped := errors.Unwrap(innermost)
		if unwrapped == nil {
			break
		}
		innermost = unwrapped
	}
	return strings.TrimSpace(innermost.Error())
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
func atlasFailureStatement(sqlText, dialect string, failedIndex int, stmt string) string {
	statements := sqlutil.SplitSourceStatements(sqlText, dialect)
	if failedIndex < 1 || failedIndex > len(statements) {
		return stmt
	}
	source := strings.TrimSpace(statements[failedIndex-1].Text)
	if source == "" {
		return stmt
	}
	return source
}
