package pglint

import (
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/txrequire"
)

// checkTransactionMix reports a file whose statements cannot all run inside
// one transaction.
//
// The classification is [txrequire]'s, which the planner and the migrator's
// apply-time preflight also use. A classification of this rule's own -- a scan
// for concurrent indexes and nothing else -- disagrees with theirs in exactly
// the place that matters: a file adding a value to an existing enum type and
// then using it gets no TX101, and that is the file PostgreSQL refuses at apply
// with `unsafe use of new value` (stokaro/ptah#996).
//
// Either direction is checked. The migrator wraps a down file in the same
// transaction it wraps an up file in, so a rollback that mixes a concurrent
// index with transactional DDL is refused the same way.
func checkTransactionMix(_ lintext.Target, files []lintext.File, index int) []lintext.Violation {
	file := &files[index]
	if (!file.Up && !file.Down) || file.NoTransaction {
		return nil
	}
	statements := make([]txrequire.Statement, 0, len(file.Statements))
	for position := range file.Statements {
		statement := &file.Statements[position]
		if isTransactionControlStatement(statement.Words) {
			continue
		}
		statements = append(statements, txrequire.Statement{
			Index: position,
			Line:  statement.Line,
			SQL:   statement.SQL,
		})
	}
	result := txrequire.Analyze(platform.Postgres, transactionMixCapabilities, statements)
	if !result.RequiresAutocommit() {
		return nil
	}
	// A file that is ENTIRELY non-transactional is not a mix; it is a file that
	// should carry the directive, which is a different rule's business.
	transactional := false
	for _, statement := range statements {
		if !isReportedStatement(result, statement.Index) {
			transactional = true
			break
		}
	}
	if !transactional {
		return nil
	}
	first := result.Findings[0]
	return []lintext.Violation{{
		Statement: first.Statement.Index,
		Message: "this migration mixes PostgreSQL statements that require autocommit with " +
			"transactional DDL: " + first.Message + "; split them into separate migrations",
	}}
}

// checkEmbeddedTransaction reports an explicit transaction block, which the
// migrator's own transaction management cannot nest.
func checkEmbeddedTransaction(_ lintext.Target, _ *lintext.File, statement *lintext.Statement) (bool, string) {
	if !hasWordPrefix(statement.Words, "BEGIN") && !hasWordPrefix(statement.Words, "START", "TRANSACTION") {
		return false, ""
	}
	return true, "explicit transaction blocks conflict with the migrator's transaction management; remove BEGIN/COMMIT or mark the migration non-transactional"
}

// transactionMixCapabilities is the server a file is judged against offline.
//
// PostgreSQL 16 carries the concurrent-index keys. Hypertables is added
// because [txrequire.Analyze] keys the per-chunk build on it, and the linter
// has no connection to ask: a statement that names TimescaleDB's own storage
// parameter is the evidence that the target has the extension, the way a
// schema that declares the extension is for the renderer
// ([capability.WithDeclaredExtensions]). On a server without it the statement
// is refused whatever the transaction, so no file is misjudged by assuming it.
var transactionMixCapabilities = capability.Postgres16().With(capability.Hypertables, true)

func isReportedStatement(result txrequire.Result, index int) bool {
	for _, finding := range result.Findings {
		if finding.Statement.Index == index {
			return true
		}
	}
	return false
}

// isTransactionControlStatement reports a statement that opens or closes a
// transaction, which the mix judges as neither side.
func isTransactionControlStatement(words []string) bool {
	return hasWordPrefix(words, "BEGIN") ||
		hasWordPrefix(words, "START", "TRANSACTION") ||
		hasWordPrefix(words, "COMMIT") ||
		hasWordPrefix(words, "ROLLBACK")
}

// hasWordPrefix reports whether words starts with prefix.
func hasWordPrefix(words []string, prefix ...string) bool {
	return len(words) >= len(prefix) && slices.Equal(words[:len(prefix)], prefix)
}
