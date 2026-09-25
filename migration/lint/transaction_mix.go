package lint

import (
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
func checkTransactionMix(file *File) []Finding {
	if (!file.IsUp && !file.IsDown) || file.NoTransaction {
		return nil
	}
	statements := make([]txrequire.Statement, 0, len(file.Statements))
	transactional := false
	for index := range file.Statements {
		statement := &file.Statements[index]
		if isTransactionControlStatement(statement.Words) {
			continue
		}
		statements = append(statements, txrequire.Statement{
			Index: statement.Index,
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
	return []Finding{{
		Rule:     "TX101",
		Title:    "transactional and non-transactional statements mixed",
		Severity: SeverityWarning,
		File:     file.Path,
		Line:     first.Statement.Line,
		Message: "this migration mixes PostgreSQL statements that require autocommit with " +
			"transactional DDL: " + first.Message + "; split them into separate migrations",
		Context: statementFindingContext(first.Statement.Index),
	}}
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
