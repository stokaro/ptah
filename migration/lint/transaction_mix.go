package lint

import (
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/txrequire"
)

// checkTransactionMix reports a file whose statements cannot all run inside
// one transaction.
//
// The classification is [txrequire]'s, which the planner and the migrator's
// apply-time preflight also use. This rule used to carry its own -- a scan for
// concurrent indexes and nothing else -- and the two disagreed in exactly the
// place that mattered: a file adding a value to an existing enum type and then
// using it got no TX101, and that is the file PostgreSQL refuses at apply with
// `unsafe use of new value` (stokaro/ptah#996).
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
	result := txrequire.Analyze(platform.Postgres, capability.Postgres16(), statements)
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

func isReportedStatement(result txrequire.Result, index int) bool {
	for _, finding := range result.Findings {
		if finding.Statement.Index == index {
			return true
		}
	}
	return false
}
