package pglint

import (
	"ptah.run/core/lintext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/txrequire"
)

// transactionRules is the TX family.
func transactionRules() []lintkit.Rule {
	return []lintkit.Rule{
		{
			Rule: lintext.Rule{
				Code:     "TX101",
				Title:    "transactional and non-transactional statements mixed",
				Severity: lintext.Warning,
				Targets:  []string{platform.Postgres},
			},
			// Either direction, and whoever opens the transaction: a down
			// file runs the way an up file does, so a rollback that mixes
			// CREATE INDEX CONCURRENTLY with transactional DDL is refused by
			// PostgreSQL exactly the same way.
			File: checkTransactionMix,
		},
		{
			Rule: lintext.Rule{
				Code:     "TX201",
				Title:    "transaction block embedded in migration",
				Severity: lintext.Warning,
				Targets:  []string{platform.Postgres},
			},
			Statement: checkEmbeddedTransaction,
		},
	}
}

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
// Either direction is checked, because a down file runs the way an up file
// does. A migrator that wraps each file runs both inside its transaction. One
// that opens none, as golang-migrate does by default, sends the file as one
// query, and PostgreSQL runs that query as one implicit transaction, which
// refuses a concurrent index beside another statement just the same. Only
// golang-migrate's x-multi-statement mode, a setting of the database URL the
// directory does not carry, sends the statements one by one.
func checkTransactionMix(file *lintkit.File) []lintext.Violation {
	if (!file.IsUp && !file.IsDown) || file.NoTransaction {
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

// checkEmbeddedTransaction reports an explicit transaction block in a file the
// migrator wraps in a transaction of its own, which the block cannot nest in.
//
// A migrator that opens no transaction around a file leaves the block as the
// only transaction the file gets, so the block is not reported there. Measured
// with golang-migrate v4.20.1 on PostgreSQL 15.19 (stokaro/ptah#4953): by
// default the tool sends a file as one query, which PostgreSQL runs as one
// implicit transaction, so the block changes nothing; with
// x-multi-statement=true it sends each statement on its own, and the block is
// what keeps a file that fails midway from committing part of itself. The tool
// has no marker for a non-transactional file either, so the remedy the message
// names could not be followed.
func checkEmbeddedTransaction(file *lintkit.File, statement *lintext.Statement) (bool, string) {
	if file.TransactionOwner == lintext.TransactionOwnerFile {
		return false, ""
	}
	if !lintkit.HasWordPrefix(statement.Words, "BEGIN") && !lintkit.HasWordPrefix(statement.Words, "START", "TRANSACTION") {
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
	return lintkit.HasWordPrefix(words, "BEGIN") ||
		lintkit.HasWordPrefix(words, "START", "TRANSACTION") ||
		lintkit.HasWordPrefix(words, "COMMIT") ||
		lintkit.HasWordPrefix(words, "ROLLBACK")
}
