package lint

import (
	"errors"
	"fmt"

	"ptah.run/core/lintext"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/filetx"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/migratorext/builtin"
	"ptah.run/internal/txcontrol"
)

// transactionControlRules is TX201, which every dialect lint analyzes shares.
func transactionControlRules() []Rule {
	return []Rule{{
		Code:     "TX201",
		Title:    "transaction block embedded in migration",
		Severity: SeverityWarning,
		Dialects: lintDialects(func(described descriptor.Lint) bool { return described.Analyzed }),
		// The rule reads both halves of a migration and decides for itself
		// which statements to judge, so it sets CheckFile.
		CheckFile: checkTransactionControl,
	}}
}

// checkTransactionControl reports the transaction control a migration body
// carries where the migrator refuses it.
//
// The migrator refuses a body statement that controls transaction state, in
// the up and the down half alike, whenever it opens the transaction around the
// file or runs the file without one and commits each statement itself. Both
// refusals read [txcontrol.Controls], which this rule reads too, so lint
// reports what an apply would stop at on every engine the migrator applies. A
// block of several statements is one finding, on the statement that opens it:
// the statements the block holds are the same mistake.
//
// A run whose files run in transactions they open themselves, as with
// golang-migrate, is judged by the same reading the migrator applies such a
// directory with: see [ownTransactionFindings]. Only an up file on an engine
// whose migrator services read a body's own transactions is applied that way.
// A down file, and an up file on any other engine, keeps the transaction around
// the file, and is judged as above.
func checkTransactionControl(file *File) []Finding {
	if !file.IsUp && !file.IsDown {
		return nil
	}
	if file.transactionOwner == lintext.TransactionOwnerFile && file.IsUp {
		if read := ownTransactionReading(file.dialect); read != nil {
			return ownTransactionFindings(file, read)
		}
	}
	return wrappedTransactionControl(file)
}

// wrappedTransactionControl reports each block of transaction control in a
// file the migrator runs inside its transaction, or without one, committing
// each statement itself: it refuses every such statement either way.
func wrappedTransactionControl(file *File) []Finding {
	verb := "apply"
	if !file.IsUp {
		verb = "rollback"
	}
	var findings []Finding
	open := false
	for i := range file.Statements {
		stmt := &file.Statements[i]
		if !txcontrol.Controls(stmt.SQL, file.dialect) {
			continue
		}
		if open {
			open = !closesTransactionBlock(stmt.Words)
			continue
		}
		open = opensTransactionBlock(stmt.Words)
		findings = append(findings, transactionControlFinding(file, i, transactionControlMessage(file, verb, stmt)))
	}
	return findings
}

// ownTransactionReading returns the reading of a body's own transactions the
// migrator applies a directory of dialect with when the directory's files open
// their own, or nil where it keeps the transaction around each file. It is the
// field of the engine's migrator services the migrator reads, so the two cannot
// name different engines.
//
// A run that names no dialect reads a body the way the one engine that offers
// a reading does, [filetx.ReadUnwrapped]. Such a run cannot know the engine,
// and a golang-migrate directory that opens its own transactions is written
// for PostgreSQL far more often than for any other: read as wrapped, every
// such file of it would be reported, as ptah-compat's lint without a dev
// database does for a directory shaped like OpenMeter's (stokaro/ptah#4953).
// A run that names another engine is judged for it.
func ownTransactionReading(dialect string) func(statements []string) (filetx.Blocks, error) {
	if dialect == "" {
		return filetx.ReadUnwrapped
	}
	return builtin.Lookup(dialect).OwnTransactions
}

// ownTransactionFindings judges an up file the migrator applies with the
// transactions the file opens itself.
//
// read is the reading the migrator applies the file with. A form it refuses
// is refused before any migration runs, and is reported on the statement the
// refusal names. A file whose directives ask for a transaction around it and
// that opens its own is refused the same way. A file that opens none keeps the
// transaction around it, so its transaction control is judged as a wrapped
// file's. In a file the reading accepts, a block opened inside another is
// reported too, because the author's nesting is not what runs: see
// [nestedTransactionBlocks].
func ownTransactionFindings(file *File, read func(statements []string) (filetx.Blocks, error)) []Finding {
	statements := make([]string, len(file.Statements))
	for i := range file.Statements {
		statements[i] = file.Statements[i].SQL
	}
	blocks, err := read(statements)
	if structure, ok := errors.AsType[*filetx.StructureError](err); ok {
		return []Finding{transactionControlFinding(file, structure.Index, fmt.Sprintf(
			"the migrator runs the transactions this file opens itself and refuses the file before any migration "+
				"runs: %s; remove the transaction control or change it to a form the file can run",
			structure.Reason))}
	}
	if blocks.Empty() {
		return wrappedTransactionControl(file)
	}
	if file.txModeNamed && !file.NoTransaction {
		first := blocks.Controls()[0]
		return []Finding{transactionControlFinding(file, first,
			"the file asks for a transaction around it and opens its own, which the migrator refuses before any "+
				"migration runs; remove the transaction mode directive or the file's own transaction control")}
	}
	return nestedTransactionBlocks(file)
}

// transactionControlMessage says what the migrator does with the file where it
// runs, and the way out.
func transactionControlMessage(file *File, verb string, stmt *Statement) string {
	word := "statement"
	if len(stmt.Words) > 0 {
		word = stmt.Words[0]
	}
	if runsWithoutATransaction(file) {
		return fmt.Sprintf("the migrator refuses transaction control in a migration that runs without a transaction "+
			"and commits each statement on its own, so the %s stops at this %s; remove the transaction control", verb, word)
	}
	return fmt.Sprintf("the migrator runs this file in one transaction and refuses a statement that controls "+
		"transaction state, so the %s stops at this %s; remove the transaction control", verb, word)
}

// runsWithoutATransaction reports a file the migrator runs without a
// transaction: one whose directive says so, and a file of one statement in a
// directory whose files open their own transactions, on an engine whose
// migrator reads them. Lint marks such a file on every engine (see
// [markAloneOutsideATransaction]), but elsewhere the migrator keeps the
// transaction around a file of one transaction control statement.
func runsWithoutATransaction(file *File) bool {
	return file.NoTransaction && (file.txModeNamed || ownTransactionReading(file.dialect) != nil)
}

// nestedTransactionBlocks reports each BEGIN or START TRANSACTION of a file
// the migrator applies with its own transactions that runs while a block the
// file opened earlier is still open.
//
// A runner that opens no transaction around a file leaves the block as the only
// transaction the file gets, so a block alone changes nothing there. Measured
// with golang-migrate v4.20.1 on PostgreSQL 15.19 (stokaro/ptah#4953): by
// default the tool sends a file as one query, which PostgreSQL runs as one
// implicit transaction; with x-multi-statement=true it sends each statement on
// its own, and the block is what keeps a file that fails midway from
// committing part of itself. A second BEGIN inside an open block draws "there
// is already a transaction in progress": the file applies, and the first
// COMMIT ends what the author wrote as two blocks.
func nestedTransactionBlocks(file *File) []Finding {
	var findings []Finding
	open := -1
	for i := range file.Statements {
		words := file.Statements[i].Words
		switch {
		case opensTransactionBlock(words) && open >= 0:
			findings = append(findings, transactionControlFinding(file, i, fmt.Sprintf(
				"a transaction block is already open since the BEGIN on line %d; PostgreSQL ignores this BEGIN "+
					"with a warning, and the first COMMIT ends both blocks", file.Statements[open].Line)))
		case opensTransactionBlock(words):
			open = i
		case closesTransactionBlock(words):
			open = -1
		}
	}
	return findings
}

func transactionControlFinding(file *File, statement int, message string) Finding {
	return Finding{
		Rule:     "TX201",
		Title:    "transaction block embedded in migration",
		Severity: SeverityWarning,
		File:     file.Path,
		Line:     file.Statements[statement].Line,
		Message:  message,
		Context:  statementFindingContext(statement),
	}
}

// opensTransactionBlock reports a statement that opens a transaction block.
func opensTransactionBlock(words []string) bool {
	return lintkit.HasWordPrefix(words, "BEGIN") || lintkit.HasWordPrefix(words, "START", "TRANSACTION")
}

// closesTransactionBlock reports a statement that ends the open transaction
// block: COMMIT, END, ROLLBACK or ABORT, unless it rolls back to a savepoint,
// chains a new transaction, or settles a prepared one, and PREPARE
// TRANSACTION, which detaches the transaction from the session.
func closesTransactionBlock(words []string) bool {
	if lintkit.HasWordPrefix(words, "PREPARE", "TRANSACTION") {
		return true
	}
	if !lintkit.HasWordPrefix(words, "COMMIT") && !lintkit.HasWordPrefix(words, "END") &&
		!lintkit.HasWordPrefix(words, "ROLLBACK") && !lintkit.HasWordPrefix(words, "ABORT") {
		return false
	}
	if len(words) > 1 && words[1] == "PREPARED" {
		return false
	}
	return !lintkit.HasWordSeq(words, "TO") && !lintkit.HasWordSeq(words, "AND", "CHAIN")
}
