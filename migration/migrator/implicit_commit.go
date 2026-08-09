package migrator

import (
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/lexer"
)

// implicitCommitDialect reports whether the server commits an open transaction
// on its own, without the client asking, before certain statements run.
//
// Only the MySQL family does. Everywhere else a migration body that ran inside
// one transaction either survives whole or leaves nothing behind, so "did the
// rollback undo this statement?" has a single answer for the whole file. On
// MySQL and MariaDB the answer differs statement by statement, and a resume
// that assumes otherwise either repeats committed SQL or skips SQL that never
// ran.
func implicitCommitDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

// implicitCommitEffect is what a MySQL-family server does to the transaction
// that is open when a statement starts.
//
// Two bits matter, not one. Whether the statement makes the work before it
// durable is the obvious bit. Whether it leaves a transaction open afterwards
// is the bit the first version of this file missed, and it is the one that
// decides the fate of every statement that follows: after a DDL statement no
// transaction is open, so the statements after it commit themselves and a later
// ROLLBACK cannot reach them.
type implicitCommitEffect int

const (
	// implicitCommitNone leaves the transaction state alone. The statement is
	// durable exactly when no transaction was open.
	implicitCommitNone implicitCommitEffect = iota
	// implicitCommitEnds commits the open transaction and leaves none open, so
	// every statement after it commits on its own.
	implicitCommitEnds
	// implicitCommitRestarts commits the open transaction and immediately opens
	// a new one, so the statements after it are pending again.
	implicitCommitRestarts
	// implicitCommitDiscards throws the open transaction away. The work before
	// it is gone, which no prefix counter can describe.
	implicitCommitDiscards
)

// commitsBeforeItRuns reports whether the server has already committed whatever
// was pending by the time the statement itself executes — including when the
// statement then fails. Measured on both servers: an INSERT before a
// `CREATE TABLE` that fails with "table already exists" still survives the
// ROLLBACK that follows.
func (e implicitCommitEffect) commitsBeforeItRuns() bool {
	return e == implicitCommitEnds || e == implicitCommitRestarts
}

// implicitCommitEffectOf classifies one statement of a MySQL-family migration
// body.
//
// Every row below was measured on MySQL 9.7.1 and MariaDB 11.4.12 with the same
// probe, one statement at a time:
//
//	START TRANSACTION;
//	INSERT INTO led VALUES (1,'a_pre');
//	<the statement under test>
//	INSERT INTO led VALUES (2,'b_post');
//	ROLLBACK;
//	SELECT note FROM led;
//
// Both rows surviving means the statement committed the prefix and left no
// transaction open (implicitCommitEnds). Only `a_pre` surviving means it
// committed the prefix and opened a new transaction (implicitCommitRestarts).
// Neither surviving means it committed nothing (implicitCommitNone). Only
// `b_post` surviving means the prefix was thrown away (implicitCommitDiscards).
// [TestImplicitCommitEffectOf] carries the measured result of every probe.
//
// Two rows differ between the servers, which is why the dialect reaches this
// far down: `CACHE INDEX` and `LOAD INDEX INTO CACHE` end the transaction on
// MySQL and do nothing at all on MariaDB.
//
// An unclassified statement is reported as implicitCommitNone. That direction
// is the one a resume can survive: the statement is treated as undone and run
// again, instead of being skipped forever on the strength of a guess.
func implicitCommitEffectOf(statement, dialect string) implicitCommitEffect {
	tokens := significantSQLTokens(statement, dialect)
	if len(tokens) == 0 {
		return implicitCommitNone
	}
	head := tokens[0]
	switch {
	case matchesAnyKeyword(head, "CREATE", "DROP"):
		// Measured: every CREATE and DROP ends the transaction — table, index,
		// view, database, user — except the TEMPORARY forms, which commit
		// nothing.
		if hasTemporaryQualifier(tokens) {
			return implicitCommitNone
		}
		return implicitCommitEnds
	case head.MatchIdentifierValue("SET"):
		return setStatementEffect(tokens)
	case head.MatchIdentifierValue("LOAD"):
		return loadStatementEffect(tokens, dialect)
	case head.MatchIdentifierValue("CACHE"):
		// Measured: `CACHE INDEX mi IN default` ends the transaction on MySQL
		// and commits nothing on MariaDB.
		return keyCacheEffect(dialect)
	case matchesAnyKeyword(head, "BEGIN", "START"):
		// Measured: both commit the prefix and open a new transaction, so the
		// statement after them is pending again. `START` reaches here only as
		// `START TRANSACTION`; no other START form was measured, and treating
		// one as opening a transaction costs nothing a resume can notice.
		return implicitCommitRestarts
	case head.MatchIdentifierValue("COMMIT"):
		// Measured: `COMMIT` ends the transaction, `COMMIT AND CHAIN` opens a
		// new one straight away.
		if hasChainClause(tokens) {
			return implicitCommitRestarts
		}
		return implicitCommitEnds
	case head.MatchIdentifierValue("ROLLBACK"):
		// Measured: `ROLLBACK TO SAVEPOINT` keeps the transaction and commits
		// nothing; a whole-transaction ROLLBACK destroys the prefix.
		if isSavepointRollback(tokens) {
			return implicitCommitNone
		}
		return implicitCommitDiscards
	case matchesAnyKeyword(head,
		// Measured, identical on both servers: each of these ends the
		// transaction. The representative statement measured for each keyword
		// is the one [TestImplicitCommitEffectOf] carries.
		"ALTER", "ANALYZE", "CHECK", "FLUSH", "GRANT", "INSTALL", "LOCK",
		"OPTIMIZE", "RENAME", "REPAIR", "RESET", "REVOKE", "TRUNCATE",
		"UNINSTALL",
	):
		return implicitCommitEnds
	}
	return implicitCommitNone
}

// setStatementEffect classifies the SET forms.
//
// Measured: `SET PASSWORD` and `SET DEFAULT ROLE` end the transaction. Every
// other SET commits nothing — including `SET autocommit = 1`, which the first
// version of this file listed as committing. It does not, because the session
// autocommit value is already 1: Ptah opens the migration transaction with
// START TRANSACTION and never turns session autocommit off, so the assignment
// has nothing to change and commits nothing. `SET autocommit = 0` was measured
// in the same session shape and also commits nothing.
func setStatementEffect(tokens []lexer.Token) implicitCommitEffect {
	rest := tokens[1:]
	if len(rest) == 0 {
		return implicitCommitNone
	}
	if rest[0].MatchIdentifierValue("PASSWORD") {
		return implicitCommitEnds
	}
	if len(rest) > 1 && rest[0].MatchIdentifierValue("DEFAULT") && rest[1].MatchIdentifierValue("ROLE") {
		return implicitCommitEnds
	}
	return implicitCommitNone
}

// loadStatementEffect classifies the LOAD forms.
//
// Measured: `LOAD DATA INFILE` commits nothing on either server — a ROLLBACK
// takes the INSERT before it and the loaded rows with it. `LOAD INDEX INTO
// CACHE` ends the transaction on MySQL and commits nothing on MariaDB.
func loadStatementEffect(tokens []lexer.Token, dialect string) implicitCommitEffect {
	if len(tokens) > 1 && tokens[1].MatchIdentifierValue("INDEX") {
		return keyCacheEffect(dialect)
	}
	return implicitCommitNone
}

// keyCacheEffect answers for the two MyISAM key-cache statements, the only
// place the two servers disagree.
func keyCacheEffect(dialect string) implicitCommitEffect {
	if platform.NormalizeDialect(dialect) == platform.MySQL {
		return implicitCommitEnds
	}
	return implicitCommitNone
}

// hasTemporaryQualifier reports whether TEMP or TEMPORARY qualifies the object
// a leading CREATE or DROP names.
//
// The qualifier can only appear before the TABLE keyword, so the scan stops
// there. Scanning a fixed number of leading tokens instead reads a column named
// `temp` in `CREATE TABLE hastemp (temp INT)` as the qualifier — measured, that
// statement ends the transaction like any other CREATE TABLE.
func hasTemporaryQualifier(tokens []lexer.Token) bool {
	for i := range tokens[1:] {
		token := tokens[1+i]
		if token.MatchIdentifierValue("TEMP") || token.MatchIdentifierValue("TEMPORARY") {
			return true
		}
		if token.MatchIdentifierValue("TABLE") {
			return false
		}
	}
	return false
}

// hasChainClause reports whether a COMMIT carries AND CHAIN rather than
// AND NO CHAIN.
func hasChainClause(tokens []lexer.Token) bool {
	for i := range tokens {
		if !tokens[i].MatchIdentifierValue("CHAIN") {
			continue
		}
		return i == 0 || !tokens[i-1].MatchIdentifierValue("NO")
	}
	return false
}

// isSavepointRollback reports whether a ROLLBACK names a savepoint instead of
// discarding the whole transaction.
func isSavepointRollback(tokens []lexer.Token) bool {
	return containsAnyKeyword(tokens[1:], "TO")
}

func containsAnyKeyword(tokens []lexer.Token, keywords ...string) bool {
	for i := range tokens {
		if matchesAnyKeyword(tokens[i], keywords...) {
			return true
		}
	}
	return false
}

// committedPrefixAfterRollback returns how many leading statements of sqlText a
// MySQL-family server has already committed when the transaction wrapping the
// body rolls back, given that executed statements ran and that the statement at
// failedIndex (1-based, 0 when no statement itself failed) is the one that
// stopped the run.
//
// The rule is not "the prefix ends at the last statement that forced a commit".
// An implicit commit ENDS the transaction; it does not merely flush it. Every
// statement after one therefore runs with no transaction open and commits
// itself, and the ROLLBACK the failure triggers reaches none of them. Measured
// on MySQL 9.7.1 and MariaDB 11.4.12:
//
//	START TRANSACTION; INSERT INTO led VALUES (1,'one'); CREATE TABLE ddl1 (i INT);
//	INSERT INTO led VALUES (3,'three'); ROLLBACK; SELECT id,note FROM led ORDER BY id;
//	-> rows 1 and 3 both survive
//
// So the walk below tracks whether a transaction is open at all, and counts a
// statement as committed either because it forced the commit itself or because
// nothing was open to hold it. The failing statement counts the same way: the
// server commits before it runs, and it does so even when the statement then
// fails.
//
// A body that rolls back a transaction it is itself inside is the one shape no
// counter can describe — the statements after the ROLLBACK are durable and the
// ones before it are not, so the durable set is not a prefix. Zero is the only
// prefix that never claims a statement committed which did not, so that is what
// it reports, at the cost of re-running the statements after the in-body
// ROLLBACK. A ROLLBACK that arrives with no transaction open is a no-op and
// costs nothing.
func committedPrefixAfterRollback(sqlText, dialect string, executed, failedIndex int) int {
	if executed <= 0 {
		return 0
	}
	statements := splitSQLStatementsForDialect(sqlText, dialect)
	if len(statements) < executed {
		// The executor counted more statements than the split finds, so the
		// prefix cannot be classified statement by statement. Reporting nothing
		// committed is the conservative answer: a retry re-runs the body from
		// the top instead of skipping a statement that may never have run.
		return 0
	}
	inTransaction := true
	committed := 0
	for i, statement := range statements[:executed] {
		switch implicitCommitEffectOf(statement, dialect) {
		case implicitCommitEnds:
			committed, inTransaction = i+1, false
		case implicitCommitRestarts:
			committed, inTransaction = i+1, true
		case implicitCommitDiscards:
			// With no transaction open there is nothing to throw away and the
			// statement is a no-op; the prefix ahead of it is already durable.
			if inTransaction {
				return 0
			}
			committed = i + 1
		case implicitCommitNone:
			if !inTransaction {
				committed = i + 1
			}
		}
	}
	if failedIndex > executed && failedIndex <= len(statements) &&
		implicitCommitEffectOf(statements[failedIndex-1], dialect).commitsBeforeItRuns() {
		return executed
	}
	return committed
}
