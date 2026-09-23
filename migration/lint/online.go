package lint

import (
	"fmt"
	"slices"
	"strings"
)

// The ON family is an allowlist, which is what separates it from every other
// family here.
//
// The rest of this package is a blocklist: a rule fires on a hazard somebody
// wrote a rule for, and a statement nobody anticipated -- a DDL form no rule
// covers, hand-written SQL, a spelling a dialect grew last year -- passes in
// silence. Silence there reads exactly like approval, which is fine for advice
// and useless for a guarantee.
//
// So these two rules invert it. Each proves a statement belongs to a set
// measured on the engine, and reports everything else, including everything it
// does not recognize. A form that arrives tomorrow is refused until somebody
// measures it, which is the only way the answer can be a guarantee rather than
// a hope.
//
// # What the mode proves, exactly
//
// Every statement in this migration is, on this engine at this version, a
// catalog-only change or an operation whose lock does not conflict with reads
// and writes for longer than it takes to acquire.
//
// It deliberately does not say "and scans no rows". PostgreSQL's
// `VALIDATE CONSTRAINT` scans the table under SHARE UPDATE EXCLUSIVE, which
// blocks neither readers nor writers -- measured on PostgreSQL 18.6 through
// pg_locks -- and it is the second half of the form Ptah generates to make a
// constraint addition online at all. A property that refused it would refuse
// the repair along with the hazard. What a scan costs depends on how many rows
// there are, which is the first of the things below that nothing here can see.
//
// # What it does not prove, and must not be read as proving
//
// Table size, concurrent load, replication lag and application rollout are all
// outside what any reading of the SQL can answer. The lock a statement takes
// is a property of the statement; an outage is a property of the system around
// it. On PostgreSQL the gap between the two is usually the lock queue: an
// instant ADD COLUMN still takes ACCESS EXCLUSIVE briefly, and if anything
// holds a conflicting lock the ALTER waits -- with every later reader and
// writer of that table waiting behind it. That is why the apply-time mode
// requires a lock timeout rather than only reading the SQL.

// onlineRules is the family: one rule per engine family the mode covers.
//
// Two rules rather than one because each proves a different thing. The
// PostgreSQL rule reads the statement and decides; the MySQL-family rule
// checks that the statement asked the SERVER to decide, which is a stronger
// answer and a shorter list.
func onlineRules() []Rule {
	return []Rule{postgresOnlineRule(), mysqlOnlineRule(), postgresOnlineTransactionRule()}
}

// onlineModeDialects are the dialects the online mode covers, in the order a
// refusal names them.
//
// The list is short because the guarantee is: each of these has a measurement
// behind it in this package. An engine is added here when somebody measures
// it, not when its documentation claims online DDL.
func onlineModeDialects() []string {
	return []string{"postgres", "mysql", "mariadb"}
}

// onlineModeCovers reports whether the mode has a measured answer for the
// dialect.
func onlineModeCovers(dialect string) bool {
	return slices.Contains(onlineModeDialects(), dialect)
}

// ValidateOnlineDialect refuses the online mode on an engine it has no
// measurement for, naming the ones it covers.
//
// It is exported because the refusal has two callers: the configuration, which
// can check the dialect it names itself, and the apply-time gate, which learns
// the engine from the connection. One predicate for both, so a policy accepted
// offline cannot be refused at apply for a different reason than it would have
// been refused here.
func ValidateOnlineDialect(dialect string) error {
	if onlineModeCovers(dialect) {
		return nil
	}
	return fmt.Errorf(
		"online mode covers %s; it is refused on %q rather than reporting a guarantee nothing "+
			"here has measured on that engine",
		strings.Join(onlineModeDialects(), ", "), dialect,
	)
}

// OnlineFamily is the identifier prefix of the rules the online mode reports
// under. The apply-time gate blocks on it when the mode is selected.
const OnlineFamily = "ON"

func postgresOnlineRule() Rule {
	rule := Rule{
		Code:     "ON101",
		Title:    "statement not provably online on PostgreSQL",
		Severity: SeverityError,
		Dialects: []string{"postgres"},
		CheckStatement: func(stmt *Statement) (bool, string) {
			if postgresStatementIsOnline(stmt.Words) {
				return false, ""
			}
			return true, "this statement is not in the set measured to take no lock conflicting with reads " +
				"and writes on PostgreSQL; rewrite it into one that is, or apply it in a window where " +
				"blocking is acceptable"
		},
	}
	// A rollback takes the same locks the forward statement did, so the
	// guarantee has to cover both halves or it covers the half nobody runs at
	// three in the morning.
	rule.AppliesToDown = true
	return rule
}

func mysqlOnlineRule() Rule {
	rule := Rule{
		Code:     "ON102",
		Title:    "statement does not ask MySQL to apply it online",
		Severity: SeverityError,
		Dialects: []string{"mysql", "mariadb"},
		CheckStatement: func(stmt *Statement) (bool, string) {
			if mysqlStatementIsOnline(stmt.Words) {
				return false, ""
			}
			return true, "this statement does not ask the server to apply it without blocking writes; " +
				"add ALGORITHM=INPLACE, LOCK=NONE (or ALGORITHM=INSTANT) so the server refuses what it " +
				"cannot do online, or apply it in a window where a table copy is acceptable"
		},
	}
	rule.AppliesToDown = true
	return rule
}

// postgresOnlineTransactionRule reports a transactional file whose statements
// are individually online and together are not.
//
// PostgreSQL holds a lock until the transaction commits, not until the
// statement ends. So an ADD COLUMN that takes ACCESS EXCLUSIVE for an instant
// holds it for the rest of the file, and a VALIDATE CONSTRAINT after it scans
// the whole table with every reader and writer waiting -- each statement
// proven, the migration blocking for the length of the scan. Classifying
// statements one at a time cannot see it, which is why this reads the file.
//
// The remedy is in the message because it is the shape the planner already
// generates: the constraint pair belongs in a migration of its own, marked
// no_transaction, where each statement commits and the validation takes the
// weaker lock it was written for.
func postgresOnlineTransactionRule() Rule {
	return Rule{
		Code:     "ON103",
		Title:    "an online statement's lock is held across a scan",
		Severity: SeverityError,
		Dialects: []string{"postgres"},
		CheckFile: func(file *File) []Finding {
			if (!file.IsUp && !file.IsDown) || file.NoTransaction {
				return nil
			}
			return postgresHeldLockFindings(file)
		},
	}
}

// postgresHeldLockFindings reports each validation that runs behind a lock the
// same transaction already took.
func postgresHeldLockFindings(file *File) []Finding {
	var findings []Finding
	locked := false
	for index, stmt := range file.Statements {
		switch {
		case postgresStatementValidatesAConstraint(stmt.Words):
			if !locked {
				continue
			}
			findings = append(findings, Finding{
				Rule:     "ON103",
				Title:    "an online statement's lock is held across a scan",
				Severity: SeverityError,
				File:     file.Path,
				Line:     stmt.Line,
				Message: "an earlier statement in this migration took an ACCESS EXCLUSIVE lock, and " +
					"PostgreSQL holds it until the transaction commits, so this validation scans the " +
					"table with every reader and writer waiting; put the constraint and its validation " +
					"in a migration of their own marked no_transaction",
				Context: statementFindingContext(index),
			})
		case postgresStatementTakesAccessExclusive(stmt.Words):
			locked = true
		}
	}
	return findings
}

// postgresStatementValidatesAConstraint reports the one allowlisted statement
// that scans the table.
func postgresStatementValidatesAConstraint(words []string) bool {
	return isAlterTable(words) && hasWordSeq(words, "VALIDATE", "CONSTRAINT")
}

// postgresStatementTakesAccessExclusive reports a statement that takes the
// lock PostgreSQL keeps to commit.
//
// Every ALTER TABLE takes it, including the catalog-only forms this mode
// proves: what makes them online is that they hold it for an instant, which
// stops being true once something long-running follows them in the same
// transaction.
func postgresStatementTakesAccessExclusive(words []string) bool {
	return isAlterTable(words) && !postgresStatementValidatesAConstraint(words)
}

// postgresStatementIsOnline reports whether the statement belongs to the set
// measured to take no conflicting lock on PostgreSQL.
//
// The list is short on purpose. Everything it does not name is reported,
// including a statement this function does not recognize at all.
func postgresStatementIsOnline(words []string) bool {
	if len(words) == 0 {
		return true
	}
	switch {
	case isCreateIndex(words), hasWordPrefix(words, "DROP", "INDEX"):
		// A build or a drop holds a lock for its whole duration unless it is
		// concurrent, and a concurrent one holds SHARE UPDATE EXCLUSIVE, which
		// conflicts with neither reads nor writes.
		return slices.Contains(words, "CONCURRENTLY")
	case isAlterTable(words):
		return postgresAlterIsOnline(words)
	case hasWordPrefix(words, "CREATE", "TABLE"),
		hasWordPrefix(words, "CREATE", "SCHEMA"),
		hasWordPrefix(words, "CREATE", "TYPE"),
		hasWordPrefix(words, "COMMENT", "ON"):
		// Nothing that exists is locked: the object is new, or the change is a
		// catalog row about it.
		return true
	default:
		return false
	}
}

// postgresAlterIsOnline decides one ALTER TABLE by the clause it carries.
//
// A statement carrying more than one recognized clause is online only when
// every one of them is, which is why this reads the whole word sequence rather
// than stopping at the first match.
func postgresAlterIsOnline(words []string) bool {
	clauses := postgresAlterClauses(words)
	if len(clauses) == 0 {
		return false
	}
	return !slices.Contains(clauses, false)
}

// postgresAlterClauses answers for each recognized clause of an ALTER TABLE,
// and returns nothing when the statement carries a clause it cannot name --
// which is the unrecognized case, reported rather than assumed safe.
func postgresAlterClauses(words []string) []bool {
	var verdicts []bool
	for index, word := range words {
		verdict, recognized := postgresAlterClauseAt(words, index, word)
		if !recognized {
			continue
		}
		verdicts = append(verdicts, verdict)
	}
	if len(verdicts) == 0 {
		return nil
	}
	return verdicts
}

// postgresAlterClauseAt answers for the clause starting at index, if one does.
//
// The measurements behind each answer are PostgreSQL's documented lock levels
// for the form, confirmed on 18.6 for the two that carry the pair this mode
// generates: ADD CONSTRAINT ... NOT VALID leaves relfilenode unchanged and
// convalidated false, and VALIDATE CONSTRAINT takes ShareUpdateExclusiveLock.
func postgresAlterClauseAt(words []string, index int, word string) (online, recognized bool) {
	switch word {
	case "ADD":
		return postgresAddClauseIsOnline(words, index), true
	case "DROP":
		// DROP COLUMN and DROP CONSTRAINT are catalog edits; the column's data
		// is reclaimed later by VACUUM. What they break is deployed code,
		// which is the BC family's subject and not a lock.
		return postgresWordAfter(words, index, "COLUMN", "CONSTRAINT", "DEFAULT", "NOT"), true
	case "SET":
		// SET DEFAULT is a catalog edit. SET NOT NULL scans the table under
		// ACCESS EXCLUSIVE, and the form that does not is a three-step
		// sequence this mode does not yet generate.
		return postgresWordAfter(words, index, "DEFAULT"), true
	case "VALIDATE":
		return postgresWordAfter(words, index, "CONSTRAINT"), true
	case "ALTER":
		// ALTER COLUMN ... TYPE rewrites the table. The clause words that
		// follow decide, so this only recognizes the ones that are answered
		// elsewhere in this switch.
		return false, postgresAlterColumnRewrites(words, index)
	case "RENAME":
		// A rename is a catalog edit under ACCESS EXCLUSIVE held for the
		// statement alone.
		return true, true
	default:
		return false, false
	}
}

// postgresAddClauseIsOnline answers for ADD COLUMN and ADD CONSTRAINT.
func postgresAddClauseIsOnline(words []string, index int) bool {
	switch {
	case postgresWordAfter(words, index, "COLUMN"):
		return postgresAddColumnIsCatalogOnly(words[index:])
	case postgresWordAfter(words, index, "CONSTRAINT"), postgresWordAfter(words, index, "CHECK"),
		postgresWordAfter(words, index, "FOREIGN"):
		// A catalog edit only when the constraint is one whose cost is a scan
		// AND the statement declines it. A primary key or a unique constraint
		// builds an index under ACCESS EXCLUSIVE, which NOT VALID says nothing
		// about -- PostgreSQL refuses the clause there, and a classifier that
		// read the words alone would call the refused statement online.
		return postgresConstraintDeclinesItsScan(words[index:clauseEnd(words, index)])
	default:
		return false
	}
}

// postgresConstraintDeclinesItsScan reports whether one ADD CONSTRAINT clause
// names a constraint that can arrive unvalidated and does.
//
// It is the one answer to "added NOT VALID": this mode reads it to call the
// clause online, and PG305 and PG306 read it to stay silent about the form
// they recommend (stokaro/ptah#3502). A second predicate would agree with this
// one only until either learned a new spelling. It takes one clause, because a
// NOT VALID later in the statement belongs to another constraint, and it reads
// NOT VALID only outside parentheses, where PostgreSQL's grammar puts the
// attribute; inside them the words are the expression's, as in
// CHECK (NOT valid).
func postgresConstraintDeclinesItsScan(clause []string) bool {
	if !hasTopLevelWordSeq(clause, "NOT", "VALID") {
		return false
	}
	return slices.Contains(clause, "CHECK") || hasWordSeq(clause, "FOREIGN", "KEY")
}

// postgresAddColumnIsCatalogOnly reports whether an ADD COLUMN clause edits
// the catalog and leaves the rows alone.
//
// PostgreSQL 11 and newer store a column's default in the catalog and read it
// for the rows that predate it, so a plain addition with a constant default is
// free. Three shapes are not, and each rewrites every row under ACCESS
// EXCLUSIVE:
//
//   - a constraint, which is the constraint's cost rather than the column's;
//   - GENERATED, whether an identity column or a stored generated one, which
//     computes a value per row;
//   - a default that is not a constant, because a volatile expression has to
//     be evaluated per row. A call is the recognizable shape, so a default
//     carrying one is refused rather than judged: `now()` would be safe and
//     `random()` would not, and this reads the text.
func postgresAddColumnIsCatalogOnly(words []string) bool {
	if hasWordSeq(words, "NOT", "NULL") || hasWordSeq(words, "PRIMARY", "KEY") ||
		slices.Contains(words, "UNIQUE") || slices.Contains(words, "REFERENCES") ||
		slices.Contains(words, "CHECK") || slices.Contains(words, "GENERATED") {
		return false
	}
	return postgresDefaultIsConstant(words)
}

// postgresDefaultIsConstant reports whether the DEFAULT this clause carries,
// if any, is a value rather than something the server has to call.
func postgresDefaultIsConstant(words []string) bool {
	index := slices.Index(words, "DEFAULT")
	if index < 0 {
		return true
	}
	// A call is a name followed by an opening parenthesis, which is what
	// separates `DEFAULT 0` and `DEFAULT 'free'` from `DEFAULT random()`.
	return !slices.Contains(words[index:], "(")
}

// postgresAlterColumnRewrites reports whether an ALTER COLUMN clause names a
// type change, which rewrites the table.
func postgresAlterColumnRewrites(words []string, index int) bool {
	return postgresWordAfter(words, index, "COLUMN") && slices.Contains(words[index:], "TYPE")
}

// postgresWordAfter reports whether any of want follows index, skipping the
// optional keyword PostgreSQL allows between the two.
func postgresWordAfter(words []string, index int, want ...string) bool {
	next := index + 1
	if next >= len(words) {
		return false
	}
	return slices.Contains(want, words[next])
}

// mysqlStatementIsOnline reports whether the statement asks the server to
// apply it without blocking writes.
//
// The MySQL family needs no clause table, which is the point of asking. A
// server that cannot honor ALGORITHM=INPLACE with LOCK=NONE refuses the whole
// statement with 1845 or 1846 -- measured on MySQL 8.4.6 and MariaDB 12.3.3
// over eighteen ALTER TABLE forms -- so the guarantee is the server's rather
// than a model of it.
//
// Statements that lock nothing that exists are online for the same reason they
// are on PostgreSQL.
func mysqlStatementIsOnline(words []string) bool {
	if len(words) == 0 {
		return true
	}
	if !isAlterTable(words) {
		return hasWordPrefix(words, "CREATE", "TABLE") ||
			hasWordPrefix(words, "CREATE", "DATABASE") ||
			hasWordPrefix(words, "CREATE", "SCHEMA")
	}
	algorithm := mysqlClauseValue(words, "ALGORITHM")
	if algorithm == "INSTANT" {
		// INSTANT touches the catalog and nothing else, so it needs no lock
		// level beside it.
		return true
	}
	return algorithm == "INPLACE" && mysqlClauseValue(words, "LOCK") == "NONE"
}

// mysqlClauseValue reads the value of a trailing `NAME=VALUE` clause, upper
// cased, and is empty when the statement carries no such clause.
//
// The words arrive with punctuation as its own entry, so the value sits two
// positions after the name.
func mysqlClauseValue(words []string, name string) string {
	for index, word := range words {
		if word != name || index+2 >= len(words) || words[index+1] != "=" {
			continue
		}
		return strings.ToUpper(words[index+2])
	}
	return ""
}
