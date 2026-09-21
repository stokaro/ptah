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
	return []Rule{postgresOnlineRule(), mysqlOnlineRule()}
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
		// A column addition is a catalog edit on PostgreSQL 11 and newer,
		// including one with a non-volatile default. A column that arrives
		// with a constraint is the constraint's question, not the column's.
		return !hasWordSeq(words[index:], "NOT", "NULL") &&
			!hasWordSeq(words[index:], "PRIMARY", "KEY") &&
			!slices.Contains(words[index:], "UNIQUE") &&
			!slices.Contains(words[index:], "REFERENCES") &&
			!slices.Contains(words[index:], "CHECK")
	case postgresWordAfter(words, index, "CONSTRAINT"), postgresWordAfter(words, index, "CHECK"),
		postgresWordAfter(words, index, "FOREIGN"):
		// A catalog edit only when the constraint is one whose cost is a scan
		// AND the statement declines it. A primary key or a unique constraint
		// builds an index under ACCESS EXCLUSIVE, which NOT VALID says nothing
		// about -- PostgreSQL refuses the clause there, and a classifier that
		// read the words alone would call the refused statement online.
		return postgresConstraintDeclinesItsScan(words[index:])
	default:
		return false
	}
}

// postgresConstraintDeclinesItsScan reports whether an ADD CONSTRAINT clause
// names a constraint that can arrive unvalidated and does.
func postgresConstraintDeclinesItsScan(words []string) bool {
	if !hasWordSeq(words, "NOT", "VALID") {
		return false
	}
	return slices.Contains(words, "CHECK") || hasWordSeq(words, "FOREIGN", "KEY")
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
