package pglint

import (
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
)

func postgresOnlineRule() lintkit.Rule {
	rule := lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "ON101",
			Title:    "statement not provably online on PostgreSQL",
			Severity: lintext.Error,
			Targets:  []string{"postgres"},
		},
		Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
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
func postgresOnlineTransactionRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "ON103",
			Title:    "an online statement's lock is held across a scan",
			Severity: lintext.Error,
			Targets:  []string{"postgres"},
		},
		File: func(file *lintkit.File) []lintext.Violation {
			if (!file.IsUp && !file.IsDown) || file.NoTransaction {
				return nil
			}
			return postgresHeldLockFindings(file)
		},
	}
}

// postgresHeldLockFindings reports each validation that runs behind a lock the
// same transaction already took.
func postgresHeldLockFindings(file *lintkit.File) []lintext.Violation {
	var findings []lintext.Violation
	locked := false
	for index, stmt := range file.Statements {
		switch {
		case postgresStatementValidatesAConstraint(stmt.Words):
			if !locked {
				continue
			}
			findings = append(findings, lintext.Violation{Statement: index, Message: "an earlier statement in this migration took an ACCESS EXCLUSIVE lock, and " +
				"PostgreSQL holds it until the transaction commits, so this validation scans the " +
				"table with every reader and writer waiting; put the constraint and its validation " +
				"in a migration of their own marked no_transaction"})
		case postgresStatementTakesAccessExclusive(stmt.Words):
			locked = true
		}
	}
	return findings
}

// postgresStatementValidatesAConstraint reports the one allowlisted statement
// that scans the table.
func postgresStatementValidatesAConstraint(words []string) bool {
	return lintkit.IsAlterTable(words) && lintkit.HasWordSeq(words, "VALIDATE", "CONSTRAINT")
}

// postgresStatementTakesAccessExclusive reports a statement that takes the
// lock PostgreSQL keeps to commit.
//
// Every ALTER TABLE takes it, including the catalog-only forms this mode
// proves: what makes them online is that they hold it for an instant, which
// stops being true once something long-running follows them in the same
// transaction.
func postgresStatementTakesAccessExclusive(words []string) bool {
	return lintkit.IsAlterTable(words) && !postgresStatementValidatesAConstraint(words)
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
	case lintkit.IsCreateIndex(words), lintkit.HasWordPrefix(words, "DROP", "INDEX"):
		// A build or a drop holds a lock for its whole duration unless it is
		// concurrent, and a concurrent one holds SHARE UPDATE EXCLUSIVE, which
		// conflicts with neither reads nor writes.
		return slices.Contains(words, "CONCURRENTLY")
	case lintkit.IsAlterTable(words):
		return postgresAlterIsOnline(words)
	case lintkit.HasWordPrefix(words, "CREATE", "TABLE"),
		lintkit.HasWordPrefix(words, "CREATE", "SCHEMA"),
		lintkit.HasWordPrefix(words, "CREATE", "TYPE"),
		lintkit.HasWordPrefix(words, "COMMENT", "ON"):
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
		return postgresConstraintDeclinesItsScan(words[index:lintkit.ClauseEnd(words, index)])
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
	return slices.Contains(clause, "CHECK") || lintkit.HasWordSeq(clause, "FOREIGN", "KEY")
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
	if lintkit.HasWordSeq(words, "NOT", "NULL") || lintkit.HasWordSeq(words, "PRIMARY", "KEY") ||
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
