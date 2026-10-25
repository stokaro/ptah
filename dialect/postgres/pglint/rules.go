package pglint

import (
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/txrequire"
)

func definerRoutineWithoutSearchPathRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:          "PG312P",
			Title:         "SECURITY DEFINER routine does not pin search_path",
			Severity:      lintext.Warning,
			Targets:       []string{"postgres"},
			AppliesToDown: true,
		},
		Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
			header, ok := lintkit.RoutineHeaderWords(stmt.Words)
			if !ok || !lintkit.ContainsWord(header, "DEFINER") || lintkit.ContainsWord(header, "SET") {
				return false, ""
			}
			return true, "a SECURITY DEFINER routine resolves unqualified names through the caller's search_path; " +
				"add SET search_path = pg_catalog, pg_temp so the caller cannot decide what it reads"
		},
	}
}

// postgresRules covers the PG family: PostgreSQL-specific hazards.
func postgresRules() []lintkit.Rule {
	return []lintkit.Rule{
		postgresCreateIndexRule(),
		postgresEnumAddValueRule(),
		postgresConcurrentIndexRule(),
		postgresPerChunkIndexInTransactionRule(),
		postgresAddPrimaryKeyRule(),
		postgresAddUniqueConstraintRule(),
		postgresDropIndexRule(),
		postgresColumnAlignmentRule(),
		postgresVolatileDefaultRule(),
		postgresAddCheckRule(),
		postgresAddForeignKeyRule(),
		postgresSetPersistenceRule(),
		postgresCreateTriggerRule(),
		postgresAddStoredGeneratedRule(),
		postgresAddIdentityRule(),
		postgresSetAccessMethodRule(),
	}
}

func postgresCreateIndexRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "PG101",
			Title:    "index built with a table lock",
			Severity: lintext.Warning,
			Targets:  []string{"postgres"},
			Input:    lintext.InputBaselineRefinement,
		},
		BaselineSubjects: postgresLockingIndexBuilds,
		File: func(file *lintkit.File) []lintext.Violation {
			var findings []lintext.Violation
			for _, i := range postgresLockingIndexBuilds(file) {
				stmt := &file.Statements[i]
				findings = append(findings, lintext.Violation{Statement: i, Message: postgresLockingIndexAdvice(file, stmt)})
			}
			return findings
		},
	}
}

// postgresLockingIndexBuilds returns the CREATE INDEX statements PG101
// reports, and it is also the rule's answer to which statements want the
// starting state. One predicate for both, because a statement requested but
// never reported costs a dev-database read for nothing, and one reported but
// never requested is refined on no run at all.
//
// File-level: an index on a table this same migration created is built on an
// empty table, so there is no lock hazard. A CONCURRENTLY build takes no lock
// that blocks writes, and neither does a TimescaleDB per-chunk build on the
// table as a whole; see [txrequire.PerChunkIndexBuild]. Both are refused inside
// a transaction, which PG103 and PG103P report.
func postgresLockingIndexBuilds(file *lintkit.File) []int {
	if !file.IsUp {
		return nil
	}
	var indexes []int
	created := make(map[string]bool)
	for i := range file.Statements {
		stmt := &file.Statements[i]
		if ref := lintkit.CreatedTableRef(stmt.Words); ref != "" {
			created[ref] = true
			continue
		}
		if !lintkit.IsCreateIndex(stmt.Words) || slices.Contains(stmt.Words, "CONCURRENTLY") || txrequire.PerChunkIndexBuild(stmt.Words) {
			continue
		}
		target, only := createIndexTarget(stmt.Words, stmt.SourceWords)
		if lintkit.RefersToCreated(created, target.Normalized) {
			continue
		}
		// ON ONLY on a partitioned table creates the parent's index alone,
		// invalid until each partition's index is attached, and reads no row:
		// measured on PostgreSQL 18.6, 11 ms beside a 5.3 s full build, with an
		// INSERT during it taking 8 ms. It is the first step of the remedy PG108
		// names, so reporting it would flag the advice.
		if only && file.Baseline.Partitioned(target.Normalized) {
			continue
		}
		indexes = append(indexes, i)
	}
	return indexes
}

// postgresLockingIndexAdvice is PG101's message for one statement: the
// ordinary advice, or the hypertable advice when the starting state says the
// table is one.
//
// Measured on TimescaleDB 2.30.1 over PostgreSQL 18.6, on a hypertable of three
// populated chunks:
//
//	CREATE INDEX CONCURRENTLY                refused: hypertables do not support
//	                                         concurrent index creation
//	CREATE INDEX                             SHARE on the hypertable and on every
//	                                         chunk for the whole build; an INSERT
//	                                         waits
//	CREATE INDEX ... WITH                    SHARE on one chunk at a time; an
//	  (timescaledb.transaction_per_chunk)    INSERT into another chunk, or into a
//	                                         new one, proceeds; refused inside a
//	                                         transaction block
//	the per-chunk build, canceled            the hypertable's index stays with
//	                                         indisvalid = false and the chunks
//	                                         not yet built have none; a rerun
//	                                         with IF NOT EXISTS skips it
func postgresLockingIndexAdvice(file *lintkit.File, stmt *lintext.Statement) string {
	target, _ := createIndexTarget(stmt.Words, stmt.SourceWords)
	if !file.Baseline.Hypertable(target.Normalized) {
		return "CREATE INDEX without CONCURRENTLY blocks writes to the table for the whole build; " +
			"on a populated table use CREATE INDEX CONCURRENTLY outside a transaction"
	}
	return "CREATE INDEX on the hypertable " + target.Name + " blocks writes to it and to every chunk for the whole " +
		"build, and TimescaleDB refuses CONCURRENTLY on a hypertable; " +
		"WITH (timescaledb.transaction_per_chunk) in a no_transaction migration builds one chunk at a time, so only " +
		"writes to the chunk being indexed wait. A per-chunk build that is interrupted leaves the index invalid, and " +
		"IF NOT EXISTS then skips it, so drop the index before you retry"
}

// createIndexTarget is the table a CREATE INDEX statement builds on, in the
// source spelling and the normalized one, and whether the statement names it
// ON ONLY, which on a partitioned table builds the parent's index alone.
//
// PG101 and PG108 both read the target, and they must place it the same way:
// a statement one of them resolved and the other did not is reported twice, or
// not at all. The word after ON is not the table in either of these spellings:
// it is `ONLY` in `ON ONLY m` and `PUBLIC` in `ON public.m`.
func createIndexTarget(w, sourceWords []string) (lintkit.TableReference, bool) {
	for k := range w {
		if w[k] != "ON" {
			continue
		}
		start := k + 1
		only := start < len(w) && w[start] == "ONLY"
		if only {
			start++
		}
		ref, _ := lintkit.TableRefAt(w, sourceWords, start)
		return ref, only
	}
	return lintkit.TableReference{}, false
}

func postgresEnumAddValueRule() lintkit.Rule {
	return postgresStatementRule("PG102", "enum value added inside a transaction", func(stmt *lintext.Statement) (bool, string) {
		if !lintkit.HasWordPrefix(stmt.Words, "ALTER", "TYPE") || !lintkit.HasWordSeq(stmt.Words, "ADD", "VALUE") {
			return false, ""
		}
		return true, "ALTER TYPE ... ADD VALUE cannot run inside a transaction block before PostgreSQL 12, and the new value stays unusable within the same transaction on newer versions; run it in its own non-transactional migration"
	})
}

func postgresConcurrentIndexRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "PG103",
			Title:    "concurrent index operation in a transactional migration",
			Severity: lintext.Warning,
			Targets:  []string{"postgres"},
		},
		File: func(file *lintkit.File) []lintext.Violation {
			if (!file.IsUp && !file.IsDown) || file.NoTransaction || runsAlone(file) {
				return nil
			}
			var findings []lintext.Violation
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if !scanConcurrentIndexOperation(stmt.Words) {
					continue
				}
				findings = append(findings, lintext.Violation{Statement: i, Message: "CONCURRENTLY cannot run inside PostgreSQL's normal migration transaction; mark the migration non-transactional before applying"})
			}
			return findings
		},
	}
}

// runsAlone reports a file of one statement whose runner opens no
// transaction around a file. Such a runner sends the statement on its own, and
// PostgreSQL runs a statement sent alone outside a transaction, so a statement
// it refuses inside one runs: measured with golang-migrate on PostgreSQL, a file
// holding only CREATE INDEX CONCURRENTLY applies (stokaro/ptah#4962). A second
// statement makes the file one implicit transaction again, which TX101 reports.
func runsAlone(file *lintkit.File) bool {
	if file.TransactionOwner != lintext.TransactionOwnerFile {
		return false
	}
	statements := 0
	for i := range file.Statements {
		if len(file.Statements[i].Words) > 0 {
			statements++
		}
	}
	return statements == 1
}

// postgresPerChunkIndexInTransactionRule is PG103 for TimescaleDB's per-chunk
// index build: WITH (timescaledb.transaction_per_chunk) commits one transaction
// per chunk, so TimescaleDB refuses it inside a transaction block, the same way
// PostgreSQL refuses CONCURRENTLY (SQLSTATE 25001, measured on TimescaleDB
// 2.30.1 over PostgreSQL 18.6).
//
// It is a rule of its own rather than a wider PG103 because PG103 reports the
// Atlas check for a concurrent operation without the txmode header, and a
// per-chunk build is not a concurrent operation. Folding it in would put a
// TimescaleDB statement under an Atlas identifier that does not describe it,
// and `atlas:nolint PG103` would silence both.
//
// The recognition is [txrequire.PerChunkIndexBuild], the predicate the
// migrator's transaction preflight uses, so a file this reports is the file
// `migrations up` refuses, and a statement PG101 leaves alone as the per-chunk
// remedy is the statement this reports when it sits in a transaction.
func postgresPerChunkIndexInTransactionRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "PG103P",
			Title:    "per-chunk index build in a transactional migration",
			Severity: lintext.Warning,
			Targets:  []string{"postgres"},
		},
		File: func(file *lintkit.File) []lintext.Violation {
			if (!file.IsUp && !file.IsDown) || file.NoTransaction || runsAlone(file) {
				return nil
			}
			var findings []lintext.Violation
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if !txrequire.PerChunkIndexBuild(stmt.Words) {
					continue
				}
				findings = append(findings, lintext.Violation{Statement: i, Message: "WITH (timescaledb.transaction_per_chunk) commits one transaction per chunk, so TimescaleDB " +
					"refuses it inside the migration's transaction; mark the migration non-transactional before applying"})
			}
			return findings
		},
	}
}

func postgresAddPrimaryKeyRule() lintkit.Rule {
	return postgresAlterRule("PG104", "primary key added with an access-exclusive lock", lintkit.ScanAddPrimaryKey,
		"adding a primary key takes an ACCESS EXCLUSIVE lock and can scan existing rows; build a supporting unique index concurrently first, then attach it")
}

func postgresAddUniqueConstraintRule() lintkit.Rule {
	return postgresAlterRule("PG105", "unique constraint added with an access-exclusive lock", scanAddUniqueConstraint,
		"adding a unique constraint takes an ACCESS EXCLUSIVE lock and validates existing rows; build a unique index concurrently first when the table is populated")
}

// postgresDropIndexRule reports a blocking index drop in either direction.
//
// The down half is where this rule earns its keep: the statement that removes
// an index is what a rollback file is normally made of, and PostgreSQL takes
// the same ACCESS EXCLUSIVE lock whichever direction asked for it. Confining
// statement rules to up files left a rollback that blocks writes on a populated
// table reported as clean -- including one Ptah generates itself, whenever the
// forward statement was not a concurrent build.
func postgresDropIndexRule() lintkit.Rule {
	rule := postgresStatementRule("PG106", "index dropped with a table lock", func(stmt *lintext.Statement) (bool, string) {
		if !lintkit.HasWordPrefix(stmt.Words, "DROP", "INDEX") || slices.Contains(stmt.Words, "CONCURRENTLY") {
			return false, ""
		}
		return true, "DROP INDEX without CONCURRENTLY blocks writes while PostgreSQL removes the index; use DROP INDEX CONCURRENTLY outside a transaction for populated tables"
	})
	rule.AppliesToDown = true
	return rule
}

func postgresColumnAlignmentRule() lintkit.Rule {
	return postgresStatementRule("PG110", "non-optimal column alignment", func(stmt *lintext.Statement) (bool, string) {
		if !scanCreateTableMixedAlignment(stmt.Words) {
			return false, ""
		}
		return true, "this PostgreSQL column order can waste tuple padding; place wider fixed-size columns before narrow columns when creating large tables"
	})
}

func postgresVolatileDefaultRule() lintkit.Rule {
	return postgresAlterRule("PG302", "volatile default rewrites existing rows", scanAddColumnWithVolatileDefault,
		"adding a column with a volatile DEFAULT rewrites or evaluates every existing row; add the column first, backfill in batches, then set the default")
}

// PG305 and PG306 advise adding the constraint NOT VALID and validating it
// separately, so the form they recommend is not a finding: each reports only
// a clause that validates the existing rows as it adds the constraint
// (stokaro/ptah#3502). Measured on PostgreSQL 18.6 and 13.23 over a table with
// one violating row: ADD CONSTRAINT ... NOT VALID accepts it, leaves
// convalidated false and rewrites nothing, and VALIDATE CONSTRAINT then takes
// SHARE UPDATE EXCLUSIVE on the table (and ROW SHARE on a foreign key's
// referenced table) and lets writes to both through, where the same ADD
// without NOT VALID refuses the violating row and blocks writes to the table,
// and for a foreign key to the referenced table as well.
func postgresAddCheckRule() lintkit.Rule {
	return postgresAlterRule("PG305", "check constraint validates existing rows", scanAddValidatedCheck,
		"adding a CHECK constraint validates existing rows and can hold locks; add it NOT VALID first, then validate separately")
}

func postgresAddForeignKeyRule() lintkit.Rule {
	return postgresAlterRule("PG306", "foreign key validates existing rows", scanAddValidatedForeignKey,
		"adding a foreign key validates existing rows and can block writes on both tables; add it NOT VALID first, then validate separately")
}

func postgresSetPersistenceRule() lintkit.Rule {
	return postgresAlterRule("PG307", "table persistence changed", scanSetTablePersistence,
		"changing LOGGED/UNLOGGED rewrites the table and takes heavyweight locks; schedule it as a maintenance operation")
}

func postgresCreateTriggerRule() lintkit.Rule {
	return postgresStatementRule("PG308", "trigger added with a write-blocking lock", func(stmt *lintext.Statement) (bool, string) {
		if !lintkit.HasWordPrefix(stmt.Words, "CREATE", "TRIGGER") {
			return false, ""
		}
		return true, "CREATE TRIGGER takes a SHARE ROW EXCLUSIVE lock and can block concurrent writes; deploy during a quiet window on hot tables"
	})
}

func postgresAddStoredGeneratedRule() lintkit.Rule {
	return postgresAlterRule("PG309", "stored generated column rewrites rows", lintkit.ScanAddStoredGeneratedColumn,
		"adding a STORED generated column computes and stores a value for every existing row; plan the rewrite and lock impact")
}

func postgresAddIdentityRule() lintkit.Rule {
	return postgresAlterRule("PG310", "identity column rewrites rows", scanAddIdentityColumn,
		"adding an identity column can rewrite existing rows and requires sequence ownership changes; use a staged nullable column path on populated tables")
}

func postgresSetAccessMethodRule() lintkit.Rule {
	return postgresAlterRule("PG311", "table access method changed", scanSetAccessMethod,
		"changing a table's access method rewrites the table; schedule it as a maintenance operation")
}

func postgresAlterRule(code, title string, scan func([]string) bool, message string) lintkit.Rule {
	return postgresStatementRule(code, title, func(stmt *lintext.Statement) (bool, string) {
		if !lintkit.IsAlterTable(stmt.Words) || !scan(stmt.Words) {
			return false, ""
		}
		return true, message
	})
}

func postgresStatementRule(code, title string, check func(*lintext.Statement) (bool, string)) lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     code,
			Title:    title,
			Severity: lintext.Warning,
			Targets:  []string{"postgres"},
		},
		Statement: func(_ *lintkit.File, statement *lintext.Statement) (bool, string) { return check(statement) },
	}
}

// hasTopLevelWordSeq is hasWordSeq outside every pair of parentheses. A
// clause's own keywords sit at the top level; the same words inside a
// parenthesized expression are the expression's, as in CHECK (NOT valid) over
// a boolean column named valid.
func hasTopLevelWordSeq(w []string, seq ...string) bool {
	depth := 0
	for i, word := range w {
		switch word {
		case "(":
			depth++
		case ")":
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && lintkit.HasWordPrefix(w[i:], seq...) {
				return true
			}
		}
	}
	return false
}

func scanAddColumnWithVolatileDefault(w []string) bool {
	for _, i := range lintkit.ClauseStarts(w) {
		start, end, ok := lintkit.AddColumnClause(w, i)
		if !ok {
			continue
		}
		clause := w[start:end]
		for j := range clause {
			if clause[j] == "DEFAULT" && j+1 < len(clause) && isVolatilePostgresDefault(clause[j+1]) {
				return true
			}
		}
	}
	return false
}

func scanAddIdentityColumn(w []string) bool {
	for _, i := range lintkit.ClauseStarts(w) {
		start, end, ok := lintkit.AddColumnClause(w, i)
		if ok && lintkit.HasWordSeq(w[start:end], "GENERATED", "ALWAYS", "AS", "IDENTITY") {
			return true
		}
	}
	return false
}

func isVolatilePostgresDefault(word string) bool {
	switch strings.Trim(word, "\"`") {
	case "GEN_RANDOM_UUID", "UUID_GENERATE_V4", "NOW", "CLOCK_TIMESTAMP", "RANDOM":
		return true
	default:
		return false
	}
}

func scanConcurrentIndexOperation(w []string) bool {
	if lintkit.IsCreateIndex(w) {
		return slices.Contains(w, "CONCURRENTLY")
	}
	return lintkit.HasWordPrefix(w, "DROP", "INDEX") && slices.Contains(w, "CONCURRENTLY")
}

func scanAddUniqueConstraint(w []string) bool {
	for _, i := range lintkit.ClauseStarts(w) {
		clause := w[i:lintkit.ClauseEnd(w, i)]
		if lintkit.HasWordPrefix(clause, "ADD", "UNIQUE") ||
			lintkit.HasWordPrefix(clause, "ADD", "CONSTRAINT") && slices.Contains(clause, "UNIQUE") {
			return true
		}
	}
	return false
}

// scanAddValidatedCheck is scanAddCheckConstraint for PostgreSQL, where a
// constraint added NOT VALID does not read the existing rows. A statement
// adding two checks, one of them NOT VALID, still validates the other.
func scanAddValidatedCheck(w []string) bool {
	return lintkit.AnyAlterClause(w, func(clause []string) bool {
		return lintkit.AddsCheckConstraint(clause) && !postgresConstraintDeclinesItsScan(clause)
	})
}

// scanAddValidatedForeignKey is scanAddValidatedCheck for a foreign key.
func scanAddValidatedForeignKey(w []string) bool {
	return lintkit.AnyAlterClause(w, func(clause []string) bool {
		return lintkit.AddsForeignKey(clause) && !postgresConstraintDeclinesItsScan(clause)
	})
}

func scanSetTablePersistence(w []string) bool {
	for _, i := range lintkit.ClauseStarts(w) {
		if lintkit.HasWordPrefix(w[i:], "SET", "LOGGED") || lintkit.HasWordPrefix(w[i:], "SET", "UNLOGGED") {
			return true
		}
	}
	return false
}

func scanSetAccessMethod(w []string) bool {
	for _, i := range lintkit.ClauseStarts(w) {
		if lintkit.HasWordPrefix(w[i:], "SET", "ACCESS", "METHOD") {
			return true
		}
	}
	return false
}

func scanCreateTableMixedAlignment(w []string) bool {
	if !lintkit.HasWordPrefix(w, "CREATE", "TABLE") {
		return false
	}
	open := slices.Index(w, "(")
	closeIndex := len(w) - 1
	for closeIndex > open && w[closeIndex] != ")" {
		closeIndex--
	}
	if open < 0 || closeIndex <= open {
		return false
	}
	seenNarrow := false
	for i := open + 1; i < closeIndex; i++ {
		if w[i] == "," {
			continue
		}
		if !lintkit.IdentLike(w[i]) || i+1 >= closeIndex {
			continue
		}
		width := postgresTypeWidthClass(w[i+1])
		if width == 0 {
			continue
		}
		if width == 1 {
			seenNarrow = true
			continue
		}
		if seenNarrow && width > 1 {
			return true
		}
	}
	return false
}

func postgresTypeWidthClass(word string) int {
	switch strings.Trim(word, "\"`") {
	case "BOOL", "BOOLEAN", "CHAR", "SMALLINT", "INT2":
		return 1
	case "BIGINT", "INT8", "BIGSERIAL", "DOUBLE", "TIMESTAMP", "TIMESTAMPTZ", "UUID":
		return 2
	default:
		return 0
	}
}
