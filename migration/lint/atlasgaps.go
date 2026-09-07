package lint

import (
	"slices"
	"strings"
)

// The rules in this file close the gaps a refresh of the Atlas analyzer
// comparison found (stokaro/ptah#2972). Each one reports a consequence no
// existing rule named: a broad warning that fires on the same statement is not
// coverage, so where an existing rule already fired -- MY101 on an ALTER TABLE
// form, DS101 on a DROP TABLE, PG101 on a CREATE INDEX -- these say the thing
// that rule does not.
//
// Two consequences are kept apart throughout, because collapsing them is how a
// finding stops being actionable:
//
//   - what the statement costs: metadata edit, in-place rebuild, or table copy;
//   - whether writes continue while it runs.
//
// They are independent. MY141 rebuilds in place AND blocks writes; MY147
// rebuilds in place and lets them through; PG320 takes a light lock and is
// still a hazard, for a reason that is not cost at all.
//
// MYSQL AND MARIADB, MEASURED
//
// Every MySQL claim below was measured on MySQL 8.4.11 and MariaDB 11.8.9 by
// asking for ALGORITHM=INSTANT, then INPLACE, then COPY, and for the in-place
// forms also LOCK=NONE. That is the same method mysqlcost.go records for the
// rules it owns. Three answers are kept apart, because conflating them invents
// a measurement: the server accepted the algorithm, the server refused it for
// this operation (1845/1846), or the clause is not accepted in that statement
// form at all (1064), which means no online-DDL negotiation exists for it.
//
//	                                         INSTANT   INPLACE   COPY   LOCK=NONE
//	DROP PRIMARY KEY, ADD PRIMARY KEY (…)     refused   ok        ok     ok      MY137
//	ENGINE=…                                  refused   refused   ok     refused MY138
//	PARTITION BY … / REMOVE PARTITIONING      syntax    syntax    syntax syntax  MY139
//	ADD COLUMN … GENERATED … STORED           refused   refused   ok     refused MY140
//	ADD COLUMN … AUTO_INCREMENT               refused   ok        ok     refused MY141
//	MODIFY … GENERATED … STORED               refused   refused   ok     refused MY143
//	ADD CONSTRAINT … CHECK (…)                refused   refused   ok     refused MY144
//	ALTER CONSTRAINT … ENFORCED               refused   refused   ok     refused MY145 (MySQL only)
//	DROP SYSTEM VERSIONING                    refused   ok        ok     refused MY146 (MariaDB only)
//	MODIFY … NULL -> NOT NULL                 refused   ok        ok     ok      MY147
//
// Both servers agreed on every row above except where the row says otherwise.
// Two divergences are not cost differences but missing syntax, and each is why
// its rule is restricted to one dialect rather than assumed to generalize:
//
//   - MariaDB has no ENFORCED / NOT ENFORCED at all. `CHECK (…) NOT ENFORCED`
//     and `ALTER CONSTRAINT … ENFORCED` are both ERROR 1064 there, so MY145 is
//     a MySQL rule.
//   - MySQL 8.4 has no system versioning at all. `WITH SYSTEM VERSIONING` is
//     ERROR 1064 there, so MY146 is a MariaDB rule.
//
// MY142 IS NOT IMPLEMENTED, AND THAT IS A MEASUREMENT
//
// Atlas reports that adding a column before existing ones prevents an instant
// operation on older versions. Measured, `ADD COLUMN … FIRST` accepts
// ALGORITHM=INSTANT on MySQL 8.4.11 and on MySQL 8.0.46, with a plain
// `ADD COLUMN` at the end as the control accepting it on both. The hazard is a
// pre-8.0.29 behavior, and the lowest MySQL release line this repository
// declares in internal/capabilityprobe/cells.go is 8.4. A rule firing on every
// `FIRST` or `AFTER` would therefore be a false positive on every line Ptah
// tests, so the catalog records the check as not implemented with that
// measurement rather than carrying a warning nobody should act on.
//
// POSTGRESQL, MEASURED
//
// Every PostgreSQL claim below was measured on PostgreSQL 18.6 by running the
// statement inside a transaction and reading pg_locks for the relation while
// the transaction still held it. A committed statement has released its locks,
// so asking afterwards reports nothing and reads exactly like a lock-free
// operation.
//
//	CREATE INDEX on a partitioned parent   ShareLock on the parent AND on every
//	                                       partition                      PG108
//	ADD CONSTRAINT … EXCLUDE               AccessExclusiveLock            PG109
//	DROP CONSTRAINT pkey, ADD PRIMARY KEY  AccessExclusiveLock            PG312
//	REPLICA IDENTITY FULL                  AccessExclusiveLock            PG314
//	SET (autovacuum_enabled = false)       ShareUpdateExclusiveLock       PG320
//
// PG108 carries one more measurement, and it is the reason the rule exists
// beside PG101 rather than being folded into it: `CREATE INDEX CONCURRENTLY`
// on a partitioned table is refused outright -- `ERROR: cannot create index on
// partitioned table "p" concurrently`. PG101's remedy is to add CONCURRENTLY,
// which cannot be done here, so a reader who follows it writes a statement the
// server rejects.
//
// PG320 is the one rule here whose hazard is not a lock or a rewrite. The
// statement is cheap; what it costs is paid later, by the rows nothing
// reclaims.

// atlasGapGeneratedStored reports a STORED generated column in a MySQL MODIFY
// or CHANGE clause.
//
// It is separate from scanAddStoredGeneratedColumn, which answers for the ADD
// side: modifying an existing generated column and adding one are different
// statements with the same cost, and MY143 must not fire on the ADD that MY140
// already reports.
func atlasGapGeneratedStored(w []string) bool {
	if !isAlterTable(w) {
		return false
	}
	if !hasWordSeq(w, "GENERATED", "ALWAYS", "AS") && !hasWordSeq(w, "AS") {
		return false
	}
	if !hasWordSeq(w, "STORED") {
		return false
	}
	return hasWordSeq(w, "MODIFY") || hasWordSeq(w, "CHANGE")
}

// atlasGapAutoIncrementColumn reports an ADD COLUMN carrying AUTO_INCREMENT.
//
// The keyword has to be an attribute of the added column rather than its name,
// so the scan starts at the clause head and steps over the name: `ADD COLUMN
// auto_increment INT` adds an ordinary column that happens to be named after
// the keyword, and reporting a rebuild for it is a false claim about a cheap
// statement.
func atlasGapAutoIncrementColumn(w []string) bool {
	if !isAlterTable(w) {
		return false
	}
	for _, start := range clauseStarts(w) {
		j := start
		if j >= len(w) || w[j] != "ADD" {
			continue
		}
		j++
		if j < len(w) && w[j] == "COLUMN" {
			j++
		}
		j = skipIfExists(w, j)
		if j >= len(w) || !identLike(w[j]) {
			continue
		}
		if clauseHasWord(w, j+1, "AUTO_INCREMENT") {
			return true
		}
	}
	return false
}

// clauseHasWord reports whether one word appears in the clause starting at
// from, stopping at the next top-level clause head.
//
// Scanning the whole statement instead is how a keyword in one clause was read
// as belonging to another: `ADD COLUMN engine VARCHAR(10), DROP COLUMN note`
// has no table option in it at all.
func clauseHasWord(w []string, from int, word string) bool {
	for _, start := range clauseStarts(w) {
		if start > from && start <= len(w) {
			return slices.Contains(w[from:start], word)
		}
	}
	if from >= len(w) {
		return false
	}
	return slices.Contains(w[from:], word)
}

// atlasGapStorageEngine reports an ENGINE= clause on an ALTER TABLE.
//
// `ENGINE=InnoDB` on a table that is already InnoDB is the documented way to
// force a rebuild, so the clause is reported whether or not the engine name
// changes: the cost is the same, and the statement alone cannot tell the two
// apart without the current engine from schema state.
func atlasGapStorageEngine(w []string) bool {
	return isAlterTable(w) && clauseHeadIs(w, "ENGINE")
}

// clauseHeadIs reports whether any top-level clause of an ALTER TABLE begins
// with word.
//
// A table option is a clause of its own, so anchoring here is what separates it
// from a column that carries the same name: `ADD COLUMN engine VARCHAR(10)` is
// a clause headed by ADD.
func clauseHeadIs(w []string, word string) bool {
	for _, start := range clauseStarts(w) {
		if start < len(w) && w[start] == word {
			return true
		}
	}
	return false
}

// atlasGapPartitioning reports a partitioning change.
func atlasGapPartitioning(w []string) bool {
	if !isAlterTable(w) {
		return false
	}
	return hasWordSeq(w, "PARTITION", "BY") || hasWordSeq(w, "REMOVE", "PARTITIONING")
}

// atlasGapCheckEnforcement reports a CHECK constraint being enforced or
// unenforced. MySQL only; MariaDB has no such syntax.
//
// ENFORCED has to sit in a clause that alters or adds a constraint. It is not a
// reserved word, so `ADD COLUMN enforced BOOLEAN` is an ordinary column add and
// the keyword there says nothing about any constraint.
func atlasGapCheckEnforcement(w []string) bool {
	if !isAlterTable(w) {
		return false
	}
	for _, start := range clauseStarts(w) {
		// ALTER only. An ADD that spells ENFORCED is stating the default for
		// a constraint being created, and the validation it costs is the one
		// MY144 already reports; the transition this rule is about is turning
		// enforcement on for a constraint that already exists.
		if start >= len(w) || w[start] != "ALTER" {
			continue
		}
		// The enforcement state closes the clause -- `... CHECK (…) NOT
		// ENFORCED`, `ALTER CHECK c ENFORCED` -- so anywhere else in it the
		// word is a name. `ADD CONSTRAINT enforced CHECK (total > 0)` names a
		// constraint and enforces nothing.
		//
		// Only the transition TO enforced costs anything. Turning enforcement
		// off validates nothing, so NOT ENFORCED is a metadata edit and the
		// pair is what separates the two.
		if !clauseEndsWith(w, start, "ENFORCED") || clauseEndsWithPair(w, start, "NOT", "ENFORCED") {
			continue
		}
		if clauseHasWord(w, start, "CONSTRAINT") || clauseHasWord(w, start, "CHECK") {
			return true
		}
	}
	return false
}

// clauseEndsWith reports whether the clause starting at from has word as its
// last word.
func clauseEndsWith(w []string, from int, word string) bool {
	end := len(w)
	for _, start := range clauseStarts(w) {
		if start > from {
			end = start
			break
		}
	}
	return end > from && w[end-1] == word
}

// atlasGapDropSystemVersioning reports MariaDB's DROP SYSTEM VERSIONING.
func atlasGapDropSystemVersioning(w []string) bool {
	return isAlterTable(w) && hasWordSeq(w, "DROP", "SYSTEM", "VERSIONING")
}

// atlasGapRedefinesPrimaryKey reports one statement that drops a primary key
// and adds another.
//
// Both halves in one statement is the point: dropping alone is MY133 and
// CD103's subject, adding alone is MY132's and PG104's, and the redefinition
// costs what neither of those reports on its own.
func atlasGapRedefinesPrimaryKey(w []string) bool {
	if !isAlterTable(w) {
		return false
	}
	dropped := scanDropPrimaryKey(w) || scanDropClause(w, "DROP", "CONSTRAINT")
	return dropped && scanAddPrimaryKey(w)
}

// atlasGapExcludeConstraint reports an EXCLUDE constraint being added.
func atlasGapExcludeConstraint(w []string) bool {
	return isAlterTable(w) && hasWordSeq(w, "EXCLUDE", "USING")
}

// atlasGapReplicaIdentity reports REPLICA IDENTITY being set to FULL or
// NOTHING.
//
// DEFAULT and USING INDEX are not reported: they keep a row identity logical
// replication can use, which is the property the hazard is about.
func atlasGapReplicaIdentity(w []string) bool {
	if !isAlterTable(w) || !hasWordSeq(w, "REPLICA", "IDENTITY") {
		return false
	}
	return hasWordSeq(w, "REPLICA", "IDENTITY", "FULL") ||
		hasWordSeq(w, "REPLICA", "IDENTITY", "NOTHING")
}

// atlasGapDisablesAutovacuum reports autovacuum being turned off for a table.
//
// It reads the joined statement rather than the word list because
// `autovacuum_enabled=false`, `autovacuum_enabled = false` and
// `autovacuum_enabled =false` tokenize differently and mean the same thing.
func atlasGapDisablesAutovacuum(canonical string) bool {
	text := strings.ToUpper(canonical)
	if !strings.Contains(text, "AUTOVACUUM_ENABLED") {
		return false
	}
	compact := strings.NewReplacer(" ", "", "\t", "").Replace(text)
	return strings.Contains(compact, "AUTOVACUUM_ENABLED=FALSE") ||
		strings.Contains(compact, "AUTOVACUUM_ENABLED=OFF") ||
		strings.Contains(compact, "AUTOVACUUM_ENABLED='FALSE'") ||
		strings.Contains(compact, "AUTOVACUUM_ENABLED='OFF'") ||
		strings.Contains(compact, "AUTOVACUUM_ENABLED=0")
}

// atlasGapNullabilityTransitions finds every MODIFY or CHANGE that takes a
// column from nullable to NOT NULL, according to the schema state the version
// starts from.
//
// The baseline is what makes this a transition rather than a shape. `MODIFY
// total BIGINT NOT NULL` on a column that was already NOT NULL restates the
// nullability and changes only the type, and reporting a rebuild there would
// be a cost claim about a change that did not happen -- caught by the MY130
// row of TestAtlasRowsNameTheRulesThatFire, whose fixture is exactly that.
//
// Without a baseline the rule finds nothing and says so through
// [Analysis.UnmetInputs], rather than reporting every NOT NULL it sees.
func atlasGapNullabilityTransitions(file *File) []columnChangeSite {
	var sites []columnChangeSite
	for _, site := range columnChangeSites(file) {
		stmt := &file.Statements[site.statement]
		if !clauseDeclaresNotNull(stmt.Words, site.typeStart) {
			continue
		}
		before, found := file.baseline.column(site.table.normalized, normalizeIdent(site.oldName))
		if !found || before.NotNull {
			continue
		}
		sites = append(sites, site)
	}
	return sites
}

// clauseDeclaresNotNull reports whether the clause beginning at the column's
// type spells NOT NULL before the next clause starts.
//
// Bounded by the clause rather than by the statement: `MODIFY a INT, MODIFY b
// INT NOT NULL` declares it for one column and not the other, and a scan over
// the whole word list would answer for both.
func clauseDeclaresNotNull(w []string, typeStart int) bool {
	end := len(w)
	for _, start := range clauseStarts(w) {
		if start > typeStart && start < end {
			end = start
		}
	}
	for i := typeStart; i+1 < end; i++ {
		if w[i] == "NOT" && w[i+1] == "NULL" {
			return true
		}
	}
	return false
}

// atlasGapNullabilityStatements is the BaselineSubjects answer for MY147: the
// statements whose analysis would say more with the starting schema state.
func atlasGapNullabilityStatements(file *File) []int {
	return columnChangeStatements(file)
}

// atlasGapRules returns the rules that close the comparison gaps.
func atlasGapRules() []Rule {
	return append(atlasGapCompatibilityRules(), append(atlasGapPostgresRules(), atlasGapMySQLRules()...)...)
}

// atlasGapCompatibilityRules holds BC103 and BC104.
//
// Both are file-level so they inherit the exemption DS101 and BC101 already
// apply: an object this migration itself created was never visible to a
// deployed application version, so retiring it breaks no client. The
// destructive rules report the same statements for the other consequence, and
// the two stay separate because the unit of suppression is the rule and its
// family -- accepting the data loss in a drop is not accepting a rollout
// break, and a config that silences one must not silence the other.
//
// Both are scoped to the native surface. This is a deliberate divergence and
// the reason is the surface, not the hazard: on the compatibility surface a
// drop is already reported as destructive, and adding a second diagnostic
// there would change the exit code of a migration directory that passes today.
// Ptah's compatibility rows for a dropped column are measured behavior a
// ported pipeline reads, so the fuller answer lives where a reader asked for
// Ptah's own -- `ptah migrations lint`. [tableRenamedFindings] scopes the
// mirror-image case for the same reason.
func atlasGapCompatibilityRules() []Rule {
	return []Rule{atlasGapDroppedTableRule(), atlasGapDroppedColumnRule()}
}

// atlasGapReportsOnThisSurface reports whether the rollout-break rules run for
// this file.
//
// One predicate for both rules: two checks would agree until one of them was
// changed, and a rule that leaked onto the compatibility surface would show up
// as an exit code, not as a wrong message.
func atlasGapReportsOnThisSurface(file *File) bool {
	return file.IsUp && file.compatibility != CompatibilityProfileAtlas
}

// atlasGapDroppedTableRule reports BC103: a dropped table retires a name
// deployed clients still query.
func atlasGapDroppedTableRule() Rule {
	return Rule{
		Code:     "BC103",
		Title:    "dropped table breaks deployed code",
		Severity: SeverityWarning,
		CheckFile: func(file *File) []Finding {
			if !atlasGapReportsOnThisSurface(file) {
				return nil
			}
			var findings []Finding
			created := make(map[string]bool)
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if ref := createdTableRef(stmt.Words); ref != "" {
					created[ref] = true
					continue
				}
				findings = append(findings, atlasGapDroppedTableFindings(file, stmt, i, created)...)
			}
			return findings
		},
	}
}

// atlasGapDroppedTableFindings reports one finding per table a DROP TABLE
// retires.
//
// Every decision here is DS101's, taken from DS101's own implementation rather
// than reproduced: the target extraction, the create-then-drop exemption, the
// logical-name ordering, and the subject-less fail-closed finding for a target
// list that could not be read to the end. The two rules report the same
// statements for different consequences, so a statement one of them recognizes
// and the other does not would be a silent hole in whichever ran second.
func atlasGapDroppedTableFindings(file *File, stmt *Statement, index int, created map[string]bool) []Finding {
	if !hasWordPrefix(stmt.Words, "DROP", "TABLE") {
		return nil
	}
	tables, complete := droppedTablesNotCreated(stmt.Words, stmt.sourceWords, created)
	if complete && len(tables) == 0 {
		return nil
	}
	finding := func(message string, subjects ...Subject) Finding {
		return Finding{
			Rule:     "BC103",
			Title:    "dropped table breaks deployed code",
			Severity: SeverityWarning,
			File:     file.Path,
			Line:     stmt.Line,
			Message:  message,
			Context:  statementFindingContext(index, subjects...),
		}
	}
	if len(tables) == 0 {
		return []Finding{finding("dropping the table retires a name " + atlasGapDroppedTableAdvice)}
	}
	ordered := orderedDropTargets(tables)
	findings := make([]Finding, 0, len(ordered))
	for _, table := range ordered {
		findings = append(findings, finding(
			"dropping table "+table.name+" retires a name "+atlasGapDroppedTableAdvice,
			Subject{Kind: SubjectTable, Name: table.name},
		))
	}
	return findings
}

// atlasGapDroppedTableAdvice is the rest of every BC103 message: what the
// retired name costs and what to do instead.
//
// It names the rollout break rather than the rows, because the rows are DS101's
// subject and a reader who has already accepted that loss still has to be told
// this. "Even on an empty table" is the part that separates the two: a backup
// answers DS101 and answers nothing here.
const atlasGapDroppedTableAdvice = "application versions already deployed against the old schema still " +
	"query, so each of them starts failing the moment this migration commits -- a rollout break that " +
	"lands even on an empty table, which no backup mitigates; deploy code that no longer reads it " +
	"first, then drop it in a later release"

// atlasGapDroppedColumnRule reports BC104: a dropped column retires a name
// deployed clients still select and insert.
//
// It differs from DS102 in one place, and the difference is the question rather
// than an oversight: a column on a table this migration itself created is
// exempt here and reported there. No deployed application version ever saw that
// table, so nothing can break on the name -- while the rows DS102 speaks for
// are its own question, which that rule answers for itself.
func atlasGapDroppedColumnRule() Rule {
	return Rule{
		Code:     "BC104",
		Title:    "dropped column breaks deployed code",
		Severity: SeverityWarning,
		CheckFile: func(file *File) []Finding {
			if !atlasGapReportsOnThisSurface(file) {
				return nil
			}
			var findings []Finding
			created := make(map[string]bool)
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if ref := createdTableRef(stmt.Words); ref != "" {
					created[ref] = true
					continue
				}
				if !isAlterTable(stmt.Words) {
					continue
				}
				// The recognizer is DS102's, so every spelling that rule
				// reports is reported here: MySQL's `DROP col` without the
				// keyword, the IF EXISTS and ONLY forms, and a comma list,
				// which stays one finding carrying every retired name.
				subjects := droppedColumnSubjects(stmt.Words, stmt.sourceWords)
				if len(subjects) == 0 {
					continue
				}
				owner := alterTableReference(stmt.Words, stmt.sourceWords)
				if owner.normalized != "" && refersToCreated(created, owner.normalized) {
					continue
				}
				findings = append(findings, Finding{
					Rule:     "BC104",
					Title:    "dropped column breaks deployed code",
					Severity: SeverityWarning,
					File:     file.Path,
					Line:     stmt.Line,
					Message: "dropping a column retires a name application versions already deployed against " +
						"the old schema still select and insert, so each of them starts failing the moment " +
						"this migration commits, whether or not the column held any rows; " + columnDroppedAdvice,
					Context: statementFindingContext(i, subjects...),
				})
			}
			return findings
		},
	}
}

// atlasGapPostgresRules holds PG108, PG109, PG312, PG314 and PG320.
//
// PG312 is the Atlas identifier. Ptah's own PG312P, a different rule about a
// SECURITY DEFINER routine that does not pin search_path, keeps its code: the
// trailing P is what marks a rule of ours inside an Atlas family, and the two
// stay separately selectable, suppressible and reportable.
func atlasGapPostgresRules() []Rule {
	return []Rule{
		{
			Code:     "PG108",
			Title:    "index on a partitioned table locks every partition",
			Severity: SeverityWarning,
			// PostgreSQL alone. The lock behavior above was measured on
			// PostgreSQL 18.6 and on nothing else, and a wire-compatible
			// engine's partitioning is its own implementation: naming
			// CockroachDB or YugabyteDB here would be a cost claim about a
			// server this rule has never been run against.
			Dialects: []string{"postgres"},
			// File-level because the statement alone cannot say the table is
			// partitioned. The evidence is a CREATE TABLE … PARTITION BY
			// earlier in the same file; without it the rule is silent rather
			// than guessing, and PG101 still reports the ordinary index build.
			CheckFile: func(file *File) []Finding {
				if !file.IsUp {
					return nil
				}
				partitioned := make(map[string]bool)
				var findings []Finding
				for i := range file.Statements {
					stmt := &file.Statements[i]
					if ref := createdTableRef(stmt.Words); ref != "" && hasWordSeq(stmt.Words, "PARTITION", "BY") {
						partitioned[ref] = true
						continue
					}
					if !isCreateIndex(stmt.Words) {
						continue
					}
					target := atlasGapIndexTargetRef(stmt.Words)
					if target == "" || !refersToCreated(partitioned, target) {
						continue
					}
					findings = append(findings, Finding{
						Rule:     "PG108",
						Title:    "index on a partitioned table locks every partition",
						Severity: SeverityWarning,
						File:     file.Path,
						Line:     stmt.Line,
						Message: "building this index takes a SHARE lock on " + target + " and on every one of its " +
							"partitions at once, so writes stop across the whole set rather than one partition at a " +
							"time; CONCURRENTLY is refused on a partitioned table, so the way to keep writes going " +
							"is to build the index on each partition concurrently and attach it to a parent index " +
							"created ONLY",
						Context: statementFindingContext(i),
					})
				}
				return findings
			},
		},
		{
			Code:     "PG109",
			Title:    "exclusion constraint added under an exclusive lock",
			Severity: SeverityWarning,
			Dialects: []string{"postgres"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !atlasGapExcludeConstraint(stmt.Words) {
					return false, ""
				}
				return true, "adding an EXCLUDE constraint holds an ACCESS EXCLUSIVE lock while it builds the " +
					"backing index and checks every existing row against it, so reads and writes both stop for the " +
					"whole scan; there is no CONCURRENTLY form for it, so build the index separately with " +
					"CREATE INDEX CONCURRENTLY and add the constraint USING that index"
			},
		},
		{
			Code:     "PG312",
			Title:    "primary key redefined under an exclusive lock",
			Severity: SeverityWarning,
			Dialects: []string{"postgres"},
			// PG104 reports the lock an added primary key takes; on a
			// redefinition this says the same thing and adds what is built
			// under it. CD103 and DS105 are left alone: losing row identity is
			// a separate hazard from what the rebuild costs.
			Subsumes: []string{"PG104"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !atlasGapRedefinesPrimaryKey(stmt.Words) {
					return false, ""
				}
				if hasWordSeq(stmt.Words, "USING", "INDEX") {
					// The index already exists, so nothing is built under the
					// lock; the statement is a catalog edit and the expensive
					// half was the CREATE INDEX CONCURRENTLY before it.
					return false, ""
				}
				return true, "replacing the primary key builds the new unique index while holding an ACCESS " +
					"EXCLUSIVE lock, so reads and writes both stop for the whole build rather than for a catalog " +
					"edit; build the index first with CREATE INDEX CONCURRENTLY and add the constraint USING INDEX"
			},
		},
		{
			Code:     "PG314",
			Title:    "replica identity weakened for logical replication",
			Severity: SeverityWarning,
			Dialects: []string{"postgres"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !atlasGapReplicaIdentity(stmt.Words) {
					return false, ""
				}
				if hasWordSeq(stmt.Words, "REPLICA", "IDENTITY", "NOTHING") {
					return true, "REPLICA IDENTITY NOTHING stops the table publishing any row identity, so a " +
						"logical replication subscriber cannot apply an UPDATE or DELETE from it and the change " +
						"is silently lost downstream; the statement itself takes an ACCESS EXCLUSIVE lock"
				}
				return true, "REPLICA IDENTITY FULL makes every UPDATE and DELETE write every column of the old " +
					"row into the WAL, so replication volume grows with the row width and a subscriber matches " +
					"rows by a full scan rather than by key; the statement itself takes an ACCESS EXCLUSIVE lock"
			},
		},
		{
			Code:     "PG320",
			Title:    "autovacuum disabled for a table",
			Severity: SeverityWarning,
			Dialects: []string{"postgres"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) || !atlasGapDisablesAutovacuum(stmt.Canonical) {
					return false, ""
				}
				return true, "disabling autovacuum leaves dead rows for nothing to reclaim, so the table and its " +
					"indexes keep growing, planner statistics stop being refreshed, and the transaction id horizon " +
					"stops advancing for it; the statement is cheap -- it takes only a SHARE UPDATE EXCLUSIVE " +
					"lock -- and the cost is paid later, so pair it with the vacuum schedule that replaces it"
			},
		},
	}
}

// atlasGapMySQLRules holds MY137 through MY148 except MY142, which the header
// records as measured absent on every release line this repository declares.
func atlasGapMySQLRules() []Rule {
	return []Rule{
		{
			Code:     "MY137",
			Title:    "primary key redefined with a table rebuild",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			// MY132 reports the rebuild an added primary key causes; this says
			// that and the secondary indexes. CD103 stays: row identity is a
			// separate hazard from the rebuild.
			Subsumes: []string{"MY132"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !atlasGapRedefinesPrimaryKey(stmt.Words) {
					return false, ""
				}
				return true, "replacing the primary key rebuilds the table around the new clustered index and " +
					"rebuilds every secondary index with it, because a secondary index stores the primary key as " +
					"its row pointer; measured on MySQL 8.4.11 and MariaDB 11.8.9 the rebuild is in place and " +
					"LOCK=NONE is accepted, so writes continue, but ALGORITHM=INSTANT is refused and the work is " +
					"proportional to the table and to the number of secondary indexes"
			},
		},
		{
			Code:     "MY138",
			Title:    "storage engine change copies the table",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !atlasGapStorageEngine(stmt.Words) {
					return false, ""
				}
				return true, "changing the storage engine copies the whole table row by row and blocks writes for " +
					"the duration: measured on MySQL 8.4.11 and MariaDB 11.8.9, ALGORITHM=INPLACE is refused, only " +
					"COPY is accepted, and LOCK=NONE is refused with it; note that ENGINE= naming the engine the " +
					"table already uses costs the same, because it is the documented way to force a rebuild"
			},
		},
		{
			Code:     "MY139",
			Title:    "partitioning change rebuilds the table",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !atlasGapPartitioning(stmt.Words) {
					return false, ""
				}
				return true, "partitioning a table, or removing its partitioning, rewrites every row into the new " +
					"layout; measured on MySQL 8.4.11 and MariaDB 11.8.9 this statement form does not accept an " +
					"ALGORITHM or LOCK clause at all, so there is no online-DDL negotiation to ask for and no way " +
					"to keep writes going through it -- use an external online-DDL tool on a large table"
			},
		},
		{
			Code:     "MY140",
			Title:    "stored generated column added with a table copy",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) || !scanAddStoredGeneratedColumn(stmt.Words) {
					return false, ""
				}
				return true, "adding a STORED generated column computes and stores a value for every existing row, " +
					"which copies the table and blocks writes: measured on MySQL 8.4.11 and MariaDB 11.8.9, both " +
					"INSTANT and INPLACE are refused and LOCK=NONE with them; a VIRTUAL column is the in-place " +
					"alternative where the value can be computed on read"
			},
		},
		{
			Code: "MY141",
			// MY101 says only that this ALTER TABLE form usually rebuilds the
			// table; this names the operation and what it costs. DD101, DD103
			// and DS103 stay: whether a row fails is a different question from
			// what the statement costs.
			Subsumes: []string{"MY101"},
			Title:    "auto-increment column added with blocked writes",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !atlasGapAutoIncrementColumn(stmt.Words) {
					return false, ""
				}
				return true, "adding an AUTO_INCREMENT column numbers every existing row, so the table is rebuilt " +
					"and writes stop while it happens: measured on MySQL 8.4.11 and MariaDB 11.8.9 the rebuild is " +
					"in place (ALGORITHM=INPLACE is accepted) but LOCK=NONE is refused, so this is the case where " +
					"in-place does not mean online"
			},
		},
		{
			Code: "MY143",
			// MY101 says only that this ALTER TABLE form usually rebuilds the
			// table; this names the operation and what it costs. DD101, DD103
			// and DS103 stay: whether a row fails is a different question from
			// what the statement costs.
			Subsumes: []string{"MY101"},
			Title:    "generated column modified with a table copy",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !atlasGapGeneratedStored(stmt.Words) {
					return false, ""
				}
				return true, "changing a STORED generated column recomputes and rewrites its value for every " +
					"existing row, which copies the table and blocks writes: measured on MySQL 8.4.11 and " +
					"MariaDB 11.8.9, both INSTANT and INPLACE are refused and LOCK=NONE with them"
			},
		},
		{
			Code:     "MY144",
			Title:    "check constraint added with a full scan",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) || !scanAddCheckConstraint(stmt.Words) {
					return false, ""
				}
				// A constraint added NOT ENFORCED is recorded and never
				// checked, so nothing is scanned and nothing can fail. The
				// question is per clause rather than per statement: one
				// statement can add an enforced check beside an unenforced
				// one, and the enforced one still validates every row.
				if !atlasGapAddsEnforcedCheck(stmt.Words) {
					return false, ""
				}
				return true, "adding a CHECK constraint validates every existing row against it, and a row that " +
					"fails the predicate fails the migration; measured on MySQL 8.4.11 and MariaDB 11.8.9 both " +
					"INSTANT and INPLACE are refused, so the table is copied and writes stop for the duration"
			},
		},
		{
			Code:     "MY145",
			Title:    "check constraint enforcement revalidates every row",
			Severity: SeverityWarning,
			// MySQL only: MariaDB has no ENFORCED / NOT ENFORCED syntax, and
			// both spellings are a syntax error there rather than a cheaper
			// path, so a mariadb finding would describe a statement the server
			// refuses to parse.
			Dialects: []string{"mysql"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !atlasGapCheckEnforcement(stmt.Words) {
					return false, ""
				}
				if hasWordSeq(stmt.Words, "NOT", "ENFORCED") {
					return true, "unenforcing a CHECK constraint stops the server validating new rows against it, " +
						"so values the constraint was there to refuse start being accepted; the constraint stays " +
						"in the schema, which makes the change invisible to a reader comparing column definitions"
				}
				return true, "enforcing a CHECK constraint revalidates every existing row against it, and a row " +
					"that fails the predicate fails the migration; measured on MySQL 8.4.11 both INSTANT and " +
					"INPLACE are refused, so the table is copied and writes stop for the duration"
			},
		},
		{
			Code:     "MY146",
			Title:    "system versioning dropped with its history",
			Severity: SeverityError,
			// MariaDB only: MySQL 8.4 has no system versioning at all, and
			// `WITH SYSTEM VERSIONING` is a syntax error there.
			Dialects: []string{"mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !atlasGapDropSystemVersioning(stmt.Words) {
					return false, ""
				}
				return true, "DROP SYSTEM VERSIONING deletes every historical row version the table has " +
					"accumulated, permanently and without a separate DROP: the history is not moved anywhere, and " +
					"no rollback of this migration restores it; copy the history out first if any audit or " +
					"point-in-time query depends on it"
			},
		},
		{
			Code: "MY147",
			// MY101 says only that this ALTER TABLE form usually rebuilds the
			// table; this names the operation and what it costs. DD101, DD103
			// and DS103 stay: whether a row fails is a different question from
			// what the statement costs.
			Subsumes:         []string{"MY101"},
			Title:            "nullability change rebuilds the table",
			Severity:         SeverityWarning,
			Dialects:         []string{"mysql", "mariadb"},
			Input:            InputBaselineSchema,
			BaselineSubjects: atlasGapNullabilityStatements,
			CheckFile: func(file *File) []Finding {
				if !file.IsUp {
					return nil
				}
				var findings []Finding
				for _, site := range atlasGapNullabilityTransitions(file) {
					findings = append(findings, Finding{
						Rule:     "MY147",
						Title:    "nullability change rebuilds the table",
						Severity: SeverityWarning,
						File:     file.Path,
						Line:     file.Statements[site.statement].Line,
						Message: "making " + site.oldName + " NOT NULL rebuilds the table: measured on " +
							"MySQL 8.4.11 and MariaDB 11.8.9, ALGORITHM=INSTANT is refused while INPLACE and " +
							"LOCK=NONE are accepted, so writes continue but every row is read and rewritten; " +
							"whether an existing NULL fails the statement is a separate question, and DD103 " +
							"answers it from the baseline",
						Context: statementFindingContext(site.statement,
							Subject{Kind: SubjectColumn, Name: site.oldName}),
					})
				}
				return findings
			},
		},
	}
}

// atlasGapIndexTargetRef returns the table a CREATE INDEX targets, or "".
func atlasGapIndexTargetRef(w []string) string {
	for i := 0; i+1 < len(w); i++ {
		if strings.EqualFold(w[i], "ON") {
			return w[i+1]
		}
	}
	return ""
}

// atlasGapAddsEnforcedCheck reports an ADD clause whose CHECK will be
// validated.
//
// Enforcement is the default, so a clause that says nothing about it enforces;
// only an explicit trailing NOT ENFORCED turns it off. The answer is per clause
// rather than per statement, because `ADD CONSTRAINT a CHECK (…), ADD
// CONSTRAINT b CHECK (…) NOT ENFORCED` still validates every row for a -- and a
// statement-wide answer would silence the scan that actually runs.
func atlasGapAddsEnforcedCheck(w []string) bool {
	for _, start := range clauseStarts(w) {
		if start >= len(w) || w[start] != "ADD" {
			continue
		}
		if !clauseHasWord(w, start, "CHECK") {
			continue
		}
		if !clauseEndsWithPair(w, start, "NOT", "ENFORCED") {
			return true
		}
	}
	return false
}

// clauseEndsWithPair reports whether the clause starting at from ends with two
// given words in order.
func clauseEndsWithPair(w []string, from int, first, second string) bool {
	end := len(w)
	for _, start := range clauseStarts(w) {
		if start > from {
			end = start
			break
		}
	}
	return end-from >= 2 && w[end-2] == first && w[end-1] == second
}
