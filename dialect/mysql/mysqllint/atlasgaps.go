package mysqllint

import (
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintkit/mytype"
)

// The rules in this file close the MySQL and MariaDB gaps a refresh of the
// Atlas analyzer comparison found (stokaro/ptah#2972). Each one reports a
// consequence no existing rule named; migration lint's comparison-gap rules
// record the method.
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

// atlasGapGeneratedStored reports a STORED generated column in a MySQL MODIFY
// or CHANGE clause.
//
// It is separate from scanAddStoredGeneratedColumn, which answers for the ADD
// side: modifying an existing generated column and adding one are different
// statements with the same cost, and MY143 must not fire on the ADD that MY140
// already reports.
func atlasGapGeneratedStored(w []string) bool {
	if !lintkit.IsAlterTable(w) {
		return false
	}
	if !lintkit.HasWordSeq(w, "GENERATED", "ALWAYS", "AS") && !lintkit.HasWordSeq(w, "AS") {
		return false
	}
	if !lintkit.HasWordSeq(w, "STORED") {
		return false
	}
	return lintkit.HasWordSeq(w, "MODIFY") || lintkit.HasWordSeq(w, "CHANGE")
}

// atlasGapAutoIncrementColumn reports an ADD COLUMN carrying AUTO_INCREMENT.
//
// The keyword has to be an attribute of the added column rather than its name,
// so the scan starts at the clause head and steps over the name: `ADD COLUMN
// auto_increment INT` adds an ordinary column that happens to be named after
// the keyword, and reporting a rebuild for it is a false claim about a cheap
// statement.
func atlasGapAutoIncrementColumn(w []string) bool {
	if !lintkit.IsAlterTable(w) {
		return false
	}
	for _, start := range lintkit.ClauseStarts(w) {
		j := start
		if j >= len(w) || w[j] != "ADD" {
			continue
		}
		j++
		if j < len(w) && w[j] == "COLUMN" {
			j++
		}
		j = lintkit.SkipIfExists(w, j)
		if j >= len(w) || !lintkit.IdentLike(w[j]) {
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
	for _, start := range lintkit.ClauseStarts(w) {
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
	return lintkit.IsAlterTable(w) && clauseHeadIs(w, "ENGINE")
}

// clauseHeadIs reports whether any top-level clause of an ALTER TABLE begins
// with word.
//
// A table option is a clause of its own, so anchoring here is what separates it
// from a column that carries the same name: `ADD COLUMN engine VARCHAR(10)` is
// a clause headed by ADD.
func clauseHeadIs(w []string, word string) bool {
	for _, start := range lintkit.ClauseStarts(w) {
		if start < len(w) && w[start] == word {
			return true
		}
	}
	return false
}

// atlasGapPartitioning reports a partitioning change.
func atlasGapPartitioning(w []string) bool {
	if !lintkit.IsAlterTable(w) {
		return false
	}
	return lintkit.HasWordSeq(w, "PARTITION", "BY") || lintkit.HasWordSeq(w, "REMOVE", "PARTITIONING")
}

// atlasGapCheckEnforcement reports a CHECK constraint being enforced or
// unenforced. MySQL only; MariaDB has no such syntax.
//
// ENFORCED has to sit in a clause that alters or adds a constraint. It is not a
// reserved word, so `ADD COLUMN enforced BOOLEAN` is an ordinary column add and
// the keyword there says nothing about any constraint.
func atlasGapCheckEnforcement(w []string) bool {
	if !lintkit.IsAlterTable(w) {
		return false
	}
	for _, start := range lintkit.ClauseStarts(w) {
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
	for _, start := range lintkit.ClauseStarts(w) {
		if start > from {
			end = start
			break
		}
	}
	return end > from && w[end-1] == word
}

// atlasGapDropSystemVersioning reports MariaDB's DROP SYSTEM VERSIONING.
func atlasGapDropSystemVersioning(w []string) bool {
	return lintkit.IsAlterTable(w) && lintkit.HasWordSeq(w, "DROP", "SYSTEM", "VERSIONING")
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
func atlasGapNullabilityTransitions(file *lintkit.File) []mytype.ChangeSite {
	var sites []mytype.ChangeSite
	for _, site := range mytype.ChangeSites(file.Statements) {
		stmt := &file.Statements[site.Statement]
		if !clauseDeclaresNotNull(stmt.Words, site.TypeStart) {
			continue
		}
		before, found := file.Baseline.Column(site.Table.Normalized, lintkit.NormalizeIdent(site.OldName))
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
	for _, start := range lintkit.ClauseStarts(w) {
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
func atlasGapNullabilityStatements(file *lintkit.File) []int {
	return columnChangeStatements(file)
}

// atlasGapMySQLRules holds MY137 through MY148 except MY142, which the header
// records as measured absent on every release line this repository declares.
func atlasGapMySQLRules() []lintkit.Rule {
	return []lintkit.Rule{
		{
			Rule: lintext.Rule{
				Code:     "MY137",
				Title:    "primary key redefined with a table rebuild",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
				Subsumes: []string{"MY132"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !lintkit.RedefinesPrimaryKey(stmt.Words) {
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
			Rule: lintext.Rule{
				Code:     "MY138",
				Title:    "storage engine change copies the table",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
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
			Rule: lintext.Rule{
				Code:     "MY139",
				Title:    "partitioning change rebuilds the table",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
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
			Rule: lintext.Rule{
				Code:     "MY140",
				Title:    "stored generated column added with a table copy",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !lintkit.IsAlterTable(stmt.Words) || !lintkit.ScanAddStoredGeneratedColumn(stmt.Words) {
					return false, ""
				}
				return true, "adding a STORED generated column computes and stores a value for every existing row, " +
					"which copies the table and blocks writes: measured on MySQL 8.4.11 and MariaDB 11.8.9, both " +
					"INSTANT and INPLACE are refused and LOCK=NONE with them; a VIRTUAL column is the in-place " +
					"alternative where the value can be computed on read"
			},
		},
		{
			Rule: lintext.Rule{
				Code:     "MY141",
				Subsumes: []string{"MY101"},
				Title:    "auto-increment column added with blocked writes",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
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
			Rule: lintext.Rule{
				Code:     "MY143",
				Subsumes: []string{"MY101"},
				Title:    "generated column modified with a table copy",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !atlasGapGeneratedStored(stmt.Words) {
					return false, ""
				}
				return true, "changing a STORED generated column recomputes and rewrites its value for every " +
					"existing row, which copies the table and blocks writes: measured on MySQL 8.4.11 and " +
					"MariaDB 11.8.9, both INSTANT and INPLACE are refused and LOCK=NONE with them"
			},
		},
		{
			Rule: lintext.Rule{
				Code:     "MY144",
				Title:    "check constraint added with a full scan",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !lintkit.IsAlterTable(stmt.Words) || !scanAddCheckConstraint(stmt.Words) {
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
			Rule: lintext.Rule{
				Code:     "MY145",
				Title:    "check constraint enforcement revalidates every row",
				Severity: lintext.Warning,
				Targets:  []string{"mysql"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !atlasGapCheckEnforcement(stmt.Words) {
					return false, ""
				}
				if lintkit.HasWordSeq(stmt.Words, "NOT", "ENFORCED") {
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
			Rule: lintext.Rule{
				Code:     "MY146",
				Title:    "system versioning dropped with its history",
				Severity: lintext.Error,
				Targets:  []string{"mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
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
			Rule: lintext.Rule{
				Code:     "MY147",
				Subsumes: []string{"MY101"},
				Title:    "nullability change rebuilds the table",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
				Input:    lintext.InputBaselineSchema,
			},
			BaselineSubjects: atlasGapNullabilityStatements,
			File: func(file *lintkit.File) []lintext.Violation {
				if !file.IsUp {
					return nil
				}
				var findings []lintext.Violation
				for _, site := range atlasGapNullabilityTransitions(file) {
					findings = append(findings, lintext.Violation{Statement: site.Statement, Message: "making " + site.OldName + " NOT NULL rebuilds the table: measured on " +
						"MySQL 8.4.11 and MariaDB 11.8.9, ALGORITHM=INSTANT is refused while INPLACE and " +
						"LOCK=NONE are accepted, so writes continue but every row is read and rewritten; " +
						"whether an existing NULL fails the statement is a separate question, and DD103 " +
						"answers it from the baseline", Subjects: []lintext.Subject{{Kind: lintext.SubjectColumn, Name: site.OldName}}})
				}
				return findings
			},
		},
	}
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
	for _, start := range lintkit.ClauseStarts(w) {
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
	for _, start := range lintkit.ClauseStarts(w) {
		if start > from {
			end = start
			break
		}
	}
	return end-from >= 2 && w[end-2] == first && w[end-1] == second
}
