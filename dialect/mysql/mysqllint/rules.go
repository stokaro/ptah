package mysqllint

import (
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
)

// mysqlRules covers the MY family: MySQL/MariaDB-specific hazards.
func mysqlRules() []lintkit.Rule {
	return []lintkit.Rule{
		{
			Rule: lintext.Rule{
				Code:     "MY101",
				Title:    "lock-heavy ALTER TABLE",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !lintkit.IsAlterTable(stmt.Words) {
					return false, ""
				}
				heavy := lintkit.ScanModifyChange(stmt.Words) || scanConvertCharset(stmt.Words) ||
					scanAddColumnNotNullWithoutDefault(stmt.Words)
				if !heavy || scanPinnedOnlineDDL(stmt.Words) {
					return false, ""
				}
				return true, "this ALTER TABLE form usually rebuilds the table and blocks writes for the duration on MySQL/MariaDB; " +
					"for large tables use an online-DDL tool (gh-ost, pt-online-schema-change), or pin ALGORITHM=INPLACE/LOCK=NONE " +
					"so the server refuses a blocking rebuild instead of performing it"
			},
		},
		{
			Rule: lintext.Rule{
				Code:     "MY102",
				Title:    "inline reference ignored on a column",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !scanInlineColumnReference(stmt.Words) {
					return false, ""
				}
				return true, "an inline REFERENCES clause on a column means different things on the " +
					"two engines: MySQL ignores it and creates no foreign key, MariaDB enforces it; " +
					"write an explicit FOREIGN KEY constraint so both build the same relationship"
			},
		},
		{
			Rule: lintext.Rule{
				Code:     "MY131",
				Title:    "foreign key added with blocking DDL",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !lintkit.IsAlterTable(stmt.Words) || !scanAddForeignKey(stmt.Words) {
					return false, ""
				}
				return true, "adding a foreign key can copy or lock the table and block writes on MySQL/MariaDB; use an online migration plan for populated tables"
			},
		},
		{
			Rule: lintext.Rule{
				Code:     "MY132",
				Title:    "primary key added with table rebuild",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !lintkit.IsAlterTable(stmt.Words) || !lintkit.ScanAddPrimaryKey(stmt.Words) {
					return false, ""
				}
				// Measured on MySQL 8.4 and MariaDB 11.8.9 (mysqlcost.go): the
				// rebuild is in place and LOCK=NONE is accepted, so the older
				// wording that it blocks DML overstated the cost.
				return true, "adding a primary key rebuilds the table around the new clustered index; " +
					"both MySQL and MariaDB do it in place with writes allowed (ALGORITHM=INPLACE, LOCK=NONE), " +
					"but the rebuild reads and rewrites every row and ALGORITHM=INSTANT is refused, " +
					"so plan it for a quiet period or an online-DDL tool on a large table"
			},
		},
		{
			Rule: lintext.Rule{
				Code:     "MY134",
				Title:    "fulltext index added with blocking DDL",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !lintkit.IsAlterTable(stmt.Words) || !scanAddIndexKind(stmt.Words, "FULLTEXT") {
					return false, ""
				}
				return true, "adding a FULLTEXT index can rebuild the table and block writes on MySQL/MariaDB; use an online-DDL strategy for populated tables"
			},
		},
		{
			Rule: lintext.Rule{
				Code:     "MY135",
				Title:    "spatial index added with blocking DDL",
				Severity: lintext.Warning,
				Targets:  []string{"mysql", "mariadb"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !lintkit.IsAlterTable(stmt.Words) || !scanAddIndexKind(stmt.Words, "SPATIAL") {
					return false, ""
				}
				return true, "adding a SPATIAL index can rebuild the table and block writes on MySQL/MariaDB; use an online-DDL strategy for populated tables"
			},
		},
	}
}

// scanConvertCharset reports whether an ALTER TABLE statement converts the
// table to another character set (a full-table rewrite on MySQL/MariaDB).
// CHARSET is the accepted synonym of CHARACTER SET.
func scanConvertCharset(w []string) bool {
	for _, i := range lintkit.ClauseStarts(w) {
		if lintkit.HasWordPrefix(w[i:], "CONVERT", "TO", "CHARACTER", "SET") ||
			lintkit.HasWordPrefix(w[i:], "CONVERT", "TO", "CHARSET") {
			return true
		}
	}
	return false
}

func scanAddColumnNotNullWithoutDefault(w []string) bool {
	return len(lintkit.NonNullableAddedColumnSubjects(w, w)) > 0
}

// scanInlineColumnReference reports a column-level REFERENCES clause in either
// statement that can carry one.
//
// The ALTER TABLE half is the original rule. The CREATE TABLE half is the same
// defect in the spelling a schema is usually written in, and it was not
// reported: measured, a migration body creating a table with an inline
// REFERENCES passed `ptah migrations lint --dialect mysql` with no finding
// while the ADD COLUMN form warned (stokaro/ptah#2831).
func scanInlineColumnReference(w []string) bool {
	if lintkit.IsAlterTable(w) {
		return scanAddColumnInlineReference(w)
	}
	return isCreateTable(w) && scanCreateTableInlineReference(w)
}

func isCreateTable(w []string) bool {
	return lintkit.HasWordPrefix(w, "CREATE", "TABLE")
}

// scanCreateTableInlineReference reports a REFERENCES clause inside a column
// definition rather than inside a table constraint.
//
// A table-level `FOREIGN KEY (a) REFERENCES p (id)` is enforced and must not be
// reported, so an element opening with a constraint keyword is skipped --
// including `CONSTRAINT f REFERENCES p (id)`, which MySQL refuses outright with
// error 1064 rather than ignoring.
func scanCreateTableInlineReference(w []string) bool {
	depth := 0
	elementStart := -1
	for i, word := range w {
		switch word {
		case "(":
			depth++
			if depth == 1 {
				elementStart = i + 1
			}
			continue
		case ")":
			depth--
			if depth == 0 {
				return inlineReferenceElement(w, elementStart, i)
			}
			continue
		case ",":
			if depth != 1 {
				continue
			}
			if inlineReferenceElement(w, elementStart, i) {
				return true
			}
			elementStart = i + 1
			continue
		}
	}
	return false
}

// inlineReferenceElement reports whether one element of a table body is a
// column definition carrying REFERENCES.
func inlineReferenceElement(w []string, start, end int) bool {
	if start < 0 || start >= end || end > len(w) {
		return false
	}
	if lintkit.AddConstraintTargets[w[start]] {
		return false
	}
	return slices.Contains(w[start:end], "REFERENCES")
}

func scanAddColumnInlineReference(w []string) bool {
	for _, i := range lintkit.ClauseStarts(w) {
		start, end, ok := lintkit.AddColumnClause(w, i)
		if ok && slices.Contains(w[start:end], "REFERENCES") {
			return true
		}
	}
	return false
}

func scanAddCheckConstraint(w []string) bool {
	return lintkit.AnyAlterClause(w, lintkit.AddsCheckConstraint)
}

func scanAddForeignKey(w []string) bool {
	return lintkit.AnyAlterClause(w, lintkit.AddsForeignKey)
}

func scanAddIndexKind(w []string, kind string) bool {
	for _, i := range lintkit.ClauseStarts(w) {
		clause := w[i:lintkit.ClauseEnd(w, i)]
		if !lintkit.HasWordPrefix(clause, "ADD") || !slices.Contains(clause, kind) {
			continue
		}
		if slices.Contains(clause, "INDEX") || slices.Contains(clause, "KEY") {
			return true
		}
	}
	return false
}

// scanPinnedOnlineDDL reports whether an ALTER TABLE statement pins a
// non-blocking online-DDL path: with ALGORITHM=INPLACE/INSTANT or LOCK=NONE
// the server errors out instead of silently falling back to a locking
// rebuild, so the lock hazard cannot occur. The = is optional in the MySQL
// grammar.
func scanPinnedOnlineDDL(w []string) bool {
	for _, i := range lintkit.ClauseStarts(w) {
		if i >= len(w) || (w[i] != "ALGORITHM" && w[i] != "LOCK") {
			continue
		}
		j := i + 1
		if j < len(w) && w[j] == "=" {
			j++
		}
		if j >= len(w) {
			continue
		}
		if w[i] == "ALGORITHM" && (w[j] == "INPLACE" || w[j] == "INSTANT") {
			return true
		}
		if w[i] == "LOCK" && w[j] == "NONE" {
			return true
		}
	}
	return false
}
