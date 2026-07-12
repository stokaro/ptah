package lint

import (
	"fmt"
	"slices"
	"unicode"
	"unicode/utf8"
)

// Rules returns the built-in rule set. The slice is rebuilt on every call so
// callers can never corrupt the registry.
func Rules() []Rule {
	var rules []Rule
	rules = append(rules, dataSafetyRules()...)
	rules = append(rules, migrationFormRules()...)
	rules = append(rules, compatibilityRules()...)
	rules = append(rules, postgresRules()...)
	rules = append(rules, mysqlRules()...)
	return rules
}

// dataSafetyRules covers the DS family: statements that destroy data.
func dataSafetyRules() []Rule {
	return []Rule{
		{
			Code:     "DS101",
			Title:    "table dropped",
			Severity: SeverityError,
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !hasWordPrefix(stmt.Words, "DROP", "TABLE") {
					return false, ""
				}
				return true, "DROP TABLE permanently deletes the table and every row in it; take a verified backup first and consider a rename-and-retire window instead"
			},
		},
		{
			Code:     "DS102",
			Title:    "column dropped",
			Severity: SeverityError,
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) || !scanDropColumn(stmt.Words) {
					return false, ""
				}
				return true, "DROP COLUMN permanently deletes the column's data; deploy readers that no longer use the column first, then drop it in a later release"
			},
		},
		{
			Code:     "DS103",
			Title:    "column type changed",
			Severity: SeverityWarning,
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) {
					return false, ""
				}
				if !scanModifyChange(stmt.Words) && !scanAlterColumnType(stmt.Words) {
					return false, ""
				}
				return true, "changing a column type can truncate or reject existing values and may rewrite the table under a lock; verify the old-to-new value mapping on production data first"
			},
		},
	}
}

// migrationFormRules covers the MF family: file-level migration hygiene.
func migrationFormRules() []Rule {
	return []Rule{
		{
			Code:     "MF101",
			Title:    "missing down migration",
			Severity: SeverityWarning,
			CheckFile: func(file *File) []Finding {
				if !file.IsUp || file.HasPair {
					return nil
				}
				return []Finding{{
					Rule:     "MF101",
					Title:    "missing down migration",
					Severity: SeverityWarning,
					File:     file.Path,
					Message:  "no matching .down.sql exists; a failed deploy cannot be rolled back mechanically",
				}}
			},
		},
		{
			Code:     "MF102",
			Title:    "empty migration",
			Severity: SeverityWarning,
			CheckFile: func(file *File) []Finding {
				if !file.IsUp || len(file.Statements) > 0 {
					return nil
				}
				return []Finding{{
					Rule:     "MF102",
					Title:    "empty migration",
					Severity: SeverityWarning,
					File:     file.Path,
					Message:  "the migration contains no executable statements; delete it or fill it in",
				}}
			},
		},
		{
			Code:     "MF103",
			Title:    "non-conventional file name",
			Severity: SeverityWarning,
			CheckFile: func(file *File) []Finding {
				if file.WellFormedName {
					return nil
				}
				return []Finding{{
					Rule:     "MF103",
					Title:    "non-conventional file name",
					Severity: SeverityWarning,
					File:     file.Path,
					Message:  "file name does not match NNNNNNNNNN_description.(up|down).sql; the migrator will not pick it up",
				}}
			},
		},
	}
}

// compatibilityRules covers the BC family: changes that break deployed code.
func compatibilityRules() []Rule {
	return []Rule{
		{
			Code:     "BC101",
			Title:    "rename breaks deployed code",
			Severity: SeverityWarning,
			CheckStatement: func(stmt *Statement) (bool, string) {
				standalone := hasWordPrefix(stmt.Words, "RENAME", "TABLE")
				if !standalone && (!isAlterTable(stmt.Words) || !scanAlterRename(stmt.Words)) {
					return false, ""
				}
				return true, "renames are not backwards compatible: application versions deployed against the old name fail instantly; prefer add-new/backfill/drop-old across releases"
			},
		},
	}
}

// postgresRules covers the PG family: PostgreSQL-specific hazards.
func postgresRules() []Rule {
	return []Rule{
		{
			Code:     "PG101",
			Title:    "index built with a table lock",
			Severity: SeverityWarning,
			Dialects: []string{"postgres"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isCreateIndex(stmt.Words) || slices.Contains(stmt.Words, "CONCURRENTLY") {
					return false, ""
				}
				return true, "CREATE INDEX without CONCURRENTLY blocks writes to the table for the whole build; on a populated table use CREATE INDEX CONCURRENTLY outside a transaction"
			},
		},
		{
			Code:     "PG102",
			Title:    "enum value added inside a transaction",
			Severity: SeverityWarning,
			Dialects: []string{"postgres"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !hasWordPrefix(stmt.Words, "ALTER", "TYPE") || !hasWordSeq(stmt.Words, "ADD", "VALUE") {
					return false, ""
				}
				return true, "ALTER TYPE ... ADD VALUE cannot run inside a transaction block before PostgreSQL 12, and the new value stays unusable within the same transaction on newer versions; run it in its own non-transactional migration"
			},
		},
	}
}

// mysqlRules covers the MY family: MySQL/MariaDB-specific hazards.
func mysqlRules() []Rule {
	return []Rule{
		{
			Code:     "MY101",
			Title:    "lock-heavy ALTER TABLE",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) {
					return false, ""
				}
				heavy := scanModifyChange(stmt.Words) || scanConvertCharset(stmt.Words)
				if !heavy {
					return false, ""
				}
				return true, "this ALTER TABLE form usually rebuilds the table and blocks writes for the duration on MySQL/MariaDB; for large tables use an online-DDL tool (gh-ost, pt-online-schema-change) or verify ALGORITHM=INPLACE applies"
			},
		},
	}
}

// dropTargets are the keywords that can follow ALTER TABLE ... DROP when the
// clause drops something other than a column: constraints, the key family,
// partitioning, system versioning. A column with one of these names must be
// quoted to be valid SQL, and quoted identifiers keep their quotes in Words,
// so real columns never collide with this set.
var dropTargets = map[string]bool{
	"CONSTRAINT":   true,
	"INDEX":        true,
	"KEY":          true,
	"FOREIGN":      true,
	"PRIMARY":      true,
	"UNIQUE":       true,
	"CHECK":        true,
	"PARTITION":    true,
	"PARTITIONING": true,
	"SYSTEM":       true,
}

// isAlterTable reports whether the statement's words begin with ALTER TABLE.
func isAlterTable(w []string) bool {
	return hasWordPrefix(w, "ALTER", "TABLE")
}

// isCreateIndex reports whether the statement's words begin with
// CREATE [UNIQUE] INDEX.
func isCreateIndex(w []string) bool {
	if len(w) == 0 || w[0] != "CREATE" {
		return false
	}
	j := 1
	if j < len(w) && w[j] == "UNIQUE" {
		j++
	}
	return j < len(w) && w[j] == "INDEX"
}

// hasWordPrefix reports whether the word sequence starts with the given words.
func hasWordPrefix(w []string, prefix ...string) bool {
	if len(w) < len(prefix) {
		return false
	}
	for i, p := range prefix {
		if w[i] != p {
			return false
		}
	}
	return true
}

// hasWordSeq reports whether the given words appear consecutively anywhere in
// the sequence.
func hasWordSeq(w []string, seq ...string) bool {
	for i := 0; i+len(seq) <= len(w); i++ {
		if hasWordPrefix(w[i:], seq...) {
			return true
		}
	}
	return false
}

// identLike reports whether a word can name a column or table: a quoted
// identifier, or a bare word starting with a letter, underscore, or digit.
// Single-quoted string literals are values, never identifiers.
func identLike(word string) bool {
	if word == "" {
		return false
	}
	switch word[0] {
	case '`', '"':
		return true
	case '\'':
		return false
	}
	r, _ := utf8.DecodeRuneInString(word)
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// skipIfExists advances past an optional IF EXISTS at w[j].
func skipIfExists(w []string, j int) int {
	if j+1 < len(w) && w[j] == "IF" && w[j+1] == "EXISTS" {
		return j + 2
	}
	return j
}

// clauseStarts returns the word indices where the clauses of an ALTER TABLE
// statement begin: the first word after the table reference and each word
// after a top-level comma. Commas inside parentheses (type parameters,
// expressions) do not start a clause. Anchoring scans to clause heads keeps
// clause keywords (DROP, MODIFY, RENAME, ...) from being confused with
// column names appearing mid-clause, e.g. ADD COLUMN modify TEXT.
func clauseStarts(w []string) []int {
	j := 2
	if j < len(w) && w[j] == "ONLY" {
		j++ // postgres: ALTER TABLE ONLY tbl
	}
	j = skipIfExists(w, j) // postgres: ALTER TABLE IF EXISTS tbl
	if j < len(w) && identLike(w[j]) {
		j++
		for j+1 < len(w) && w[j] == "." && identLike(w[j+1]) {
			j += 2 // schema-qualified reference: schema.tbl
		}
	}
	starts := []int{j}
	depth := 0
	for k := j; k < len(w); k++ {
		switch w[k] {
		case "(":
			depth++
		case ")":
			if depth > 0 {
				depth--
			}
		case ",":
			if depth == 0 {
				starts = append(starts, k+1)
			}
		}
	}
	return starts
}

// scanDropColumn reports whether an ALTER TABLE statement drops a column.
// The COLUMN keyword is optional in PostgreSQL and the MySQL family, so a
// clause-head DROP followed by an identifier counts unless the identifier is
// a known non-column DROP target (DROP CONSTRAINT, DROP PRIMARY KEY, ...).
func scanDropColumn(w []string) bool {
	for _, i := range clauseStarts(w) {
		if i >= len(w) || w[i] != "DROP" {
			continue
		}
		j := i + 1
		explicit := false
		if j < len(w) && w[j] == "COLUMN" {
			explicit = true
			j++
		}
		j = skipIfExists(w, j)
		if j >= len(w) {
			continue
		}
		if !explicit && dropTargets[w[j]] {
			continue
		}
		if identLike(w[j]) {
			return true
		}
	}
	return false
}

// scanModifyChange reports whether an ALTER TABLE statement rewrites a column
// via the MySQL-family MODIFY/CHANGE clauses (COLUMN keyword optional).
func scanModifyChange(w []string) bool {
	for _, i := range clauseStarts(w) {
		if i >= len(w) || (w[i] != "MODIFY" && w[i] != "CHANGE") {
			continue
		}
		j := i + 1
		if j < len(w) && w[j] == "COLUMN" {
			j++
		}
		j = skipIfExists(w, j)
		if j < len(w) && identLike(w[j]) {
			return true
		}
	}
	return false
}

// scanAlterColumnType reports whether an ALTER TABLE statement changes a
// column's type via ALTER [COLUMN] name [SET DATA] TYPE. The ordered scan
// keeps a column merely named "type" (e.g. ALTER COLUMN type SET NOT NULL)
// from matching.
func scanAlterColumnType(w []string) bool {
	for _, i := range clauseStarts(w) {
		if i >= len(w) || w[i] != "ALTER" {
			continue
		}
		j := i + 1
		if j < len(w) && w[j] == "COLUMN" {
			j++
		}
		j = skipIfExists(w, j)
		if j >= len(w) || !identLike(w[j]) {
			continue
		}
		k := j + 1
		if k+1 < len(w) && w[k] == "SET" && w[k+1] == "DATA" {
			k += 2
		}
		if k < len(w) && w[k] == "TYPE" {
			return true
		}
	}
	return false
}

// scanConvertCharset reports whether an ALTER TABLE statement converts the
// table to another character set (a full-table rewrite on MySQL/MariaDB).
func scanConvertCharset(w []string) bool {
	for _, i := range clauseStarts(w) {
		if hasWordPrefix(w[i:], "CONVERT", "TO", "CHARACTER", "SET") {
			return true
		}
	}
	return false
}

// scanAlterRename reports whether an ALTER TABLE statement renames the table
// or a column. Handles RENAME TO/AS (table), RENAME COLUMN a TO b, the
// PostgreSQL form without the COLUMN keyword, and MySQL's bare RENAME
// new_name. Index, key, and constraint renames are invisible to applications
// and are deliberately skipped.
func scanAlterRename(w []string) bool {
	for _, i := range clauseStarts(w) {
		if i+1 >= len(w) || w[i] != "RENAME" {
			continue
		}
		switch w[i+1] {
		case "INDEX", "KEY", "CONSTRAINT":
			continue
		case "TO", "AS", "COLUMN":
			return true
		}
		if identLike(w[i+1]) {
			return true
		}
	}
	return false
}

// Describe renders one finding as a single human-readable line.
func Describe(f Finding) string {
	location := f.File
	if f.Line > 0 {
		location = fmt.Sprintf("%s:%d", f.File, f.Line)
	}
	return fmt.Sprintf("%s [%s] %s: %s (%s)", location, f.Severity, f.Rule, f.Message, f.Title)
}
