package mysqllint

import (
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
)

func mysqlOnlineRule() lintkit.Rule {
	rule := lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "ON102",
			Title:    "statement does not ask MySQL to apply it online",
			Severity: lintext.Error,
			Targets:  []string{"mysql", "mariadb"},
		},
		Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
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
	if !lintkit.IsAlterTable(words) {
		return lintkit.HasWordPrefix(words, "CREATE", "TABLE") ||
			lintkit.HasWordPrefix(words, "CREATE", "DATABASE") ||
			lintkit.HasWordPrefix(words, "CREATE", "SCHEMA")
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
