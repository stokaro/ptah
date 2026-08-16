// Package sqliteforeignkeys carries the one thing a SQLite table rebuild needs
// two distant packages to agree on: the spelling of the foreign-key pragmas
// that bracket the rebuild, and how to recognize them again.
//
// The planner emits them because the plan is also a file a person runs, and
// outside a transaction the pragmas do exactly what they say. The apply path
// recognizes them because inside a transaction they do nothing at all --
// PRAGMA foreign_keys is silently ignored there -- and opens that transaction
// with enforcement already suspended on the connection instead. The statements
// themselves stay where they are and run as the no-ops they are. Two packages
// matching on text is why the text lives in one place.
package sqliteforeignkeys

import (
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
)

// DisableStatement turns foreign-key enforcement off for the rebuild, and
// EnableStatement turns it back on afterwards. SQLite's own ALTER TABLE
// procedure prescribes the pair: a rebuild drops the old table and renames a
// copy over it, and a DROP with enforcement on is a foreign-key violation as
// soon as another table references the rebuilt one.
const (
	DisableStatement = "PRAGMA foreign_keys = off;"
	EnableStatement  = "PRAGMA foreign_keys = on;"
)

// Brackets reports whether statements are a plan wrapped in the foreign-key
// pragmas: a disabling one first and an enabling one last.
//
// Only the bracket counts. A pragma anywhere else is left to execute where it
// stands, because a statement a person put in the middle of a plan means
// something there. The bracket is what this repository's planner emits, and
// what the pinned community binary emits.
func Brackets(statements []string) bool {
	if len(statements) < 2 {
		return false
	}
	if enabled, ok := parse(statements[0]); !ok || enabled {
		return false
	}
	enabled, ok := parse(statements[len(statements)-1])
	return ok && enabled
}

// BracketsSQL is Brackets over SQL text that has not been split yet, which is
// the shape a migration file arrives in.
//
// Comments are dropped first. A migration file opens with a generated header,
// and a checker that counted it as the first statement would never recognize
// any file this repository writes.
func BracketsSQL(sqlText string) bool {
	split := sqlutil.SplitSQLStatementsForDialect(sqlText, platform.SQLite)
	statements := make([]string, 0, len(split))
	for _, statement := range split {
		statement = strings.TrimSpace(sqlutil.StripCommentsForDialect(statement, platform.SQLite))
		if statement != "" {
			statements = append(statements, statement)
		}
	}
	return Brackets(statements)
}

// parse reports the value a foreign-keys pragma sets, and whether stmt is one
// at all. It accepts the spellings SQLite accepts, so that a plan written by
// hand is recognized as readily as a plan this repository emitted.
func parse(stmt string) (enabled, ok bool) {
	normalized := strings.ToLower(strings.TrimSpace(stmt))
	normalized = strings.TrimSpace(strings.TrimSuffix(normalized, ";"))
	rest, isPragma := strings.CutPrefix(normalized, "pragma ")
	if !isPragma {
		return false, false
	}

	name, value, found := strings.Cut(rest, "=")
	if !found {
		return false, false
	}
	if strings.TrimSpace(name) != "foreign_keys" {
		return false, false
	}

	switch strings.TrimSpace(value) {
	case "off", "0", "false", "no":
		return false, true
	case "on", "1", "true", "yes":
		return true, true
	default:
		return false, false
	}
}
