// Package grantrefusal holds the refusals the MySQL-family, SQL Server, Oracle
// and ClickHouse renderers make for a GRANT or REVOKE whose target only the
// PostgreSQL renderer spells: a function or procedure, and columns of a table.
//
// The SQL schema reader parses `GRANT EXECUTE ON FUNCTION f(uuid) TO r` and
// `GRANT UPDATE (a) ON t TO r` for any dialect, and a plan carries the target
// to whichever renderer runs. The others would print the routine name where
// their grammar expects a table, or drop the column list and grant on the
// whole table, handing the server a statement about a different object. So
// they refuse it by name instead. The ClickHouse renderer refuses a routine
// with its own message, because a ClickHouse grant has no routine scope at
// all.
package grantrefusal

import (
	"fmt"
	"strings"

	"ptah.run/core/ptaherr"
)

// Routine reports a routine target a renderer other than PostgreSQL's cannot
// spell, wrapping [ptaherr.ErrUnsupportedFeature], or nil when the target is not
// a routine.
func Routine(dialect, statement, objectType, objectName string) error {
	switch kind := strings.ToUpper(strings.TrimSpace(objectType)); kind {
	case "FUNCTION", "PROCEDURE", "ROUTINE":
		return fmt.Errorf(
			"%w: %s ON %s %s: privileges on routines are modeled for PostgreSQL only, not %s",
			ptaherr.ErrUnsupportedFeature, statement, kind, objectName, dialect)
	default:
		return nil
	}
}

// Columns reports a column list a renderer other than PostgreSQL's cannot
// spell, wrapping [ptaherr.ErrUnsupportedFeature], or nil when there is none.
func Columns(dialect, statement, objectName string, columns []string) error {
	if len(columns) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: %s (%s) ON %s: column privileges are modeled for PostgreSQL only, not %s",
		ptaherr.ErrUnsupportedFeature, statement, strings.Join(columns, ", "), objectName, dialect)
}
