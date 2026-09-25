// Package routinegrant holds the refusal the MySQL-family, SQL Server and
// Oracle renderers make for a GRANT or REVOKE on a function or procedure.
//
// The SQL schema reader parses `GRANT EXECUTE ON FUNCTION f(uuid) TO r` for any
// dialect, and a plan carries the target to whichever renderer runs. Only the
// PostgreSQL renderer spells a routine target with its argument types; the
// others would print the routine name where their grammar expects a table and
// hand the server a statement about a different object. So they refuse it by
// name instead. The ClickHouse renderer refuses it with its own message,
// because a ClickHouse grant has no routine scope at all.
package routinegrant

import (
	"fmt"
	"strings"

	"ptah.run/core/ptaherr"
)

// Refusal reports a routine target a renderer other than PostgreSQL's cannot
// spell, wrapping [ptaherr.ErrUnsupportedFeature], or nil when the target is not
// a routine.
func Refusal(dialect, statement, objectType, objectName string) error {
	switch kind := strings.ToUpper(strings.TrimSpace(objectType)); kind {
	case "FUNCTION", "PROCEDURE", "ROUTINE":
		return fmt.Errorf(
			"%w: %s ON %s %s: privileges on routines are modeled for PostgreSQL only, not %s",
			ptaherr.ErrUnsupportedFeature, statement, kind, objectName, dialect)
	default:
		return nil
	}
}
