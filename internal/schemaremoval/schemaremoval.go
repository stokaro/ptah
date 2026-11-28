// Package schemaremoval protects resources outside a selection from removal
// through a drop of their containing schema or database.
package schemaremoval

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/backslashmode"
	"ptah.run/internal/sqlreach"
	"ptah.run/internal/sqlsafety"
	"ptah.run/internal/tableref"
	"ptah.run/migration/safety"
)

// ErrRemovalBlocked is the sentinel every refusal in this package wraps: a
// drop would remove an object the selection left out of the description.
var ErrRemovalBlocked = errors.New("drop would remove an object outside the selection")

// RefusalError is the refusal of one blocked drop. Its message names the
// object the drop would remove and the ways out that work for the cause the
// blocker records.
type RefusalError struct {
	// Removal is the withheld drop.
	Removal difftypes.BlockedRemoval
}

// Error states the refusal and its remedies.
func (e *RefusalError) Error() string {
	removal, blocker := e.Removal, e.Removal.BlockedBy
	if removal.NeededBy != "" {
		// The table that needs the object is the one the remedies are for,
		// and its own refusal names them.
		return fmt.Sprintf("cannot drop %s %q: table %q needs it, and that table stays because the selection blocks a drop; "+
			"the refusal of that drop says how to resolve it", removal.Kind, removal.Name, removal.NeededBy)
	}
	noun := "database"
	if removal.Kind == difftypes.BlockedTable {
		noun = "table"
	}
	pattern := cmp.Or(blocker.Pattern, removal.Name)
	head := fmt.Sprintf("cannot drop %s %q: ", noun, removal.Name)
	keep := fmt.Sprintf("declare the %s in the desired schema to keep it", noun)
	selectAll := fmt.Sprintf("select everything in it (--include '%s.*') to drop it", pattern)
	if removal.Kind == difftypes.BlockedTable {
		selectAll = fmt.Sprintf("select the whole table (--include %s) to drop it", pattern)
	}
	switch blocker.Cause {
	case catalog.SelectionKindExcluded:
		return head + fmt.Sprintf("--exclude %q keeps %s out of the read, so it is not known whether the %s holds any; "+
			"%s, exclude the %s (--exclude %s) to leave it alone, or run without that selector to drop it",
			blocker.Selector, blocker.Object, noun, keep, noun, pattern)
	case catalog.SelectionKindNotIncluded:
		return head + fmt.Sprintf("--include selects no %s, so the read leaves them out and it is not known whether the %s holds any; "+
			"%s, or %s", blocker.Object, noun, keep, selectAll)
	case catalog.SelectionNotIncluded:
		return head + fmt.Sprintf("it would remove %s, which --include does not select; %s, or %s", blocker.Object, keep, selectAll)
	default:
		return head + fmt.Sprintf("it would remove %s, which the selection excludes; %s, or exclude the whole %s (--exclude %s) to leave it alone",
			blocker.Object, keep, noun, pattern)
	}
}

// Unwrap returns [ErrRemovalBlocked].
func (e *RefusalError) Unwrap() error { return ErrRemovalBlocked }

// RefuseBlocked returns the refusal of the first drop diff blocked, in the
// order [difftypes.SchemaDiff.RemovalsBlocked] keeps, or nil when it blocked
// none. A command that plans or applies SQL calls it before it renders a
// statement: a plan without the drop would not converge, and one with it
// would remove what the selection keeps.
func RefuseBlocked(diff *difftypes.SchemaDiff) error {
	if diff == nil || len(diff.RemovalsBlocked) == 0 {
		return nil
	}
	return &RefusalError{Removal: diff.RemovalsBlocked[0]}
}

// Report writes one warning line to w for each drop diff blocked, for a
// command that reports a comparison and plans nothing: the drop is left out
// of the SQL it prints, and the line says why and what to do. A nil w writes
// nothing.
func Report(w io.Writer, diff *difftypes.SchemaDiff) {
	if w == nil || diff == nil {
		return
	}
	for _, removal := range diff.RemovalsBlocked {
		_, _ = fmt.Fprintf(w, "warning: %s\n", (&RefusalError{Removal: removal}).Error())
	}
}

// Refuse returns the refusal of dropping the database schema, as the
// catalog spells it, when blockers records an object for it, and nil
// otherwise. A nil or empty map permits every removal.
func Refuse(schema string, blockers map[string]catalog.SelectionBlocker) error {
	blocker, blocked := blockers[schema]
	if !blocked {
		return nil
	}
	return &RefusalError{Removal: difftypes.BlockedRemoval{Kind: difftypes.BlockedDatabase, Name: schema, BlockedBy: blocker}}
}

// RefuseStatements checks a saved plan's DROP DATABASE, DROP SCHEMA and DROP
// TABLE statements against current, a fresh read the plan's selection
// projected: a drop of a database or a table that now holds an object the
// selection leaves out is refused, as a comparison would refuse to plan it.
// Names are paired under the connected server's identifier semantics, and a
// table the statement does not qualify is in the connection's schema.
// Ordinary comments and string literals name no dropped object; executable
// comments are expanded for the target dialect. A nil current, or one with no
// blockers, permits every drop.
func RefuseStatements(statements []string, current *catalog.Database, info catalog.ServerInfo) error {
	if current == nil || len(current.SchemaRemovalBlockers) == 0 && len(current.TableRemovalBlockers) == 0 {
		return nil
	}
	semantics := info.IdentifierSemantics
	schemas := slices.Sorted(maps.Keys(current.SchemaRemovalBlockers))
	tables := blockedTables(current)
	for _, sql := range statements {
		assessed := sqlsafety.SQLForAssessment(sql, info.Dialect)
		for _, statement := range sqlutil.SplitSQLStatementsForSession(assessed, info.Dialect, info.Capabilities) {
			if dropped, ok := droppedSchema(statement, info); ok {
				for _, name := range schemas {
					if semantics.TableIdentityKey(dropped) == semantics.TableIdentityKey(name) {
						return Refuse(name, current.SchemaRemovalBlockers)
					}
				}
			}
			for _, dropped := range droppedTables(statement, info.Dialect) {
				for _, table := range tables {
					if sameTable(semantics, info.Schema, dropped, table.ref) {
						return &RefusalError{Removal: difftypes.BlockedRemoval{
							Kind: difftypes.BlockedTable, Name: table.name, BlockedBy: table.blocker,
						}}
					}
				}
			}
		}
	}
	return nil
}

// blockedTable is a table of a read whose drop a selection blocks.
type blockedTable struct {
	ref     [2]string
	name    string
	blocker catalog.SelectionBlocker
}

// blockedTables lists the tables of current whose drop a selection blocks,
// in the order of their names.
func blockedTables(current *catalog.Database) []blockedTable {
	var tables []blockedTable
	for _, table := range current.Tables {
		name := catalog.QualifyTableName(table.Schema, table.Name)
		if blocker, blocked := current.TableRemovalBlockers[name]; blocked {
			tables = append(tables, blockedTable{ref: [2]string{table.Schema, table.Name}, name: name, blocker: blocker})
		}
	}
	slices.SortFunc(tables, func(a, b blockedTable) int { return strings.Compare(a.name, b.name) })
	return tables
}

// droppedTables returns the tables a DROP TABLE statement names, each as its
// schema, empty where the statement does not qualify it, and its name.
func droppedTables(statement, dialect string) [][2]string {
	var tables [][2]string
	for _, target := range safety.StatementTargets(statement, dialect) {
		if target.Action != "DROP TABLE" && target.Action != "DROP TABLE IF EXISTS" {
			continue
		}
		for _, parts := range target.Objects {
			switch len(parts) {
			case 1:
				tables = append(tables, [2]string{"", parts[0]})
			case 2:
				tables = append(tables, [2]string{parts[0], parts[1]})
			}
		}
	}
	return tables
}

// sameTable reports whether a and b name one table under semantics, a part
// left empty being the connection's schema.
func sameTable(semantics identifier.Semantics, connected string, a, b [2]string) bool {
	schema := func(name string) string { return cmp.Or(name, connected) }
	return semantics.TableIdentityKey(schema(a[0])) == semantics.TableIdentityKey(schema(b[0])) &&
		semantics.TableIdentityKey(a[1]) == semantics.TableIdentityKey(b[1])
}

func droppedSchema(statement string, info catalog.ServerInfo) (string, bool) {
	escapes := backslashmode.Escapes(sqlreach.DialectUsesBackslashEscapes(info.Dialect), info.Capabilities)
	tokens := sqlreach.SignificantTokens(statement, escapes, info.Dialect)
	if len(tokens) < 3 || !tokens[0].MatchIdentifierValue("DROP") ||
		!tokens[1].MatchIdentifierValue("DATABASE") && !tokens[1].MatchIdentifierValue("SCHEMA") {
		return "", false
	}
	name := 2
	if len(tokens) >= 5 && tokens[2].MatchIdentifierValue("IF") && tokens[3].MatchIdentifierValue("EXISTS") {
		name = 4
	}
	ref, ok := tableref.Parse(tokens[name].Value)
	return ref.Name, ok && !ref.Qualified
}
