// Package schemaremoval protects resources outside a selection from removal
// through a drop of their containing schema or database.
package schemaremoval

import (
	"fmt"
	"maps"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/backslashmode"
	"ptah.run/internal/sqlreach"
	"ptah.run/internal/sqlsafety"
	"ptah.run/internal/tableref"
)

// Refuse returns an error naming the resource that blocks removing schema.
// The schema name is the catalog's exact spelling. A nil or empty blockers
// map, or a schema without a blocker, permits the removal.
func Refuse(schema string, blockers map[string]string) error {
	if blockedBy := blockers[schema]; blockedBy != "" {
		return fmt.Errorf("cannot drop database %q: it would remove %s outside the selection; "+
			"keep the database in the desired schema or exclude the whole database", schema, blockedBy)
	}
	return nil
}

// RefuseStatements checks a saved plan's DROP DATABASE and DROP SCHEMA
// statements against a fresh read's selection blockers. Names are paired
// under the connected server's identifier semantics. Ordinary comments and
// string literals name no dropped schema; executable comments are expanded
// with the shared safety reader. A plan that drops no protected schema is
// unchanged by drift in excluded objects.
func RefuseStatements(statements []string, blockers map[string]string, info catalog.ServerInfo) error {
	if len(blockers) == 0 {
		return nil
	}
	names := slices.Sorted(maps.Keys(blockers))
	for _, sql := range statements {
		assessed := sqlsafety.SQLForAssessment(sql, info.Dialect)
		for _, statement := range sqlutil.SplitSQLStatementsForSession(assessed, info.Dialect, info.Capabilities) {
			dropped, ok := droppedSchema(statement, info)
			if !ok {
				continue
			}
			for _, name := range names {
				if info.IdentifierSemantics.TableIdentityKey(dropped) == info.IdentifierSemantics.TableIdentityKey(name) {
					if err := Refuse(name, blockers); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// droppedSchema reads only the direct drop's target. The shared splitter and
// lexer decide statement, comment, string and quoted-identifier boundaries.
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
