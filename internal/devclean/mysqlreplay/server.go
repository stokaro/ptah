package mysqlreplay

import (
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
	"ptah.run/internal/mysqlstored"
)

// mysqlServerWideOperation names a MySQL or MariaDB statement whose only reason
// to be refused is that its effect reaches past the dev database into the rest
// of the server, or "" for any other statement.
//
// It lifts stored routines, triggers, CALL, privileges, CREATE or DROP of a
// user, a role or a database, and the statements of [mysqlServerStatements]
// that name [liftedOnOwnedServer]. An EVENT is left out: the scheduler runs it on
// its own timetable, which is during the rest of the run. A statement that
// reaches the account the run connects as is refused before this is asked;
// see [sessionAccount.operation].
func mysqlServerWideOperation(stmt string, tokens []lexer.Token) string {
	if len(tokens) == 0 {
		return ""
	}
	if statement, ok := mysqlServerStatementOf(tokens); ok {
		if statement.lift == liftedOnOwnedServer {
			return statement.operation
		}
		return ""
	}
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "CALL":
		return "CALL sublanguage"
	case "GRANT", "REVOKE":
		return "privilege or role mutation"
	case "CREATE", "ALTER", "DROP":
		// A statement Read cannot read is refused before this is asked; see
		// [rejectUnreadableDefinition].
		if definition, ok, _ := mysqlstored.Read(stmt, tokens); ok && definition.Body() {
			if definition.Kind == mysqlstored.Event {
				return ""
			}
			return first + " executable stored body"
		}
		if first == "ALTER" {
			return ""
		}
		kindIndex := replay.StatementObjectKindIndex(tokens)
		if kindIndex == replay.MutationTargetNotFound {
			return ""
		}
		switch replay.NormalizedIdentifier(tokens[kindIndex]) {
		case "USER", "ROLE", "DATABASE", "SCHEMA":
			return first + " " + replay.NormalizedIdentifier(tokens[kindIndex])
		}
	}
	return ""
}

// mysqlDatabaseOperation reports a CREATE, ALTER or DROP of a DATABASE or
// SCHEMA, the whole of what [replay.Scope.Databases] lifts beyond the
// database realm.
func mysqlDatabaseOperation(tokens []lexer.Token) bool {
	switch replay.NormalizedIdentifier(replay.TokenAt(tokens, 0)) {
	case "CREATE", "ALTER", "DROP":
	default:
		return false
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return false
	}
	switch replay.NormalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE", "SCHEMA":
		return true
	default:
		return false
	}
}
