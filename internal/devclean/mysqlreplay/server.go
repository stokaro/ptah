package mysqlreplay

import (
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// mysqlServerWideOperation names a MySQL or MariaDB statement whose only reason
// to be refused is that its effect reaches past the dev database into the rest
// of the server, or "" for any other statement.
//
// It lifts stored routines, triggers, CALL, privileges, and CREATE or DROP of a
// user, a role or a database. An EVENT is left out: the scheduler runs it on
// its own timetable, which is during the rest of the run. ALTER USER and ALTER
// ROLE are left out because they can change the credentials the run connects
// with.
func mysqlServerWideOperation(tokens []lexer.Token) string {
	if len(tokens) == 0 {
		return ""
	}
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "CALL":
		return "CALL sublanguage"
	case "GRANT", "REVOKE":
		return "privilege or role mutation"
	case "CREATE", "ALTER", "DROP":
		switch mysqlExecutableBodyKind(tokens) {
		case "":
		case "EVENT":
			return ""
		default:
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
