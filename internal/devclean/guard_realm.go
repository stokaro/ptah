package devclean

import (
	"fmt"

	"ptah.run/internal/lexer"
)

// postgresServerWideOperation names a statement whose only reason to be
// refused is that its effect reaches past the dev database into the rest of
// the server, or "" for any other statement.
//
// It is the whole of what [ReplayRealmServer] lifts on the PostgreSQL family,
// and it is deliberately narrower than everything the database realm refuses.
// Left out, and so refused on any server:
//
//   - ALTER ROLE, ALTER USER and ALTER DATABASE with SET or RESET, and ALTER
//     DATABASE in any form: they change the defaults of the sessions the rest
//     of the run opens, or the dev database under it;
//   - ALTER SYSTEM, LOAD and the cluster control functions: server
//     configuration that outlives the statement and applies to the run's own
//     later work;
//   - EVENT TRIGGER, CAST, LANGUAGE, TRANSFORM, TEXT SEARCH, ACCESS METHOD and
//     PUBLICATION: objects the realm cleanup does not remove, and which change
//     how the SQL the run executes afterwards is parsed or run;
//   - FOREIGN DATA WRAPPER, SERVER, USER MAPPING, SUBSCRIPTION, IMPORT FOREIGN
//     SCHEMA, dblink and an external COPY: they reach past the container;
//   - session and transaction control, TEMP objects and a mutation of a
//     protected namespace, for the reasons the database realm refuses them.
//
// A routine body and a DO block are opaque, so what they execute is not read.
// They are lifted because a migration that defines a function or creates its
// role in a DO block is ordinary, and on a server the run owns the body can
// only change what the run owns.
func postgresServerWideOperation(tokens []lexer.Token) string {
	if len(tokens) == 0 {
		return ""
	}
	first := normalizedIdentifier(tokens[0])
	switch first {
	case "DO":
		return "DO sublanguage"
	case "CALL":
		return "CALL sublanguage"
	case "CREATE", "DROP":
		return postgresServerWideCreateOrDrop(first, tokens)
	case "ALTER":
		return postgresServerWideAlter(tokens)
	case "GRANT", "REVOKE":
		return postgresServerWidePrivilege(tokens)
	case "REASSIGN":
		if tokenSequenceAt(tokens, 1, "OWNED") {
			return "REASSIGN OWNED"
		}
	case "COMMENT":
		return postgresServerWideComment(tokens)
	}
	return ""
}

// postgresServerWideCreateOrDrop names a routine definition, a role, user,
// group or database created or dropped, and DROP OWNED.
func postgresServerWideCreateOrDrop(first string, tokens []lexer.Token) string {
	if definesPostgresRoutine(tokens) {
		return first + " routine definition"
	}
	if first == "DROP" && tokenSequenceAt(tokens, 1, "OWNED") {
		return "DROP OWNED"
	}
	// USER MAPPING belongs to a foreign server, which reaches past the
	// container, so it is not a role.
	if tokenSequenceAt(tokens, 1, "USER", "MAPPING") {
		return ""
	}
	switch normalizedIdentifier(tokenAt(tokens, 1)) {
	case "ROLE", "USER", "GROUP", "DATABASE":
		return first + " " + normalizedIdentifier(tokens[1])
	}
	return ""
}

// postgresServerWideAlter names a routine alteration, a role, user or group
// altered without SET or RESET, and ALTER DEFAULT PRIVILEGES without IN
// SCHEMA.
func postgresServerWideAlter(tokens []lexer.Token) string {
	if definesPostgresRoutine(tokens) {
		return "ALTER routine definition"
	}
	switch normalizedIdentifier(tokenAt(tokens, 1)) {
	case "ROLE", "USER", "GROUP":
		if tokenSequenceAt(tokens, 1, "USER", "MAPPING") ||
			containsIdentifier(tokens[2:], "SET") || containsIdentifier(tokens[2:], "RESET") {
			return ""
		}
		return "ALTER " + normalizedIdentifier(tokens[1])
	case "DEFAULT":
		if tokenSequenceAt(tokens, 1, "DEFAULT", "PRIVILEGES") &&
			!containsTokenSequence(tokens, "IN", "SCHEMA") {
			return "ALTER DEFAULT PRIVILEGES without IN SCHEMA"
		}
	}
	return ""
}

// postgresServerWideComment names a comment on a role or a database.
func postgresServerWideComment(tokens []lexer.Token) string {
	onIndex := findPostgresKeyword(tokens, "ON", 1)
	if onIndex == mutationTargetNotFound || onIndex+1 >= len(tokens) {
		return ""
	}
	switch normalizedIdentifier(tokens[onIndex+1]) {
	case "ROLE", "DATABASE":
		return "COMMENT ON " + normalizedIdentifier(tokens[onIndex+1])
	}
	return ""
}

// postgresServerWidePrivilege names a GRANT or REVOKE whose target is a role,
// a database, a schema, a language or a parameter: the privileges the database
// realm refuses because they outlive the objects the cleanup drops. A table,
// sequence or function privilege is not named, because the database realm
// already allows it.
func postgresServerWidePrivilege(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	recipientKeyword := postgresPrivilegeRecipientKeyword(tokens)
	onIndex := findPostgresKeyword(tokens, "ON", 1)
	recipientIndex := findPostgresKeyword(tokens, recipientKeyword, 1)
	if onIndex == mutationTargetNotFound ||
		(recipientIndex != mutationTargetNotFound && onIndex > recipientIndex) {
		return first + " role membership"
	}
	classIndex := onIndex + 1
	if normalizedIdentifier(tokenAt(tokens, classIndex)) == "ALL" {
		classIndex++
	}
	switch normalizedIdentifier(tokenAt(tokens, classIndex)) {
	case "DATABASE", "LANGUAGE", "PARAMETER", "ROLE", "SCHEMA":
		return first + " database or global privilege"
	}
	return ""
}

// validatePostgresServerWideStatement is what a server-wide statement still
// answers to on a server the run owns. The one check that can apply to such a
// statement is the protected namespace: a routine created in pg_catalog, or a
// privilege granted on it, changes the catalogs the rest of the run reads.
func validatePostgresServerWideStatement(dialect string, tokens []lexer.Token) error {
	if namespace := protectedPostgresMutationNamespace(tokens); namespace != "" {
		return unsafeReplayStatement(
			dialect,
			fmt.Sprintf("protected namespace %q mutation", namespace),
		)
	}
	return nil
}

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
	first := normalizedIdentifier(tokens[0])
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
		kindIndex := statementObjectKindIndex(tokens)
		if kindIndex == mutationTargetNotFound {
			return ""
		}
		switch normalizedIdentifier(tokens[kindIndex]) {
		case "USER", "ROLE", "DATABASE", "SCHEMA":
			return first + " " + normalizedIdentifier(tokens[kindIndex])
		}
	}
	return ""
}

func tokenAt(tokens []lexer.Token, index int) lexer.Token {
	if index < 0 || index >= len(tokens) {
		return lexer.Token{}
	}
	return tokens[index]
}
