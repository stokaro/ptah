package postgresreplay

import (
	"fmt"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// postgresServerWideOperation names a statement whose only reason to be
// refused is that its effect reaches past the dev database into the rest of
// the server, or "" for any other statement.
//
// It is the whole of what [replay.Scope.Server] lifts on the PostgreSQL family,
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
//
// A comment on an extension or a schema of the dev database is lifted only in
// a scope with [replay.Scope.Provisioned]. The cleanup does not restore it, and a
// server the operator named or declared disposable outlives the run with the
// extensions and schemas its reset keeps, so a later run on the same dev
// database would read the comment; a provisioned server is discarded. A
// comment on a protected namespace such as pg_catalog stays refused, as its
// mutation is.
func postgresServerWideOperation(tokens []lexer.Token, scope replay.Scope) string {
	if len(tokens) == 0 {
		return ""
	}
	first := replay.NormalizedIdentifier(tokens[0])
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
		if replay.TokenSequenceAt(tokens, 1, "OWNED") {
			return "REASSIGN OWNED"
		}
	case "COMMENT":
		return postgresServerWideComment(tokens, scope)
	}
	return ""
}

// postgresServerWideCreateOrDrop names a routine definition, a role, user,
// group or database created or dropped, and DROP OWNED.
func postgresServerWideCreateOrDrop(first string, tokens []lexer.Token) string {
	if definesPostgresRoutine(tokens) {
		return first + " routine definition"
	}
	if first == "DROP" && replay.TokenSequenceAt(tokens, 1, "OWNED") {
		return "DROP OWNED"
	}
	// USER MAPPING belongs to a foreign server, which reaches past the
	// container, so it is not a role.
	if replay.TokenSequenceAt(tokens, 1, "USER", "MAPPING") {
		return ""
	}
	switch replay.NormalizedIdentifier(replay.TokenAt(tokens, 1)) {
	case "ROLE", "USER", "GROUP", "DATABASE":
		return first + " " + replay.NormalizedIdentifier(tokens[1])
	}
	return ""
}

// postgresServerWideAlter names a routine alteration, a role, user or group
// altered without SET or RESET, and ALTER DEFAULT PRIVILEGES FOR ALL ROLES
// without IN SCHEMA, which the realm cleanup cannot reset.
func postgresServerWideAlter(tokens []lexer.Token) string {
	if definesPostgresRoutine(tokens) {
		return "ALTER routine definition"
	}
	switch replay.NormalizedIdentifier(replay.TokenAt(tokens, 1)) {
	case "ROLE", "USER", "GROUP":
		if replay.TokenSequenceAt(tokens, 1, "USER", "MAPPING") ||
			replay.ContainsIdentifier(tokens[2:], "SET") || replay.ContainsIdentifier(tokens[2:], "RESET") {
			return ""
		}
		return "ALTER " + replay.NormalizedIdentifier(tokens[1])
	case "DEFAULT":
		if isGlobalAllRolesDefaultPrivileges(tokens) {
			return "ALTER DEFAULT PRIVILEGES FOR ALL ROLES without IN SCHEMA"
		}
	}
	return ""
}

// postgresServerWideComment names a comment on a role or a database, and on a
// provisioned server one on an extension or a schema.
func postgresServerWideComment(tokens []lexer.Token, scope replay.Scope) string {
	onIndex := findPostgresKeyword(tokens, "ON", 1)
	if onIndex == replay.MutationTargetNotFound || onIndex+1 >= len(tokens) {
		return ""
	}
	switch target := replay.NormalizedIdentifier(tokens[onIndex+1]); target {
	case "ROLE", "DATABASE":
		return "COMMENT ON " + target
	case "EXTENSION", "SCHEMA":
		if scope.Provisioned {
			return "COMMENT ON " + target
		}
	}
	return ""
}

// postgresServerWidePrivilege names a GRANT or REVOKE whose target is a role,
// a database, a schema, a language or a parameter: the privileges the database
// realm refuses because they outlive the objects the cleanup drops. A table,
// sequence or function privilege is not named, because the database realm
// already allows it.
func postgresServerWidePrivilege(tokens []lexer.Token) string {
	first := replay.NormalizedIdentifier(tokens[0])
	recipientKeyword := postgresPrivilegeRecipientKeyword(tokens)
	onIndex := findPostgresKeyword(tokens, "ON", 1)
	recipientIndex := findPostgresKeyword(tokens, recipientKeyword, 1)
	if onIndex == replay.MutationTargetNotFound ||
		(recipientIndex != replay.MutationTargetNotFound && onIndex > recipientIndex) {
		return first + " role membership"
	}
	classIndex := onIndex + 1
	if replay.NormalizedIdentifier(replay.TokenAt(tokens, classIndex)) == "ALL" {
		classIndex++
	}
	switch replay.NormalizedIdentifier(replay.TokenAt(tokens, classIndex)) {
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
	if namespace := (protection{dialect: dialect}).protectedPostgresMutationNamespace(tokens); namespace != "" {
		return replay.Refuse(
			dialect,
			fmt.Sprintf("protected namespace %q mutation", namespace),
		)
	}
	return nil
}
