package postgresreplay

import (
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// postgresUnconfinedOperation names a statement the guard refuses only because
// its effect may reach past the dev database, or "" for any other statement.
// It is what [replay.Scope.TrustedSQL] lifts as a whole statement, on a server
// the run does not own; such a statement still answers to the protected
// namespaces, as it does on a server the run owns.
//
// It is the set a server the run owns lifts ([postgresServerWideOperation]),
// read with a scope that provisioned nothing: a DO block, a CALL, a routine
// definition, a role, user, group or database created or dropped, a role
// altered without SET or RESET, DROP OWNED and REASSIGN OWNED, a privilege on a
// database, schema, language, parameter or role, and a comment on a role or a
// database. Their effect on the rest of the server is what the operator trusts:
// a role a DO block creates stays on the server after the run, because trust
// does not make the run the server's owner, and the cleanup drops no role.
//
// Two of that set's members stay refused. ALTER DEFAULT PRIVILEGES FOR ALL
// ROLES without IN SCHEMA is CockroachDB's global default, which the realm
// cleanup cannot read apart from the built-in one and so cannot reset: it is a
// leftover in the dev database, not an effect elsewhere. A comment on an
// extension or a schema of the dev database is lifted only on a server the run
// provisioned, because the reset keeps those objects and not their comment.
// One on an extension or a schema the run created is accepted in every scope,
// trusted or not, since every cleanup drops the object with its comment; see
// [protection.commentOnDroppedObject].
//
// Three refusals are lifted where they are checked rather than here, because
// they can share a statement with a refusal that stays: an external COPY, a
// dblink call and IMPORT FOREIGN SCHEMA. Each reaches past the server and
// changes nothing in it that the cleanup does not remove.
//
// Every other refusal protects the run rather than the server and stays under
// trust:
//
//   - session and transaction control, a setting [postgresSettingReach]
//     refuses (search_path, role, session_authorization,
//     standard_conforming_strings and client_encoding unless the value leaves
//     the guard's reading as it is, and a parameter set past its
//     transaction), SET ROLE and SET SESSION AUTHORIZATION, LOCK outside the
//     form the guard reads, and a session advisory lock: the rest of the
//     replay runs in that session;
//   - a mutation of a protected namespace such as pg_catalog, written with
//     the namespace, through a name the session's catalog holds there, or
//     through a view, a rule or a trigger's argument that writes it, a
//     CREATE with no schema on a path that lists it, and a TEMP object
//     outside a session scope: the catalogs the run reads, and objects
//     neither the reads nor the cleanup see;
//   - ALTER SYSTEM, LOAD, ALTER DATABASE, ALTER ROLE or ALTER USER with SET or
//     RESET, and the cluster control functions, called or named for an
//     operator, an aggregate or a type to call: they configure the server and
//     the sessions the run opens after the statement;
//   - TABLESPACE, which configures where the server stores what the run
//     creates, as a server the run owns keeps it refused;
//   - EVENT TRIGGER, CAST, LANGUAGE, TRANSFORM, TEXT SEARCH, ACCESS METHOD,
//     SUBSCRIPTION, FOREIGN DATA WRAPPER, SERVER and USER MAPPING, a
//     publication outside PostgreSQL, a large object and a replication slot or
//     origin: objects of the dev database the realm cleanup cannot remove, and
//     refuses once the run created one, several of which also change how the
//     run's later SQL runs. A PostgreSQL publication is accepted where every
//     cleanup drops the ones the run created, except a change to or the drop
//     of one the claim found; see confinedPublicationDDL;
//   - a privilege on, or a comment on, a tablespace, a foreign server or a
//     large object, and ALTER EXTENSION and ALTER SCHEMA metadata: state the
//     reset does not return;
//   - the CockroachDB and YugabyteDB operations of their own: cluster settings,
//     jobs and schedules that run on their own after the statement, and
//     objects the cleanup does not read.
func postgresUnconfinedOperation(stmt string, tokens []lexer.Token) string {
	if isGlobalAllRolesDefaultPrivileges(tokens) {
		return ""
	}
	return postgresServerWideOperation(stmt, tokens, replay.Scope{})
}
