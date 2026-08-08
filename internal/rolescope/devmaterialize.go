package rolescope

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
)

// RolesToCreateOnDev splits the roles a desired schema declares into the ones a
// dev database has to be given and the ones its server already has.
//
// Materializing a desired state on a dev database is how Ptah evaluates a
// schema file or a migration directory: the database is reset, the state is
// executed on it, and the result is introspected. Every other object in that
// state belongs to the database, so resetting it is enough to guarantee the
// CREATE succeeds. A role does not. Roles are SERVER-scoped, a fresh database
// does not clear them, and CREATE ROLE is not idempotent, so a role the server
// already has makes the whole materialization fail:
//
//	Error: materialize schema on dev database: failed to execute SQL statement:
//	SQL execution failed: ERROR: role "ptah_user" already exists (SQLSTATE 42710)
//	SQL: CREATE ROLE "ptah_user" WITH LOGIN SUPERUSER ...;
//
// Measured on PostgreSQL 17.10 by inspecting a database holding one table and
// one GRANT and feeding that document straight back in against a clean sibling
// database in the same cluster: exit 1, before this partition existed. That is
// the round trip stokaro/ptah#1267 asks for, and the roles that break it are
// exactly the ones the description is right to name -- they hold privileges on
// the inspected tables.
//
// Skipping is the answer rather than refusing, and rather than reconciling.
// Refusing would fail a document the pinned Atlas community binary v1.3.0
// reads at exit 0, on the surface that exists to be drop-in. Reconciling would
// mean ALTERing a role on the operator's server because a dev database was
// pointed at it, and a dev database's contract is that it is disposable --
// nothing it does may reach beyond the database it was handed. So the role is
// left exactly as the server has it, and what that costs is reported by
// [ReportNotCreatedOnDev] rather than left silent.
//
// A role the server does NOT have is still created, which is what keeps the
// same document materializing on a server that has never seen it -- a second
// cluster, or a CI runner with an empty catalog. That case never reached the
// error above, and it still does not.
//
// Matching is by exact name. PostgreSQL role names are case-sensitive as
// stored, and both sides here are stored spellings: the label of a role block
// and a pg_roles row.
func RolesToCreateOnDev(
	declared []goschema.Role,
	present []dbschematypes.DBRole,
) (create, alreadyOnServer []goschema.Role) {
	if len(declared) == 0 {
		return declared, nil
	}
	onServer := make(map[string]struct{}, len(present))
	for _, role := range present {
		onServer[role.Name] = struct{}{}
	}
	create = make([]goschema.Role, 0, len(declared))
	for _, role := range declared {
		if _, ok := onServer[role.Name]; ok {
			alreadyOnServer = append(alreadyOnServer, role)
			continue
		}
		create = append(create, role)
	}
	return create, alreadyOnServer
}

// ReportNotCreatedOnDev writes a note naming the declared roles the dev
// database was not given, and nothing at all when it was given all of them.
//
// This note NAMES its roles, where [ReportUndescribed] deliberately reports
// only a count. The two are not the same disclosure: every name printed here
// came out of the document the caller supplied, so it tells the reader nothing
// they did not already write, while the names ReportUndescribed withholds are
// the server's own and on a shared instance are other tenants'. The names are
// what makes this note actionable -- a role kept as the server has it may
// differ from the one the document declares, and an operator cannot check
// which without knowing which roles were skipped.
//
// w may be nil, which is how the inspect surfaces spell "no diagnostics
// stream"; the note is then dropped rather than panicking. Write errors are
// dropped too: a diagnostic that fails to print must not fail a
// materialization that succeeded.
func ReportNotCreatedOnDev(w io.Writer, alreadyOnServer []goschema.Role) {
	if w == nil || len(alreadyOnServer) == 0 {
		return
	}
	names := make([]string, 0, len(alreadyOnServer))
	for _, role := range alreadyOnServer {
		names = append(names, fmt.Sprintf("%q", role.Name))
	}
	slices.Sort(names)
	fmt.Fprintf(w,
		"note: %s the schema declares already %s on the server hosting the dev database and %s not"+
			" created there: %s; roles are server-scoped rather than database-scoped, so a dev"+
			" database cannot hold its own copy, and the inspected result describes each of them as"+
			" the server has it.\n",
		countedRoles(len(alreadyOnServer)), pluralExist(len(alreadyOnServer)),
		pluralWere(len(alreadyOnServer)), strings.Join(names, ", "),
	)
}

// pluralExist agrees the verb with countedRoles.
func pluralExist(count int) string {
	if count == 1 {
		return "exists"
	}
	return "exist"
}

// pluralWere agrees the second verb with countedRoles.
func pluralWere(count int) string {
	if count == 1 {
		return "was"
	}
	return "were"
}
