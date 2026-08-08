// Package rolescope owns what Ptah does about roles being server-scoped while
// a schema description is not: the opt-in that restores the full cluster read
// on the same surface, the note that tells an operator which roles the default
// description left out, and the partition that keeps a dev-database
// materialization from re-creating a role its server already has.
package rolescope

import (
	"fmt"
	"io"
	"os"
	"strconv"

	dbschematypes "go.5x5.cz/ptah/dbschema/types"
)

// DescribeAllEnvVar restores the pre-scoping read: every role Ptah manages on
// the server is described, not only the roles the inspected schemas refer to.
//
// It exists because scoping the description removed a capability, and
// AGENTS.md ("Compatibility never removes a capability. Constitute it, do not
// discard it.") does not allow that to be the end of the story. Measured on
// PostgreSQL 17.10 across two separate clusters: a database holding one table
// and one ungranted cluster role is inspected with 4 CREATE ROLE statements
// before stokaro/ptah#1267 and 0 after, and `ptah-compat schema apply
// --dry-run` against an empty database in a second cluster plans those roles
// before and does not plan them after. Copying one cluster's roles into
// another is something a user could do before, so it stays reachable here.
//
// It is an environment variable and not a flag for the reason
// [go.5x5.cz/ptah/internal/atlashclrender.KeepAtlasRefusedBlocksEnvVar] gives:
// the conformance cli-surface tier asserts that `ptah-compat` registers
// exactly the flags the pinned Atlas community binary registers, so a flag
// that binary does not have would break the promise the surface exists to
// keep. Spelling and precedent:
// [go.5x5.cz/ptah/internal/atlassource.AllowExternalSchemaEnvVar].
//
// It changes the description only. Both reads are still performed, so the set
// the comparator takes existence from is identical either way, and turning
// this on can never make Ptah plan a CREATE ROLE it would otherwise skip.
const DescribeAllEnvVar = "PTAH_POSTGRES_INSPECT_ALL_ROLES"

// DescribeAll reports whether the opt-in variable is set to a true boolean
// value. Unset, empty, false and unparsable values all keep the default,
// mirroring how [go.5x5.cz/ptah/internal/atlassource] and
// [go.5x5.cz/ptah/internal/atlashclrender] read their own opt-ins.
func DescribeAll() bool {
	all, err := strconv.ParseBool(os.Getenv(DescribeAllEnvVar))
	return err == nil && all
}

// ReportUndescribed writes a note naming how many roles the description left
// out, and nothing at all when it left none out.
//
// The omission is reported rather than silent for the reason
// cmd/schema/test.go's dropClusterScopedTestState reports its own: an operator
// who is shown a description of their database must not be told less than the
// truth about it. A reader who sees no role block and no note cannot tell a
// server with no roles from a server whose roles this description declined to
// mention.
//
// It reports a COUNT and never the names. The names are precisely what
// [dbschematypes.DBSchema.RolesOutOfScope] is `json:"-"` to keep out of
// output: on a shared instance they are other tenants' role names, and a note
// that printed them would leak through the diagnostics stream what the
// description was scoped to stop leaking.
//
// w may be nil, which is how the inspect surfaces spell "no diagnostics
// stream"; the note is then dropped rather than panicking. Write errors are
// dropped too: a diagnostic that fails to print must not fail a read that
// succeeded.
func ReportUndescribed(w io.Writer, schema *dbschematypes.DBSchema) {
	if w == nil || schema == nil || len(schema.RolesOutOfScope) == 0 {
		return
	}
	fmt.Fprintf(w,
		"note: %s Ptah manages on this server %s not described, because nothing in the inspected"+
			" schemas refers to them; comparison still treats them as present, so none of them is"+
			" planned as a CREATE ROLE. Set %s=1 to describe every role Ptah manages.\n",
		countedRoles(len(schema.RolesOutOfScope)), pluralIs(len(schema.RolesOutOfScope)), DescribeAllEnvVar,
	)
}

// countedRoles renders a count with its singular or plural noun, in the shape
// cmd/schema/test.go's countedNoun uses for the same kind of note.
func countedRoles(count int) string {
	if count == 1 {
		return "1 role"
	}
	return fmt.Sprintf("%d roles", count)
}

// pluralIs agrees the verb with countedRoles.
func pluralIs(count int) string {
	if count == 1 {
		return "is"
	}
	return "are"
}
