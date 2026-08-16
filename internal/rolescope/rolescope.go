// Package rolescope owns what Ptah does about roles being server-scoped while
// a schema description is not: the opt-in that restores the full cluster read
// on the same surface, the note that tells an operator which roles the default
// description left out, and the partition that keeps a dev-database
// materialization from re-creating a role its server already has.
package rolescope

import (
	"fmt"
	"io"

	"go.5x5.cz/ptah/core/platform"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/envbool"
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

// describeAll is the declaration of the variable, made once, in the package
// that owns it. See [go.5x5.cz/ptah/internal/envbool].
// It is [go.5x5.cz/ptah/internal/envbool.Gated]: describing every managed role
// on the server widens the description beyond anything the pinned community
// binary emits, which does not model roles.
var describeAll = envbool.New(DescribeAllEnvVar, false, envbool.Gated)

// DescribeAll reports whether the opt-in restores the full cluster read.
//
// Unset keeps the default and a valid false spelling keeps it too; an empty or
// unparsable value is a configuration error, because a description that
// silently stayed scoped after an operator believed they widened it is the one
// answer this must never give (stokaro/ptah#1334).
func DescribeAll() (bool, error) {
	return describeAll.Resolve()
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
func ReportUndescribed(w io.Writer, dialect string, schema *dbschematypes.DBSchema) {
	if w == nil || schema == nil || len(schema.RolesOutOfScope) == 0 {
		return
	}
	fmt.Fprintf(w,
		"note: %s Ptah manages on this server %s not described, because nothing in the inspected"+
			" schemas refers to them; comparison still treats them as present, so none of them is"+
			" planned as a CREATE ROLE.%s\n",
		countedRoles(len(schema.RolesOutOfScope)), pluralIs(len(schema.RolesOutOfScope)),
		describeAllRemedy(dialect),
	)
}

// describeAllRemedy returns the sentence naming the opt-out, and the empty
// string where there is none.
//
// The note used to end with it unconditionally, which was true while the
// PostgreSQL reader was the only one filling RolesOutOfScope. ClickHouse fills
// it now (stokaro/ptah#1025) and its reader consults no such variable, so an
// operator inspecting ClickHouse was told to set a PostgreSQL variable that
// would do nothing — a remedy that does not work is worse than no remedy,
// because the reader spends the attempt before learning it was never offered.
func describeAllRemedy(dialect string) string {
	if !platform.IsPostgresFamily(platform.NormalizeDialect(dialect)) {
		return ""
	}
	return fmt.Sprintf(" Set %s=1 to describe every role Ptah manages.", DescribeAllEnvVar)
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
