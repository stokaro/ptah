// Package reservedrole owns the single definition of a PostgreSQL role name
// Ptah never manages: the reserved pg_ prefix and the bootstrap superuser. The
// PostgreSQL reader excludes both from every role read through the SQL fragment
// this package renders, and the desired-schema surfaces refuse to plan them
// through the Go predicate this package exports, so the exclusion and the
// refusal cannot drift into disagreeing about what "reserved" means.
package reservedrole

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
)

const (
	// Prefix is the role-name prefix PostgreSQL reserves for system roles.
	// CREATE ROLE fails at SQLSTATE 42939 for any name carrying it, measured on
	// PostgreSQL 17.10 as a superuser: role name "pg_monitor_x" is reserved.
	//
	// The server compares the literal lowercase bytes, so the test here is a
	// plain prefix test and nothing else. Measured on the same server,
	// CREATE ROLE "PG_upper" succeeds, and so does CREATE ROLE "pgbouncer" --
	// the underscore is part of the prefix rather than a wildcard, which is the
	// defect stokaro/ptah#1291 had to fix at seven SQL sites.
	Prefix = "pg_"

	// BootstrapSuperuser is the role name a PostgreSQL cluster initialized by
	// initdb's default carries. It is not reserved by the server, only occupied:
	// CREATE ROLE "postgres" fails at SQLSTATE 42710 because the role already
	// exists, and succeeds on a cluster bootstrapped under another name.
	BootstrapSuperuser = "postgres"

	// likeEscape is the ESCAPE character ExcludeSQL declares, so the underscore
	// in Prefix is matched literally rather than as LIKE's single-character
	// wildcard.
	likeEscape = `\`
)

// AllowEnvVar plans a declared reserved role instead of refusing it, restoring
// what Ptah did before the refusal existed.
//
// It exists because the refusal removes a capability, and AGENTS.md
// ("Compatibility never removes a capability. Constitute it, do not discard
// it.") does not allow that to be the end of the story. Measured on two
// PostgreSQL 17.10 clusters: on one bootstrapped as "postgres",
// CREATE ROLE "postgres" fails at SQLSTATE 42710; on one bootstrapped as
// "admin" the same statement succeeds and the role appears in pg_roles, and
// Ptah's own dev-database materialization is what created it there. Declaring
// role "postgres" against a cluster that does not have one is therefore
// something a user could do before this refusal, so it stays reachable here.
//
// It is an environment variable and not a flag for the reason
// [go.5x5.cz/ptah/internal/rolescope.DescribeAllEnvVar] gives: the conformance
// cli-surface tier asserts that ptah-compat registers exactly the flags the
// pinned Atlas community binary registers, so a flag that binary does not have
// would break the promise the surface exists to keep.
//
// Setting it never makes a reserved role succeed that would otherwise fail. The
// reader still excludes the name from both of its reads, so the role still
// reads as absent and is still planned as a CREATE ROLE; the variable decides
// only whether Ptah refuses first or the server does.
const AllowEnvVar = "PTAH_ALLOW_RESERVED_ROLE_NAMES"

// Is reports whether name is a PostgreSQL role name Ptah never manages.
//
// This is the Go spelling of the exclusion [ExcludeSQL] renders, over the same
// two constants. A role Is reports true for is in neither DBSchema.Roles nor
// DBSchema.RolesOutOfScope, which is why declaring one has to be refused rather
// than compared: the comparator would find it in neither list and read it as
// absent.
func Is(name string) bool {
	return strings.HasPrefix(name, Prefix) || name == BootstrapSuperuser
}

// Rule names, for one role name, the reason the server refuses to create it.
// The two spellings fail for different reasons and a message that gave only one
// of them would misdescribe the other.
func Rule(name string) string {
	if strings.HasPrefix(name, Prefix) {
		return fmt.Sprintf(
			"PostgreSQL reserves the %q prefix for system roles and refuses CREATE ROLE at SQLSTATE 42939",
			Prefix,
		)
	}
	return "the bootstrap superuser is not a role Ptah manages, and CREATE ROLE for a role that" +
		" already exists fails at SQLSTATE 42710"
}

// ExcludeSQL renders the WHERE-clause fragment that keeps reserved role names
// out of a pg_roles read, for the role-name column named by column.
//
// The LIKE pattern is derived from [Prefix] rather than written out, so the
// escaping cannot be forgotten the way stokaro/ptah#1291 found it forgotten,
// and so a change to Prefix moves the SQL and [Is] together.
func ExcludeSQL(column string) string {
	return column + " NOT LIKE '" + likePattern() + "' ESCAPE '" + likeEscape + "'" +
		" AND " + column + " != '" + BootstrapSuperuser + "'"
}

// likePattern escapes every LIKE metacharacter in Prefix and appends the
// wildcard that makes it a prefix match.
func likePattern() string {
	var pattern strings.Builder
	for _, r := range Prefix {
		if r == '_' || r == '%' || r == '\\' {
			pattern.WriteString(likeEscape)
		}
		pattern.WriteRune(r)
	}
	pattern.WriteString("%")
	return pattern.String()
}

// Allowed reports whether the opt-in variable is set to a true boolean value.
// Unset, empty, false and unparsable values all keep the refusal, mirroring how
// [go.5x5.cz/ptah/internal/rolescope] and
// [go.5x5.cz/ptah/internal/atlassource] read their own opt-ins.
func Allowed() bool {
	allow, err := strconv.ParseBool(os.Getenv(AllowEnvVar))
	return err == nil && allow
}

// ValidateDeclared refuses a desired schema that declares a reserved role,
// naming every offending role and the rule it breaks.
//
// The refusal is the answer AGENTS.md gives to a construct Ptah cannot carry
// out -- "refuse loudly rather than accept and ignore" -- and it has to happen
// before anything is planned: a reserved role is in neither of the reader's
// lists, so the comparator reads it as absent and the plan becomes a CREATE
// ROLE the server is guaranteed to reject, discovered halfway through an apply
// rather than up front. See stokaro/ptah#1312.
//
// It applies to PostgreSQL-family targets, which are the only ones whose reader
// excludes these names and whose planner emits CREATE ROLE. dialect is the
// normalized or raw target dialect; an empty or non-PostgreSQL dialect declares
// nothing about PostgreSQL role names and is left alone.
func ValidateDeclared(dialect string, roles []goschema.Role) error {
	if !platform.IsPostgresFamily(dialect) || Allowed() {
		return nil
	}
	var refused []string
	for _, role := range roles {
		if !Is(role.Name) {
			continue
		}
		refused = append(refused, fmt.Sprintf("%q (%s)", role.Name, Rule(role.Name)))
	}
	if len(refused) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: desired schema declares reserved PostgreSQL %s %s;"+
			" Ptah manages reserved roles in neither direction, so the declaration is compared"+
			" against nothing and would be planned as a CREATE ROLE the server refuses;"+
			" rename the role, or set %s=1 to plan it anyway",
		ptaherr.ErrInvalidSchemaDiff,
		noun(len(refused)),
		strings.Join(refused, ", "),
		AllowEnvVar,
	)
}

// noun agrees the noun with the number of refused roles.
func noun(count int) string {
	if count == 1 {
		return "role"
	}
	return "roles"
}
