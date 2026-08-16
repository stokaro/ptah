// Package clickhouserbac decides which declared ClickHouse roles and grants
// Ptah will manage, and refuses the rest before anything reaches a server.
//
// ClickHouse's access control is not PostgreSQL's with different keywords. Four
// differences drive everything here, and each was measured against live
// clickhouse-server 24.10.4.191 and 26.7.3.19 rather than read off a manual:
//
//   - A role carries NO attributes. `system.roles` is (name, id, storage), there
//     is no LOGIN, PASSWORD, SUPERUSER, CREATEDB, CREATEROLE, INHERIT or
//     REPLICATION, and `CREATE ROLE ... COMMENT 'x'` is a syntax error. Every
//     attribute [goschema.Role] carries beyond a name is unrepresentable, so
//     declaring one is refused instead of dropped — a dropped PASSWORD would
//     leave an operator believing a credential was set.
//   - The server ABSORBS a narrower grant into a broader one. Granting SELECT on
//     `db`.`t` and on `db`.* leaves one row for `db`.*, in either order, and the
//     table-level grant is recorded nowhere. A schema declaring both can never
//     converge: the absorbed grant reads as missing on every inspection and the
//     plan re-issues it forever. [ValidateDeclared] refuses that pair.
//   - The server REWRITES some privilege spellings on the way in. `GRANT ALL`
//     records 45 individual rows on 26.7 and 39 on 24.10 and never reads back as
//     ALL; `GRANT SHOW FILESYSTEM CACHES` is accepted and records nothing at
//     all. Those spellings are refused too — see [rewrittenPrivileges] for the
//     measured table and for why the group nodes that DO round-trip are left
//     declarable.
//   - A GRANT names its target across BOTH users and roles, with no syntax to
//     say which is meant, and a USER of that name wins. Every declared grant
//     must therefore name a role the same schema declares; see
//     [undeclaredGranteeProblem].
//
// Everything this package refuses, it refuses BEFORE the target is mutated,
// which is what stokaro/ptah#1025 asks for. What it accepts is the surface that
// round-trips exactly: a role with a name, and a grant of named privileges on
// one database or one table, optionally WITH GRANT OPTION.
//
// # Known limitation
//
// A declared role whose name is also a live ClickHouse USER cannot be managed
// correctly, and this package does not detect it. `GRANT ... TO name` resolves
// to the user, the row lands in system.grants under user_name, and the reader
// filters it out because Ptah does not manage ClickHouse users — so the grant
// is re-issued on every run and the privilege accrues to a real account. Ptah
// reads no user list to check against: doing so would mean querying an access
// catalog it otherwise never touches, on every ClickHouse comparison, for a
// collision an operator creates by naming a role after an account. The
// non-convergence is at least visible — the plan never empties.
package clickhouserbac

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
)

// reservedRoleNames are principals ClickHouse ships or reserves. Declaring one
// would put Ptah in the position of managing the account it connects as.
//
// `default` is the stock image's administrative user. It is a USER rather than
// a role, so `CREATE ROLE default` would in fact succeed and create a role
// shadowing the user's name in every diagnostic that prints a principal — which
// is precisely the confusion worth refusing.
var reservedRoleNames = map[string]bool{
	"default": true,
}

// ValidateDeclared refuses declared ClickHouse roles and grants Ptah cannot
// manage, and returns nil for every other dialect.
//
// It runs at the entry points a declaration reaches before a server does — the
// renderer and the schema comparison — so the refusal arrives instead of a
// mutation, never after one. Non-ClickHouse dialects return nil immediately,
// which is how internal/reservedrole keeps PostgreSQL's rules from reaching
// MySQL and how this package keeps ClickHouse's from reaching PostgreSQL.
//
// defaultDatabase resolves an unqualified table name. Empty means there is no
// default to resolve against, and an unqualified name is then refused rather
// than attached to a database nobody named.
func ValidateDeclared(dialect string, roles []goschema.Role, grants []goschema.Grant, defaultDatabase string) error {
	if platform.NormalizeDialect(dialect) != platform.ClickHouse {
		return nil
	}
	problems := slices.Concat(
		roleProblems(roles),
		grantProblems(roles, grants, defaultDatabase),
	)
	return errors.Join(problems...)
}

// roleProblems reports every declared role ClickHouse cannot represent.
//
// Roles are visited in name order so that two runs over the same schema report
// the same first offender. A map iteration here would make the message depend
// on the hash seed, and a diagnostic that changes between runs is one nobody
// can pin in a test.
func roleProblems(roles []goschema.Role) []error {
	sorted := slices.SortedFunc(slices.Values(roles), func(a, b goschema.Role) int {
		return cmp.Compare(a.Name, b.Name)
	})
	var problems []error
	for _, role := range sorted {
		problems = append(problems, roleAttributeProblems(role)...)
	}
	return problems
}

func roleAttributeProblems(role goschema.Role) []error {
	var problems []error
	if reservedRoleNames[strings.ToLower(role.Name)] {
		problems = append(problems, fmt.Errorf(
			"role %q is a reserved ClickHouse principal and must not be declared", role.Name))
	}
	if role.Password != "" {
		// The value is deliberately not interpolated. It is a credential, and
		// this error travels to stderr, to a plan, and into whatever collects
		// them.
		problems = append(problems, fmt.Errorf(
			"role %q declares a password: ClickHouse roles carry no credentials, and Ptah does not manage ClickHouse users",
			role.Name))
	}
	for _, unsupported := range []struct {
		attribute string
		declared  bool
	}{
		{"login", role.Login},
		{"superuser", role.Superuser},
		{"createdb", role.CreateDB},
		{"createrole", role.CreateRole},
		{"replication", role.Replication},
	} {
		if unsupported.declared {
			problems = append(problems, fmt.Errorf(
				"role %q declares %s: a ClickHouse role carries no attributes",
				role.Name, unsupported.attribute))
		}
	}
	// Inherit is deliberately NOT refused, even though ClickHouse role
	// membership always inherits and an explicit inherit="false" is therefore
	// unrepresentable.
	//
	// The signal cannot be trusted. false is the Go zero value, so a Role built
	// in code — a test fixture, an internal conversion, any caller that does
	// not go through the annotation parser — is indistinguishable from one
	// whose author wrote inherit="false". Only core/goschema's annotation
	// parser defaults the field to true. A gate that fires on the default
	// rather than on the choice reports mostly false positives, which is worse
	// than the warning it would occasionally give: it would refuse schemas
	// nobody wrote an attribute on.
	return problems
}

// grantProblems reports every declared grant ClickHouse cannot represent, plus
// the pairs it would silently absorb.
func grantProblems(roles []goschema.Role, grants []goschema.Grant, defaultDatabase string) []error {
	var problems []error
	scopes := map[string][]Scope{}

	declared := make(map[string]bool, len(roles))
	for _, role := range roles {
		declared[role.Name] = true
	}

	sorted := slices.SortedFunc(slices.Values(grants), func(a, b goschema.Grant) int {
		return cmp.Or(
			cmp.Compare(a.Role, b.Role),
			cmp.Compare(strings.Join(a.Privileges, ","), strings.Join(b.Privileges, ",")),
			cmp.Compare(a.OnSchema+"|"+a.OnTable, b.OnSchema+"|"+b.OnTable),
		)
	})
	for _, grant := range sorted {
		if !declared[grant.Role] {
			problems = append(problems, undeclaredGranteeProblem(grant.Role))
		}
		scope, err := ScopeOf(grant, defaultDatabase)
		if err != nil {
			problems = append(problems, err)
			continue
		}
		problems = append(problems, privilegeProblems(grant, scope)...)
		for _, privilege := range grant.Privileges {
			key := grant.Role + "\x00" + strings.ToUpper(privilege)
			scopes[key] = append(scopes[key], scope)
		}
	}
	return append(problems, absorptionProblems(scopes)...)
}

// undeclaredGranteeProblem refuses a grant whose grantee the schema does not
// declare as a role.
//
// ClickHouse resolves the target of `GRANT ... TO name` by name across BOTH
// users and roles, and there is no syntax to say which one is meant. Measured
// on 26.7.3.19, the two outcomes are:
//
//   - no principal of that name exists: Code 511 UNKNOWN_ROLE, spelled
//     "There is no role x in user directories" — the migration fails partway
//     through, after the statements before it already ran.
//   - a USER of that name exists: the GRANT SUCCEEDS and lands on the user. The
//     row appears in system.grants under user_name, which the reader filters
//     out because Ptah does not manage ClickHouse users, so the grant is
//     invisible to every later inspection: the plan re-issues it forever, and a
//     real account quietly holds a privilege nobody declared for it.
//
// The second is why this is refused offline rather than left to the server. A
// declaration that names its own roles has neither outcome available: the role
// is created before the grants are issued, in the same plan, by
// [go.5x5.cz/ptah/internal/planner/dialects/clickhouse].
//
// One shape this cannot catch is a declared role whose name is ALSO a live
// ClickHouse user, since resolution still prefers the user. Detecting that
// needs a read of system.users, which Ptah does not otherwise do; see the
// package documentation.
func undeclaredGranteeProblem(role string) error {
	return fmt.Errorf(
		"grant names role %q, which this schema does not declare: ClickHouse resolves a grantee by name across users and roles, "+
			"so the grant would either fail with UNKNOWN_ROLE or land on a USER of that name — declare the role, or remove the grant",
		role)
}

// rewrite is what the server records in place of a privilege it accepts but
// does not store as written. An empty field means the spelling reads back as
// itself at that scope and is therefore declarable there.
type rewrite struct {
	// atDatabase is what `GRANT x ON db.*` records instead of x.
	atDatabase string
	// atTable is what `GRANT x ON db.t` records instead of x.
	atTable string
}

// rewrittenPrivileges are the spellings ClickHouse ACCEPTS and then records as
// something other than what was written.
//
// That is the whole rule, and it is deliberately narrower than "refuse every
// group in the privilege tree". ClickHouse's `system.privileges` has 22 group
// nodes, and most of them DO round-trip: `GRANT ALTER TABLE ON db.t` reads back
// as one ALTER TABLE row on both declared lines. Refusing them would remove
// grants that converge perfectly. Equally, a spelling the SERVER refuses --
// INTROSPECTION, SYSTEM RELOAD, SYSTEM DROP CACHE at either scope, or
// ALTER DATABASE at table scope -- needs no gate here: the operator gets
// `Code: 509 ... cannot be granted on the database level`, which names the
// problem more precisely than Ptah could. What Ptah has to catch is the middle
// case, where the GRANT succeeds and the row that lands has a different name,
// because THAT is the shape that reads back as missing on every inspection and
// makes the planner re-issue the grant forever.
//
// Measured by granting each spelling to a fresh role at each scope and reading
// system.grants back, on clickhouse-server 26.7.3.19 and 24.10.4.191:
//
//	spelling                on db.*                      on db.t
//	ALL / ALL PRIVILEGES    45 rows (39 on 24.10)        45 rows (39 on 24.10)
//	SHOW                    SHOW                         3 rows: SHOW TABLES, ...
//	SHOW FILESYSTEM CACHES  NOTHING AT ALL               NOTHING AT ALL
//	SHOW ACCESS             SHOW ROW POLICIES            SHOW ROW POLICIES
//	ACCESS MANAGEMENT       4 rows: CREATE ROW POLICY,…  4 rows
//	ALTER                   ALTER                        2 rows: ALTER TABLE, ALTER VIEW
//	CREATE                  4 rows: CREATE DATABASE,…    3 rows
//	DROP                    4 rows: DROP DATABASE,…      3 rows
//	SYSTEM                  23 rows (18 on 24.10)        22 rows (17 on 24.10)
//	SYSTEM FLUSH            2 rows on 26.7;              same
//	                        SYSTEM FLUSH DISTRIBUTED on 24.10
//	SYSTEM SENDS            SYSTEM SENDS                 SYSTEM SENDS
//	ALTER TABLE, ALTER VIEW, ALTER COLUMN, ALTER INDEX, ALTER STATISTICS,
//	ALTER PROJECTION, ALTER CONSTRAINT, SELECT, INSERT, OPTIMIZE, TRUNCATE,
//	dictGet, SHOW TABLES, CREATE TABLE, DROP TABLE: read back as written.
//
// SHOW FILESYSTEM CACHES is the entry that most justifies the check. The GRANT
// is accepted, returns no error, and records nothing anywhere: an operator who
// declared it would be told the grant applied and would hold no privilege.
//
// SYSTEM FLUSH is the entry that justifies refusing at BOTH scopes rather than
// at the scopes one server happened to rewrite. It round-trips on 24.10 under
// its own name and does not on 26.7, so a declaration Ptah accepted for the
// older line would stop converging on an upgrade. A privilege is declarable
// only if it round-trips on every line this preset covers.
var rewrittenPrivileges = map[string]rewrite{
	"ALL":                    {atDatabase: "every individual privilege on the target", atTable: "every individual privilege on the target"},
	"ALL PRIVILEGES":         {atDatabase: "every individual privilege on the target", atTable: "every individual privilege on the target"},
	"SHOW":                   {atTable: "SHOW TABLES, SHOW COLUMNS and SHOW DICTIONARIES"},
	"SHOW FILESYSTEM CACHES": {atDatabase: "nothing at all", atTable: "nothing at all"},
	"SHOW ACCESS":            {atDatabase: "SHOW ROW POLICIES", atTable: "SHOW ROW POLICIES"},
	"ACCESS MANAGEMENT":      {atDatabase: "the four ROW POLICY privileges", atTable: "the four ROW POLICY privileges"},
	"ALTER":                  {atTable: "ALTER TABLE and ALTER VIEW"},
	"CREATE":                 {atDatabase: "CREATE DATABASE, CREATE TABLE, CREATE VIEW and CREATE DICTIONARY", atTable: "CREATE TABLE, CREATE VIEW and CREATE DICTIONARY"},
	"DROP":                   {atDatabase: "DROP DATABASE, DROP TABLE, DROP VIEW and DROP DICTIONARY", atTable: "DROP TABLE, DROP VIEW and DROP DICTIONARY"},
	"SYSTEM":                 {atDatabase: "every individual SYSTEM privilege", atTable: "every individual SYSTEM privilege"},
	"SYSTEM FLUSH":           {atDatabase: "the individual SYSTEM FLUSH privileges", atTable: "the individual SYSTEM FLUSH privileges"},
}

// recordedAs returns what the server stores instead of privilege at scope, and
// the empty string when the spelling round-trips there.
func (r rewrite) recordedAs(scope Scope) string {
	if scope.Table == "" {
		return r.atDatabase
	}
	return r.atTable
}

func privilegeProblems(grant goschema.Grant, scope Scope) []error {
	if len(grant.Privileges) == 0 {
		return []error{fmt.Errorf("grant to role %q names no privilege", grant.Role)}
	}
	var problems []error
	for _, privilege := range grant.Privileges {
		normalized := strings.ToUpper(strings.TrimSpace(privilege))
		if normalized == "NONE" {
			problems = append(problems, fmt.Errorf(
				"grant to role %q declares privilege %q: it names no privilege; omit the grant instead",
				grant.Role, privilege))
			continue
		}
		if recorded := rewrittenPrivileges[normalized].recordedAs(scope); recorded != "" {
			problems = append(problems, fmt.Errorf(
				"grant to role %q declares privilege %q on %s: ClickHouse records it as %s, so it never reads back as written — declare the individual privileges instead",
				grant.Role, privilege, scope.Describe(), recorded))
			continue
		}
		if strings.Contains(privilege, "(") {
			problems = append(problems, fmt.Errorf(
				"grant to role %q declares column-scoped privilege %q: Ptah manages ClickHouse grants at database and table scope",
				grant.Role, privilege))
		}
	}
	return problems
}

// absorptionProblems reports the declared pairs the server would collapse.
//
// The check is per (role, privilege) because absorption is per privilege: SELECT
// on `db`.* and INSERT on `db`.`t` coexist happily, while SELECT on both does
// not. Scopes are compared with [Scope.Contains], the same rule the server
// applies, so the refusal covers database-over-table and an exact duplicate
// alike.
func absorptionProblems(scopes map[string][]Scope) []error {
	var problems []error
	for _, key := range slices.Sorted(maps.Keys(scopes)) {
		declared := scopes[key]
		role, privilege, _ := strings.Cut(key, "\x00")
		for i := range declared {
			for j := range declared {
				if i == j {
					continue
				}
				if !declared[i].Contains(declared[j]) {
					continue
				}
				problems = append(problems, fmt.Errorf(
					"role %q declares %s on both %s and %s: ClickHouse absorbs the narrower grant into the broader one, so the pair can never converge — declare only %s",
					role, privilege, declared[i].Describe(), declared[j].Describe(), declared[i].Describe()))
				return problems
			}
		}
	}
	return problems
}
