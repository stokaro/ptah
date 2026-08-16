// Package clickhouserbac decides which declared ClickHouse roles and grants
// Ptah will manage, and refuses the rest before anything reaches a server.
//
// ClickHouse's access control is not PostgreSQL's with different keywords. Three
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
//   - `GRANT ALL` expands to 45 individual rows and never reads back as ALL, so
//     it is refused for the same reason.
//
// Everything this package refuses, it refuses BEFORE the target is mutated,
// which is what stokaro/ptah#1025 asks for. What it accepts is the surface that
// round-trips exactly: a role with a name, and a grant of named privileges on
// one database or one table, optionally WITH GRANT OPTION.
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
		grantProblems(grants, defaultDatabase),
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
func grantProblems(grants []goschema.Grant, defaultDatabase string) []error {
	var problems []error
	scopes := map[string][]Scope{}

	sorted := slices.SortedFunc(slices.Values(grants), func(a, b goschema.Grant) int {
		return cmp.Or(
			cmp.Compare(a.Role, b.Role),
			cmp.Compare(strings.Join(a.Privileges, ","), strings.Join(b.Privileges, ",")),
			cmp.Compare(a.OnSchema+"|"+a.OnTable, b.OnSchema+"|"+b.OnTable),
		)
	})
	for _, grant := range sorted {
		scope, err := ScopeOf(grant, defaultDatabase)
		if err != nil {
			problems = append(problems, err)
			continue
		}
		problems = append(problems, privilegeProblems(grant)...)
		for _, privilege := range grant.Privileges {
			key := grant.Role + "\x00" + strings.ToUpper(privilege)
			scopes[key] = append(scopes[key], scope)
		}
	}
	return append(problems, absorptionProblems(scopes)...)
}

// refusedPrivileges are the spellings that name a set rather than a privilege.
//
// ALL is refused because the server expands it: `GRANT ALL ON db.*` records 45
// individual rows and reads back as an enumerated list, so a schema declaring
// ALL compares a one-element desired set against a 45-element live set forever.
// NONE is refused because it names the absence of a grant, which is expressed
// by not declaring one.
var refusedPrivileges = map[string]string{
	"ALL":            "expands to every individual privilege on the target and never reads back as ALL",
	"ALL PRIVILEGES": "expands to every individual privilege on the target and never reads back as ALL",
	"NONE":           "names no privilege; omit the grant instead",
}

func privilegeProblems(grant goschema.Grant) []error {
	if len(grant.Privileges) == 0 {
		return []error{fmt.Errorf("grant to role %q names no privilege", grant.Role)}
	}
	var problems []error
	for _, privilege := range grant.Privileges {
		normalized := strings.ToUpper(strings.TrimSpace(privilege))
		if reason, refused := refusedPrivileges[normalized]; refused {
			problems = append(problems, fmt.Errorf(
				"grant to role %q declares privilege %q: it %s", grant.Role, privilege, reason))
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
