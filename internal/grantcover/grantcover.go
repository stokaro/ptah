// Package grantcover finds the grants wider than one table that keep a
// privilege a declared REVOKE on the table names. It serves targets where a
// REVOKE on a table takes only a grant on that table
// ([platform.TableRevokeTakesOnlyTableGrants]): there such a revoke cannot
// take the privilege, and the server refuses it when the role holds nothing
// on the table itself.
//
// The grant comparison asks [Find] before it leaves out a revoke on a table
// the plan creates, and the comparison's callers refuse, through [Validate], a
// declaration that revokes a privilege a wider grant keeps. Both read one
// predicate, so the revoke the comparison leaves out is never one the
// refusal would have named.
package grantcover

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// publicGrantee is the grantee whose privileges every role holds.
const publicGrantee = "PUBLIC"

// Cover is a grant wider than one table that gives a role a privilege a
// REVOKE on the table names.
type Cover struct {
	// Grantee holds the grant: the revoke's role, or PUBLIC.
	Grantee string
	// Grant is what the grantee holds, in the words a REVOKE of it uses:
	// "SELECT ON d2.*", "SELECT ON *.*" or "SELECT ANY TABLE".
	Grant string
}

// Find returns the grants in current wider than table that give role a
// privilege privileges names, ALL naming every privilege: a grant on the
// table's database, and a grant on every table, held by the role or by
// PUBLIC. A grant on the database that the plan removes is left out, because
// it keeps nothing once the plan runs: one desired revokes there, and one held
// by a role desired declares and does not grant there.
//
// known is false when current does not say which grants on every table
// exist, as a read made without the privilege to look does not. The covers
// are then the grants on the database alone, and a caller cannot tell that
// nothing else covers the table.
func Find(desired *schemamodel.Database, current *catalog.Database, role string, privileges []string, table string) (covers []Cover, known bool) {
	if current == nil {
		return nil, false
	}
	role = strings.TrimSpace(role)
	held := func(grantee string) bool {
		return grantee == role || strings.EqualFold(grantee, publicGrantee)
	}
	database, _, qualified := strings.Cut(table, ".")
	for _, grant := range current.Grants {
		onDatabase := strings.EqualFold(strings.TrimSpace(grant.ObjectType), "SCHEMA") &&
			(!qualified || grant.ObjectName == database)
		if !onDatabase || !held(grant.Role) || !names(privileges, grant.Privilege) || removed(desired, grant) {
			continue
		}
		covers = append(covers, Cover{Grantee: grant.Role, Grant: grant.Privilege + " ON " + grant.ObjectName + ".*"})
	}
	for _, grant := range current.WideGrants {
		if held(grant.Role) && names(privileges, grant.Privilege) {
			covers = append(covers, Cover{Grantee: grant.Role, Grant: grant.Grant})
		}
	}
	return covers, current.WideGrants != nil
}

// Validate refuses each declared revoke on a table that a grant wider than
// the table keeps, on a target where a REVOKE on a table takes only a grant on
// that table, and returns nil on every other target. The revoke is refused
// whether the plan creates the table or it exists: on an existing table the
// plan finds nothing under the table's name to revoke and reads as synced,
// and a member of the role keeps the privilege.
func Validate(dialect string, desired *schemamodel.Database, current *catalog.Database) error {
	if !platform.TableRevokeTakesOnlyTableGrants(dialect) || desired == nil || current == nil {
		return nil
	}
	var problems []error
	for _, revoke := range desired.RevokedGrants {
		if revoke.OnTable == "" {
			continue
		}
		covers, _ := Find(desired, current, revoke.Role, revoke.Privileges, revoke.OnTable)
		for _, cover := range covers {
			problems = append(problems, refusal(revoke, cover))
		}
	}
	return errors.Join(problems...)
}

// refusal says which grant keeps what revoke names, and where to revoke it.
func refusal(revoke schemamodel.Grant, cover Cover) error {
	role := strings.TrimSpace(revoke.Role)
	if cover.Grantee == role {
		return fmt.Errorf(
			"role %q holds %s, which covers table %s, so a revoke on the table cannot take it: "+
				"revoke %s from %q instead, or grant the privilege on each table that needs it",
			role, cover.Grant, revoke.OnTable, cover.Grant, role)
	}
	return fmt.Errorf(
		"%s holds %s, which gives role %q the privilege on table %s, so a revoke on the table cannot take it: "+
			"revoke %s from %s instead, or grant the privilege on each table that needs it",
		cover.Grantee, cover.Grant, role, revoke.OnTable, cover.Grant, cover.Grantee)
}

// names reports whether a revoke of privileges takes privilege.
func names(privileges []string, privilege string) bool {
	return slices.ContainsFunc(privileges, func(name string) bool {
		name = strings.ToUpper(strings.TrimSpace(name))
		return name == "ALL" || name == "ALL PRIVILEGES" || strings.EqualFold(name, strings.TrimSpace(privilege))
	})
}

// removed reports whether the plan removes a grant on a database: desired
// revokes the privilege there from the grant's grantee, or the grantee is a
// role desired declares and desired does not grant the privilege, or ALL,
// there. The grant comparison removes both, so neither keeps anything once the
// plan runs.
func removed(desired *schemamodel.Database, grant catalog.Grant) bool {
	if desired == nil {
		return false
	}
	there := func(declared schemamodel.Grant) bool {
		return declared.Role == grant.Role && declared.OnSchema == grant.ObjectName && names(declared.Privileges, grant.Privilege)
	}
	if slices.ContainsFunc(desired.RevokedGrants, there) {
		return true
	}
	declared := slices.ContainsFunc(desired.Roles, func(role schemamodel.Role) bool { return role.Name == grant.Role })
	return declared && !slices.ContainsFunc(desired.Grants, there)
}
