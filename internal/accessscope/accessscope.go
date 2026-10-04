// Package accessscope refuses the access-control declarations a target cannot
// hold, before anything is rendered, compared or planned: a group declared as
// a principal of its own kind, a membership of one role in another, and a
// grant on the database itself. Each is gated by its capability key, and on
// YDB the declaration is held to what YDB's users, groups and permissions can
// carry through [ydbacl.ValidateDeclared].
//
// The renderer and the schema comparison both call [ValidateDeclared], so a
// declaration one accepts the other cannot refuse.
package accessscope

import (
	"cmp"
	"fmt"
	"slices"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbacl"
)

// ValidateDeclared refuses a schema declaring what the target's access model
// cannot hold, and returns nil when the target holds all of it.
//
// A target without [capability.GroupPrincipals] refuses a role declared as a
// group, one without [capability.RoleMembership] a role's member_of, and one
// without [capability.DatabaseGrants] a grant or revoke on the database. Each
// refusal names the first offender by name, so two runs over one schema name
// the same one. None is skipped instead: a skipped group would leave the
// grants that name it without a principal, a skipped membership would leave a
// member without the permissions it was declared to hold, and a skipped
// database grant would leave a role without them.
func ValidateDeclared(dialect string, caps capability.Capabilities, database *schemamodel.Database) error {
	if database == nil {
		return nil
	}
	gates := []struct {
		key     capability.Capability
		subject string
		names   []string
	}{
		{capability.GroupPrincipals, "declares group %q", groupNames(database.Roles)},
		{capability.RoleMembership, "declares a membership of role %q", memberNames(database.Roles)},
		{capability.DatabaseGrants, "grants or revokes a privilege on the database to %q", databaseGrantees(database)},
	}
	for _, gate := range gates {
		if caps.Has(gate.key) || len(gate.names) == 0 {
			continue
		}
		normalized := platform.NormalizeDialect(dialect)
		first := slices.MinFunc(gate.names, cmp.Compare)
		return &ptaherr.CapabilityError{
			Dialect: normalized,
			Feature: string(gate.key),
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("the schema "+gate.subject+", which requires target capability %s, "+
				"unavailable on this %s target", first, gate.key, normalized),
		}
	}
	if err := ydbacl.ValidateDeclared(dialect, database); err != nil {
		return fmt.Errorf("%w: %w", ptaherr.ErrUnsupportedFeature, err)
	}
	return nil
}

func groupNames(roles []schemamodel.Role) []string {
	var names []string
	for _, role := range roles {
		if role.Group {
			names = append(names, role.Name)
		}
	}
	return names
}

func memberNames(roles []schemamodel.Role) []string {
	var names []string
	for _, role := range roles {
		if len(role.MemberOf) > 0 {
			names = append(names, role.Name)
		}
	}
	return names
}

func databaseGrantees(database *schemamodel.Database) []string {
	var names []string
	for _, grant := range slices.Concat(database.Grants, database.RevokedGrants) {
		if grant.OnDatabase {
			names = append(names, grant.Role)
		}
	}
	return names
}
