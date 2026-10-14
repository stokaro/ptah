// Package accessscope refuses the access-control declarations a target cannot
// hold, before anything is rendered, compared or planned: a group declared as
// a principal of its own kind, a membership of one role in another, a grant on
// the database itself, and a grant on a scheme object such as a YDB topic.
// Each is gated by its capability key, and on
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
	"strings"

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
	if err := objectGrants(dialect, caps, database); err != nil {
		return err
	}
	if err := ydbacl.ValidateDeclared(dialect, database); err != nil {
		return fmt.Errorf("%w: %w", ptaherr.ErrUnsupportedFeature, err)
	}
	return nil
}

// objectGrants refuses a grant or revoke on a scheme object of a kind the
// target does not hold, such as a topic on a target without
// [capability.Topics], naming the first one by kind, name and grantee. A
// target that holds the kind is also one whose reader reads the kind's
// permissions, so a plan sees the grant it made. A kind no target holds is
// refused on every target.
func objectGrants(dialect string, caps capability.Capabilities, database *schemamodel.Database) error {
	grants := slices.Concat(database.Grants, database.RevokedGrants)
	slices.SortStableFunc(grants, func(a, b schemamodel.Grant) int {
		return cmp.Or(cmp.Compare(a.ObjectKind, b.ObjectKind), cmp.Compare(a.OnObject, b.OnObject), cmp.Compare(a.Role, b.Role))
	})
	normalized := platform.NormalizeDialect(dialect)
	for _, grant := range grants {
		if grant.OnObject == "" {
			continue
		}
		subject := fmt.Sprintf("the schema grants or revokes a privilege on %s %q to %q",
			strings.ToLower(grant.ObjectKind), grant.OnObject, grant.Role)
		key, known := ydbacl.ObjectCapability(grant.ObjectKind)
		if !known {
			return fmt.Errorf("%w: %s, and no target holds an object of kind %q", ptaherr.ErrUnsupportedFeature,
				subject, grant.ObjectKind)
		}
		if caps.Has(key) {
			continue
		}
		return &ptaherr.CapabilityError{
			Dialect: normalized,
			Feature: string(key),
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target",
				subject, key, normalized),
		}
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
