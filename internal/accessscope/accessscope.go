// Package accessscope refuses the access-control declarations a target cannot
// hold, before anything is rendered, compared or planned: a group declared as
// a principal of its own kind, a membership of one role in another, a grant on
// the database itself, a grant on a scheme object such as a topic, and a DENY.
// Each is gated by its capability key, and the DENY by the target's grammar.
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
)

// ValidateDeclared refuses a schema declaring what the target's access model
// cannot hold, and returns nil when the target holds all of it.
//
// A target without [capability.GroupPrincipals] refuses a role declared as a
// group, one without [capability.RoleMembership] a role's member_of, one
// without [capability.RoleMembershipAdmin] or [capability.RoleMembershipInheritSet]
// a membership that states the admin option or the inherit or set option, one
// without [capability.DatabaseGrants] a grant, revoke or deny on the database,
// and one whose grammar has no DENY a denied privilege. Each
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
		{capability.RoleMembershipAdmin, "states the admin option of its membership in a role: member %q",
			optionMembers(database.Roles, func(o schemamodel.RoleMembershipOptions) bool { return o.Admin != nil })},
		{capability.RoleMembershipInheritSet, "states the inherit or set option of its membership in a role: member %q",
			optionMembers(database.Roles, func(o schemamodel.RoleMembershipOptions) bool { return o.Inherit != nil || o.Set != nil })},
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
	if err := deniedGrants(dialect, database); err != nil {
		return err
	}
	return objectGrants(dialect, caps, database)
}

// deniedGrants refuses a denied privilege ([schemamodel.Database.DeniedGrants])
// on a target whose grammar has no DENY
// ([ptah.run/core/platform/descriptor.StatementForms.Deny]), naming the first
// grantee. A plan for such a target has no statement that withholds the
// privilege, so leaving the DENY out would give the grantee what the schema
// withholds wherever a grant or a membership carries it.
func deniedGrants(dialect string, database *schemamodel.Database) error {
	if len(database.DeniedGrants) == 0 {
		return nil
	}
	if described, ok := platform.Lookup(dialect); ok && described.Grammar.Forms.Deny {
		return nil
	}
	names := make([]string, 0, len(database.DeniedGrants))
	for _, denied := range database.DeniedGrants {
		names = append(names, denied.Role)
	}
	normalized := platform.NormalizeDialect(dialect)
	return fmt.Errorf("%w: the schema denies a privilege to %q, and the %s target has no DENY",
		ptaherr.ErrUnsupportedFeature, slices.MinFunc(names, cmp.Compare), normalized)
}

// ObjectCapability returns the capability under which a target holds objects
// of kind, a kind a grant names by [schemamodel.Grant.OnObject] such as TOPIC,
// matched without regard to case or spacing, and false for any other kind.
func ObjectCapability(kind string) (capability.Capability, bool) {
	key, known := objectCapabilities[strings.Join(strings.Fields(strings.ToUpper(kind)), " ")]
	return key, known
}

// objectCapabilities maps each kind of object a grant names by
// [schemamodel.Grant.OnObject] to the capability a target holds the kind
// under.
var objectCapabilities = map[string]capability.Capability{
	schemamodel.GrantObjectTopic:              capability.Topics,
	schemamodel.GrantObjectCoordinationNode:   capability.CoordinationNodes,
	schemamodel.GrantObjectSecret:             capability.Secrets,
	schemamodel.GrantObjectExternalDataSource: capability.ExternalDataSources,
	schemamodel.GrantObjectExternalTable:      capability.ExternalDataSources,
	schemamodel.GrantObjectAsyncReplication:   capability.AsyncReplication,
	schemamodel.GrantObjectTransfer:           capability.Transfers,
	schemamodel.GrantObjectStreamingQuery:     capability.StreamingQueries,
	schemamodel.GrantObjectResourcePool:       capability.ResourcePools,
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
		key, known := ObjectCapability(grant.ObjectKind)
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

// optionMembers names the roles with a membership that states an option
// stated reports.
func optionMembers(roles []schemamodel.Role, stated func(schemamodel.RoleMembershipOptions) bool) []string {
	var names []string
	for _, role := range roles {
		if slices.ContainsFunc(role.MemberOf, func(group string) bool { return stated(role.MemberOptions[group]) }) {
			names = append(names, role.Name)
		}
	}
	return names
}

func databaseGrantees(database *schemamodel.Database) []string {
	var names []string
	for _, grant := range slices.Concat(database.Grants, database.RevokedGrants, database.DeniedGrants) {
		if grant.OnDatabase {
			names = append(names, grant.Role)
		}
	}
	return names
}
