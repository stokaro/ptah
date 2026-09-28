package mysqllike

import (
	"cmp"
	"fmt"
	"slices"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// ValidateDeclaredRoles refuses a MySQL-family schema whose declared roles
// Ptah cannot read or compare. Complete-schema validation and rendering share
// this check, while the visitors use the same error for direct AST rendering.
//
// When a schema declares several roles, the sentence names the lexicographically
// first of them rather than the first one parsed. Two gates answer the same
// schema -- this one, before a caller asks for SQL, and the MySQL planner, which
// sorts diff.RolesAdded and so refuses on the alphabetically first CREATE ROLE
// it renders. Naming the parse-order role here made the two disagree about the
// same schema, and moved the name whenever a declaration was reordered.
func ValidateDeclaredRoles(dialect string, caps capability.Capabilities, roles []schemamodel.Role) error {
	normalized := platform.NormalizeDialect(dialect)
	if (normalized != platform.MySQL && normalized != platform.MariaDB) || len(roles) == 0 {
		return nil
	}
	// The blanket refusal was right while nothing read a role back, and it is a
	// gate ahead of the renderer, so it had to learn the key at the same time:
	// leaving it unconditional would have made the preset claim a capability
	// the first gate still refused (stokaro/ptah#1762).
	if caps.Has(capability.RoleManagement) {
		return validateRoleAttributes(normalized, roles)
	}
	first := slices.MinFunc(roles, func(a, b schemamodel.Role) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return unsupportedRoleError(normalized, "CREATE ROLE", first.Name)
}

// validateRoleAttributes refuses a declaration carrying an attribute a
// MySQL-family role does not have, before any statement is rendered.
//
// The renderer refuses the same declaration when it reaches VisitCreateRole.
// Both gates exist because whole-schema rendering and planning enter by
// different doors, and they name the lexicographically first offending role so
// that two answers about one schema agree.
func validateRoleAttributes(dialect string, roles []schemamodel.Role) error {
	if first, found := firstRoleWhere(roles, declaresUserAttribute); found {
		return roleAttributeError(dialect, first.Name, "LOGIN, PASSWORD or another user attribute")
	}
	if first, found := firstRoleWhere(roles, declaresNoInherit); found {
		return noInheritError(dialect, first.Name)
	}
	return nil
}

// declaresUserAttribute reports whether a role asks for something only a USER
// has on this family.
func declaresUserAttribute(role schemamodel.Role) bool {
	return role.Login || role.Password != "" || role.Superuser ||
		role.CreateDB || role.CreateRole || role.Replication
}

// declaresNoInherit reports whether a role asks not to inherit the privileges
// of the roles granted to it.
//
// A MySQL-family role always inherits them, and the reader reports every role
// that way. Accepting the declaration would create a role that inherits, and
// every later comparison would then find the difference and plan the ALTER
// ROLE this family does not have (stokaro/ptah#3890). Every schema source
// defaults inherit to true, so what reaches this is a declaration that said
// false, or a [schemamodel.Role] built in Go that left the field at its zero
// value, which means NOINHERIT on PostgreSQL too.
func declaresNoInherit(role schemamodel.Role) bool {
	return !role.Inherit
}

// firstRoleWhere names the lexicographically first role the predicate holds
// for, so that the answer does not depend on declaration order.
func firstRoleWhere(roles []schemamodel.Role, predicate func(schemamodel.Role) bool) (schemamodel.Role, bool) {
	offending := slices.DeleteFunc(slices.Clone(roles), func(role schemamodel.Role) bool {
		return !predicate(role)
	})
	if len(offending) == 0 {
		return schemamodel.Role{}, false
	}
	return slices.MinFunc(offending, func(a, b schemamodel.Role) int {
		return cmp.Compare(a.Name, b.Name)
	}), true
}

// noInheritError refuses a role declared not to inherit.
//
// It is not [roleAttributeError], whose sentence is about the attributes a
// USER carries: inherit is an attribute a role here does have, fixed at true.
func noInheritError(dialect, name string) error {
	return fmt.Errorf(
		"%w: %s: role %q declares inherit=false, which a role cannot have here: "+
			"a role always passes on the privileges of the roles granted to it, and CREATE ROLE has no NOINHERIT",
		ptaherr.ErrUnsupportedFeature, dialect, name,
	)
}

func unsupportedRoleError(dialect, operation, name string) error {
	return fmt.Errorf(
		"%w: %s: %s %s: Ptah does not read or compare MySQL-family role state; manage roles outside Ptah for this target",
		ptaherr.ErrUnsupportedFeature,
		dialect,
		operation,
		name,
	)
}
