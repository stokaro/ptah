package postgres

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// CockroachDB has no SUPERUSER and no INHERIT role option. Measured on
// CockroachDB CCL v25.4.17 and v26.3.2, both refuse `CREATE ROLE r WITH
// NOSUPERUSER` and `CREATE ROLE r WITH INHERIT` as a syntax error, and refuse
// the same words after ALTER ROLE. The engine still reports both attributes
// through pg_roles: rolsuper is true for a member of admin and false
// otherwise, and rolinherit is true for every role.
//
// So the role options CockroachDB takes are written, and those two are left
// out. A declaration that leaving them out would not honor is refused by name
// instead: a superuser role, and a role that does not inherit. Creating either
// would give the author a role without what they declared, and the comparison
// would then report the same difference on every run.

// ValidateDeclaredRoles refuses a CockroachDB schema that declares a role
// attribute CockroachDB cannot express. It returns nil for every other
// dialect.
//
// Complete-schema validation runs it before any statement is rendered, and
// the renderer gives a direct CREATE ROLE the same answer. When several roles
// are refused, the error names the lexicographically first, so the answer
// does not depend on declaration order.
func ValidateDeclaredRoles(dialect string, roles []schemamodel.Role) error {
	if platform.NormalizeDialect(dialect) != platform.CockroachDB {
		return nil
	}
	offending := slices.DeleteFunc(slices.Clone(roles), func(role schemamodel.Role) bool {
		return len(roleAttributesOf(role).unexpressedOnCockroach()) == 0
	})
	if len(offending) == 0 {
		return nil
	}
	first := slices.MinFunc(offending, func(a, b schemamodel.Role) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return roleAttributesOf(first).cockroachError("CREATE ROLE", first.Name)
}

// roleAttributes are the declared role attributes CockroachDB has no option
// for.
type roleAttributes struct {
	superuser bool
	inherit   bool
}

func roleAttributesOf(role schemamodel.Role) roleAttributes {
	return roleAttributes{superuser: role.Superuser, inherit: role.Inherit}
}

// unexpressedOnCockroach names the declared attributes a CockroachDB role
// cannot carry, in a fixed order.
//
// Both validation and rendering ask this one method, because the two must
// refuse the same declarations: a role validation accepted and the renderer
// refused would fail halfway through a plan.
func (a roleAttributes) unexpressedOnCockroach() []string {
	var names []string
	if a.superuser {
		names = append(names, "SUPERUSER")
	}
	if !a.inherit {
		names = append(names, "NOINHERIT")
	}
	return names
}

// cockroachError refuses a role that declares an attribute CockroachDB has no
// role option for. It returns nil when the role declares none.
func (a roleAttributes) cockroachError(operation, name string) error {
	names := a.unexpressedOnCockroach()
	if len(names) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: cockroachdb: %s %s: declares %s, which CockroachDB has no role option for: "+
			"a CockroachDB role is a superuser only as a member of admin, and always inherits the privileges of the roles granted to it",
		ptaherr.ErrUnsupportedFeature, operation, name, strings.Join(names, ", "))
}

// cockroachAlterRoleError refuses an ALTER ROLE that changes an attribute
// CockroachDB has no role option for.
//
// A comparison reaches this when the server reports a role differently from
// its declaration: a role that was granted admin reads as a superuser.
// CockroachDB changes that only through membership, so the refusal says so
// rather than sending a statement the server rejects.
func cockroachAlterRoleError(name, attribute string) error {
	return fmt.Errorf(
		"%w: cockroachdb: ALTER ROLE %s: cannot change %s, which CockroachDB has no role option for: "+
			"a CockroachDB role is a superuser only as a member of admin, and always inherits the privileges of the roles granted to it",
		ptaherr.ErrUnsupportedFeature, name, attribute)
}
