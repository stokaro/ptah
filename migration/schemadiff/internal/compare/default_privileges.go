package compare

import (
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// DefaultPrivileges compares PostgreSQL default privileges using the identifier
// rules its dialect name implies.
//
// Callers holding a live connection should use [DefaultPrivilegesWithSemantics]
// instead, so the two sides fold a role or a schema name the way the target
// does.
func DefaultPrivileges(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff) {
	DefaultPrivilegesWithSemantics(desired, current, diff, identifier.ForDialect(""))
}

// DefaultPrivilegesWithSemantics is [DefaultPrivileges] told which identifier
// rules the target has.
//
// # What makes two default privileges the same one
//
// The object's identity is the grantor, the schema, the object type and the
// grantee together, and the privilege names the entry within it. The grantor is
// part of the identity rather than payload: PostgreSQL keys pg_default_acl on
// it and refuses the statement from a non-member of that role, so two
// declarations differing only in grantor are two objects that need two
// statements. Dropping it would fold them into one map entry -- one role's
// privileges never issued, the other's never revoked, and an empty plan.
//
// # How an identity component is folded
//
// Three of the four components are identifier-shaped: the grantor, the schema
// and the grantee are names the target resolves under its own rules. All three
// go through the same [identifier.Semantics] fold, so the identity cannot fold
// one component and compare the next one verbatim -- an asymmetry whose two
// halves agree until somebody writes a name in a case the other half keeps.
// The object type is upper-cased instead, because it is a keyword.
//
// On PostgreSQL that fold is exact comparison, and deliberately: the renderer
// quotes every role and schema name it writes, so `App` is stored as written
// and really is a different role from `app`.
//
// # Which removals are planned
//
// A grantee is not an ownership signal here. pg_default_acl records grantee 0
// as PUBLIC, which no declaration ever creates as a role, so gating removals on
// the grantee being a declared role would make a default privilege granted to
// PUBLIC impossible to revoke -- the one grantee most worth being able to take
// back.
//
// The grantor is the signal instead, plus the declaration itself:
//
//   - a row whose grantor is a role the declaration manages is Ptah's to
//     revoke, whatever the grantee;
//   - a row under an identity the declaration names is Ptah's to trim, because
//     the author described that object and left this privilege out.
//
// A row that satisfies neither belongs to a role nobody declared and is left
// alone, which is the rule the grant comparator applies to a role it does not
// manage.
func DefaultPrivilegesWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) {
	declared := make(map[defaultPrivilegeIdentity]difftypes.DefaultPrivilegeRef)
	declaredObjects := make(map[defaultPrivilegeObject]bool)
	for _, privilege := range desired.DefaultPrivileges {
		for _, ref := range defaultPrivilegeRefsFromDeclaration(privilege) {
			key := newDefaultPrivilegeIdentity(ref, semantics)
			declaredObjects[key.object] = true
			// Two declarations that resolve to one entry are merged rather than
			// left to the map, which would keep whichever arrived last. The
			// grantable spelling wins, because that is what the server does with
			// two statements naming one identity: granting SELECT and then
			// SELECT WITH GRANT OPTION leaves one grantable row.
			if previous, collides := declared[key]; collides {
				ref.WithOption = ref.WithOption || previous.WithOption
			}
			declared[key] = ref
		}
	}

	managedGrantors := make(map[string]bool, len(desired.Roles))
	for _, role := range desired.Roles {
		managedGrantors[semantics.TableIdentityKey(strings.TrimSpace(role.Name))] = true
	}

	described := make(map[defaultPrivilegeIdentity]difftypes.DefaultPrivilegeRef, len(database.DefaultPrivileges))
	removable := make(map[defaultPrivilegeIdentity]difftypes.DefaultPrivilegeRef)
	for _, privilege := range database.DefaultPrivileges {
		ref := defaultPrivilegeRefFromDatabase(privilege)
		key := newDefaultPrivilegeIdentity(ref, semantics)
		if previous, collides := described[key]; collides {
			ref.WithOption = ref.WithOption || previous.WithOption
		}
		described[key] = ref
		if managedGrantors[key.object.grantor] || declaredObjects[key.object] {
			removable[key] = ref
		}
	}

	for key, ref := range declared {
		current, exists := described[key]
		if !exists {
			diff.DefaultPrivilegesAdded = append(diff.DefaultPrivilegesAdded, ref)
			continue
		}
		if ref.WithOption && !current.WithOption {
			diff.DefaultPrivilegeOptionsAdded = append(diff.DefaultPrivilegeOptionsAdded, ref)
		}
		if !ref.WithOption && current.WithOption {
			diff.DefaultPrivilegeOptionsRevoked = append(diff.DefaultPrivilegeOptionsRevoked, current)
		}
	}
	for key, ref := range removable {
		if _, stillDeclared := declared[key]; !stillDeclared {
			diff.DefaultPrivilegesRemoved = append(diff.DefaultPrivilegesRemoved, ref)
		}
	}

	// Every list above is built by ranging over a map, whose order Go
	// randomizes. Unsorted, the same two schemas produce a different migration
	// file on the next run.
	sortDefaultPrivilegeRefs(diff.DefaultPrivilegesAdded)
	sortDefaultPrivilegeRefs(diff.DefaultPrivilegesRemoved)
	sortDefaultPrivilegeRefs(diff.DefaultPrivilegeOptionsAdded)
	sortDefaultPrivilegeRefs(diff.DefaultPrivilegeOptionsRevoked)
}

// defaultPrivilegeRefsFromDeclaration explodes one declaration into the
// per-privilege grain both sides are compared at, which is the grain the
// catalog reports.
func defaultPrivilegeRefsFromDeclaration(
	privilege schemamodel.DefaultPrivilege,
) []difftypes.DefaultPrivilegeRef {
	privilege.Canonicalize()
	refs := make([]difftypes.DefaultPrivilegeRef, 0, len(privilege.Privileges))
	for _, granted := range privilege.Privileges {
		refs = append(refs, difftypes.DefaultPrivilegeRef{
			Grantor:    privilege.Grantor,
			Schema:     privilege.Schema,
			ObjectType: privilege.ObjectType,
			Grantee:    privilege.Grantee,
			Privilege:  granted.Privilege,
			WithOption: granted.WithOption,
		})
	}
	return refs
}

func defaultPrivilegeRefFromDatabase(privilege catalog.DefaultPrivilege) difftypes.DefaultPrivilegeRef {
	return difftypes.DefaultPrivilegeRef{
		Grantor:    strings.TrimSpace(privilege.Grantor),
		Schema:     strings.TrimSpace(privilege.Schema),
		ObjectType: strings.ToUpper(strings.TrimSpace(privilege.ObjectType)),
		Grantee:    strings.TrimSpace(privilege.Grantee),
		Privilege:  strings.ToUpper(strings.TrimSpace(privilege.Privilege)),
		WithOption: privilege.WithOption,
	}
}

// defaultPrivilegeObject is what makes two default privileges the same object,
// with each identifier-shaped component folded by the rule the target resolves
// it under. The object type is a keyword, so it is upper-cased instead.
//
// A struct rather than a joined string, for the reason the schemamodel key
// carries: folding trims and cases the components, it does not reject a role or
// schema name holding whatever separator a joined key would pick.
type defaultPrivilegeObject struct {
	grantor    string
	schema     string
	objectType string
	grantee    string
}

// defaultPrivilegeIdentity is one object and one privilege of it. Grantability
// is deliberately absent: a privilege granted plainly and the same privilege
// granted WITH GRANT OPTION are one row in the catalog, and comparing them as
// two entries is what turns a grant-option change into a GRANT plus a REVOKE.
type defaultPrivilegeIdentity struct {
	object    defaultPrivilegeObject
	privilege string
}

func newDefaultPrivilegeIdentity(
	ref difftypes.DefaultPrivilegeRef,
	semantics identifier.Semantics,
) defaultPrivilegeIdentity {
	return defaultPrivilegeIdentity{
		object: defaultPrivilegeObject{
			grantor:    semantics.TableIdentityKey(strings.TrimSpace(ref.Grantor)),
			schema:     semantics.TableIdentityKey(strings.TrimSpace(ref.Schema)),
			objectType: strings.ToUpper(strings.TrimSpace(ref.ObjectType)),
			grantee:    semantics.TableIdentityKey(strings.TrimSpace(ref.Grantee)),
		},
		privilege: strings.ToUpper(strings.TrimSpace(ref.Privilege)),
	}
}

// sortDefaultPrivilegeRefs orders a list by the object first and the privilege
// last, so the entries of one object stay together in a report and in a plan.
func sortDefaultPrivilegeRefs(refs []difftypes.DefaultPrivilegeRef) {
	sort.Slice(refs, func(i, j int) bool {
		return compareDefaultPrivilegeRefs(refs[i], refs[j]) < 0
	})
}

func compareDefaultPrivilegeRefs(left, right difftypes.DefaultPrivilegeRef) int {
	for _, values := range [][2]string{
		{left.Schema, right.Schema},
		{left.Grantor, right.Grantor},
		{left.ObjectType, right.ObjectType},
		{left.Grantee, right.Grantee},
		{left.Privilege, right.Privilege},
	} {
		if compared := strings.Compare(values[0], values[1]); compared != 0 {
			return compared
		}
	}
	return 0
}
