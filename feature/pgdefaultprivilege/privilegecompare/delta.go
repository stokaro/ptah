// Package privilegecompare compares default-privilege deltas for the
// pgdefaultprivilege owner. It uses captured state and performs no server reads.
package privilegecompare

import (
	"slices"
	"sort"
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/pgdefaultacl"
	"ptah.run/internal/pgprivilege"
)

const allPrivilege = "ALL"

type entry struct {
	Grantor    string
	Schema     string
	ObjectType string
	Grantee    string
	Privilege  string
	WithOption bool
}

type observedRow struct {
	entry
	Revoked bool
}

type declaration struct {
	pgdefaultprivilege.Identity
	*pgdefaultprivilege.Desired
}

func (d *declaration) Canonicalize() {
	d.Desired = d.Desired.Canonical()
}

type delta struct {
	grants         []entry
	revokes        []entry
	optionsAdded   []entry
	optionsRevoked []entry
}

// calculate compares grants and built-in revocations after source knowledge
// establishes the inputs. Removal authority comes from a declared grantor or
// identity, or an explicit revoke. A grantee never supplies that authority.
func calculate(desired []declaration, database []observedRow, roles []string, semantics identifier.Semantics) delta {
	diff := new(delta)

	declared, declaredObjects := declaredRights(desired, semantics)

	revoked := revokedDefaultPrivileges(desired, semantics)

	managedGrantors := make(map[string]bool, len(roles))
	for _, role := range roles {
		managedGrantors[semantics.TableIdentityKey(role)] = true
	}

	current := currentDefaultPrivileges{
		described:      make(map[defaultPrivilegeIdentity]entry, len(database)),
		revoked:        make(map[defaultPrivilegeIdentity]entry),
		revokedObjects: make(map[defaultPrivilegeObject]bool),
	}
	removable := make(map[defaultPrivilegeIdentity]entry)
	for _, privilege := range database {
		ref := defaultPrivilegeRefFromDatabase(privilege)
		key := newDefaultPrivilegeIdentity(ref, semantics)
		if privilege.Revoked {
			current.revoked[key] = ref
			current.revokedObjects[key.object] = true
			continue
		}
		if previous, collides := current.described[key]; collides {
			ref.WithOption = ref.WithOption || previous.WithOption
		}
		current.described[key] = ref
		if managedGrantors[key.object.grantor] || declaredObjects[key.object] || revokes(revoked, key) {
			removable[key] = ref
		}
	}

	planDefaultPrivilegeAdditions(declared, current, diff)
	for key, ref := range removable {
		if !declaredDefaultPrivilege(key, declared) {
			diff.revokes = append(diff.revokes, ref)
		}
	}
	planBuiltinRevokes(desired, semantics, current, diff)
	for key, ref := range current.revoked {
		owned := managedGrantors[key.object.grantor] || declaredObjects[key.object]
		if !owned || revokes(revoked, key) || declaredDefaultPrivilege(key, declared) {
			continue
		}
		diff.grants = append(diff.grants, ref)
	}
	sortDefaultPrivilegeRefs(diff.grants)
	sortDefaultPrivilegeRefs(diff.revokes)
	sortDefaultPrivilegeRefs(diff.optionsAdded)
	sortDefaultPrivilegeRefs(diff.optionsRevoked)
	return *diff
}

func declaredRights(desired []declaration, semantics identifier.Semantics) (map[defaultPrivilegeIdentity]entry, map[defaultPrivilegeObject]bool) {
	declared := make(map[defaultPrivilegeIdentity]entry)
	declaredObjects := make(map[defaultPrivilegeObject]bool)
	for _, privilege := range desired {
		identity := privilegeEntry(privilege.Identity, pgdefaultprivilege.Grant{})
		// A revoke-only declaration names the rights to remove, not a complete
		// desired ACL. An empty declaration explicitly restores the default.
		if len(privilege.Privileges) != 0 || len(privilege.Revoked) == 0 {
			declaredObjects[newDefaultPrivilegeIdentity(identity, semantics).object] = true
		}
		for _, ref := range defaultPrivilegeRefsFromDeclaration(privilege) {
			key := newDefaultPrivilegeIdentity(ref, semantics)
			if previous, collides := declared[key]; collides {
				ref.WithOption = ref.WithOption || previous.WithOption
			}
			declared[key] = ref
		}
	}

	return declared, declaredObjects
}

type currentDefaultPrivileges struct {
	described      map[defaultPrivilegeIdentity]entry
	revoked        map[defaultPrivilegeIdentity]entry
	revokedObjects map[defaultPrivilegeObject]bool
}

func (c currentDefaultPrivileges) builtinHeld(key defaultPrivilegeIdentity) bool {
	if key.object.schema != "" ||
		!pgdefaultacl.Builtin(key.object.objectType, key.object.grantor, key.object.grantee, key.privilege) {
		return false
	}
	if key.privilege == allPrivilege {
		return !c.revokedObjects[key.object]
	}
	_, revoked := c.revoked[key]
	_, revokedAll := c.revoked[defaultPrivilegeIdentity{object: key.object, privilege: allPrivilege}]
	return !revoked && !revokedAll
}

// collapseAll removes redundant declarations while retaining a specific grant
// option beside a plain ALL. Grant options combine, as repeated GRANTs do.
func collapseAll(declared map[defaultPrivilegeIdentity]entry) {
	for key, ref := range declared {
		if key.privilege == allPrivilege {
			continue
		}
		all, exists := declared[defaultPrivilegeIdentity{object: key.object, privilege: allPrivilege}]
		if exists && (all.WithOption || !ref.WithOption) {
			delete(declared, key)
		}
	}
}

func planDefaultPrivilegeAdditions(
	declared map[defaultPrivilegeIdentity]entry,
	current currentDefaultPrivileges,
	diff *delta,
) {
	collapseAll(declared)
	for key, ref := range declared {
		held, exists := heldDefaultPrivileges(key, current)
		if !exists {
			diff.grants = append(diff.grants, ref)
		}
		if exists && ref.WithOption && slices.ContainsFunc(held, func(held entry) bool { return !held.WithOption }) {
			diff.optionsAdded = append(diff.optionsAdded, ref)
		}
		if ref.WithOption {
			continue
		}
		for _, present := range held {
			specific := declared[defaultPrivilegeIdentity{object: key.object, privilege: present.Privilege}]
			if present.WithOption && !specific.WithOption {
				// Preserve the identity of the requested operation, including when held
				// comes from the built-in defaults and has only a privilege name.
				revoke := ref
				revoke.Privilege = present.Privilege
				diff.optionsRevoked = append(diff.optionsRevoked, revoke)
			}
		}
	}
}

func heldDefaultPrivileges(
	key defaultPrivilegeIdentity,
	current currentDefaultPrivileges,
) ([]entry, bool) {
	if key.privilege != allPrivilege {
		if ref, exists := current.held(key); exists {
			return []entry{ref}, true
		}
		return nil, false
	}
	portable := pgprivilege.Portable(key.object.objectType)
	if len(portable) == 0 {
		return nil, false
	}
	held := make([]entry, 0, len(portable))
	complete := true
	all, hasAll := current.described[key]
	for _, privilege := range portable {
		ref, exists := current.held(defaultPrivilegeIdentity{object: key.object, privilege: privilege})
		if !exists && !hasAll {
			complete = false
			continue
		}
		ref.Privilege = privilege
		ref.WithOption = ref.WithOption || hasAll && all.WithOption
		held = append(held, ref)
	}
	// Catalog rows establish release-specific privileges such as MAINTAIN even
	// when the portable minimum cannot require them. Their options still matter.
	for existing, ref := range current.described {
		if existing.object == key.object && existing.privilege != allPrivilege && !slices.Contains(portable, existing.privilege) {
			held = append(held, ref)
		}
	}
	return held, complete
}
func (c currentDefaultPrivileges) held(key defaultPrivilegeIdentity) (entry, bool) {
	if ref, exists := c.described[key]; exists {
		return ref, true
	}
	if !c.builtinHeld(key) {
		return entry{}, false
	}
	return entry{Privilege: key.privilege}, true
}
func planBuiltinRevokes(
	desired []declaration,
	semantics identifier.Semantics,
	current currentDefaultPrivileges,
	diff *delta,
) {
	for _, declaration := range desired {
		declaration.Canonicalize()
		if declaration.Schema != "" {
			continue
		}
		for _, name := range revokedNames(declaration) {
			base := entry{
				Grantor: declaration.Grantor, ObjectType: declaration.ObjectType, Grantee: declaration.Grantee,
			}
			diff.revokes = append(diff.revokes,
				builtinRevokesOf(base, name, semantics, current)...)
		}
	}
}
func builtinRevokesOf(
	base entry,
	name string,
	semantics identifier.Semantics,
	current currentDefaultPrivileges,
) []entry {
	candidates := []string{name}
	if name == allPrivilege {
		candidates = pgprivilege.Portable(base.ObjectType)
	}
	for _, candidate := range candidates {
		ref := base
		ref.Privilege = candidate
		key := newDefaultPrivilegeIdentity(ref, semantics)
		if !pgdefaultacl.Builtin(key.object.objectType, key.object.grantor, key.object.grantee, candidate) {
			continue
		}
		if _, described := current.described[key]; described || !current.builtinHeld(key) {
			continue
		}
		ref.Privilege = name
		return []entry{ref}
	}
	return nil
}
func revokedNames(declaration declaration) []string {
	if declaration.Schema == "" && declaration.Grantee == declaration.Grantor &&
		pgprivilege.NamesAll(declaration.ObjectType, declaration.Revoked) {
		return []string{allPrivilege}
	}
	return declaration.Revoked
}
func declaredDefaultPrivilege(
	key defaultPrivilegeIdentity,
	declared map[defaultPrivilegeIdentity]entry,
) bool {
	if _, exists := declared[key]; exists {
		return true
	}
	if _, exists := declared[defaultPrivilegeIdentity{object: key.object, privilege: allPrivilege}]; !exists {
		return false
	}
	return slices.Contains(pgprivilege.All(key.object.objectType), key.privilege)
}
func revokedDefaultPrivileges(
	desired []declaration,
	semantics identifier.Semantics,
) map[defaultPrivilegeIdentity]bool {
	revoked := make(map[defaultPrivilegeIdentity]bool)
	for _, privilege := range desired {
		privilege.Canonicalize()
		for _, name := range revokedNames(privilege) {
			ref := entry{
				Grantor: privilege.Grantor, Schema: privilege.Schema, ObjectType: privilege.ObjectType,
				Grantee: privilege.Grantee, Privilege: name,
			}
			revoked[newDefaultPrivilegeIdentity(ref, semantics)] = true
		}
	}
	return revoked
}
func revokes(revoked map[defaultPrivilegeIdentity]bool, key defaultPrivilegeIdentity) bool {
	return revoked[key] || revoked[defaultPrivilegeIdentity{object: key.object, privilege: allPrivilege}]
}
func defaultPrivilegeRefsFromDeclaration(
	privilege declaration,
) []entry {
	privilege.Canonicalize()
	refs := make([]entry, 0, len(privilege.Privileges))
	for _, granted := range privilege.Privileges {
		refs = append(refs, entry{
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

func defaultPrivilegeRefFromDatabase(privilege observedRow) entry {
	return entry{
		Grantor:    privilege.Grantor,
		Schema:     privilege.Schema,
		ObjectType: strings.ToUpper(strings.TrimSpace(privilege.ObjectType)),
		Grantee:    privilege.Grantee,
		Privilege:  strings.ToUpper(strings.TrimSpace(privilege.Privilege)),
		WithOption: privilege.WithOption,
	}
}

type defaultPrivilegeObject struct {
	grantor    string
	schema     string
	objectType string
	grantee    string
}
type defaultPrivilegeIdentity struct {
	object    defaultPrivilegeObject
	privilege string
}

func newDefaultPrivilegeIdentity(
	ref entry,
	semantics identifier.Semantics,
) defaultPrivilegeIdentity {
	return defaultPrivilegeIdentity{
		object: defaultPrivilegeObject{
			grantor:    semantics.TableIdentityKey(ref.Grantor),
			schema:     semantics.TableIdentityKey(ref.Schema),
			objectType: strings.ToUpper(strings.TrimSpace(ref.ObjectType)),
			grantee:    semantics.TableIdentityKey(ref.Grantee),
		},
		privilege: strings.ToUpper(strings.TrimSpace(ref.Privilege)),
	}
}
func sortDefaultPrivilegeRefs(refs []entry) {
	sort.Slice(refs, func(i, j int) bool {
		return compareDefaultPrivilegeRefs(refs[i], refs[j]) < 0
	})
}

func compareDefaultPrivilegeRefs(left, right entry) int {
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
