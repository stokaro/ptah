package compare

import (
	"slices"
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgdefaultacl"
	"ptah.run/internal/pgprivilege"
	"ptah.run/migration/schemadiff/difftypes"
)

// DefaultPrivileges compares PostgreSQL default privileges using the identifier
// rules its dialect name implies.
//
// Callers holding a live connection should use [DefaultPrivilegesWithSemantics]
// instead, so the two sides fold a role or a schema name the way the target
// does.
func DefaultPrivileges(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff) {
	DefaultPrivilegesWithSemantics(desired, current, diff, identifier.ForDialect(""), CoverageOf(desired, current))
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
// manage -- unless the declaration revokes it. A revoked privilege says the
// privilege is absent, so it is removed whoever the grantor is.
//
// # The global default
//
// A default privilege without a schema is the global default, and it starts
// from the built-in one: the owner holds every privilege of the object class,
// PUBLIC holds EXECUTE on functions and USAGE on types. The read reports a
// global row as its difference from that, a granted entry for what it adds and
// a revoked entry for what it takes away, so the comparison knows the
// built-in default too ([pgdefaultacl.Builtin]):
//
//   - a declared grant of a built-in privilege is held unless the database
//     revoked it;
//   - a declared revoke of a built-in privilege is planned unless the database
//     already revoked it -- a database with no row at all holds PUBLIC's
//     EXECUTE, so a declared REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC is planned
//     there;
//   - a built-in privilege the database revoked and the declaration does not is
//     granted back, under the rule removals follow: when the grantor is a role
//     the declaration manages or the object is one it declares.
//
// A global revoke from the owner that names every privilege of the object
// class is a revoke of ALL, which is how the SQL schema reader spells REVOKE
// ALL. The two have to be one: CockroachDB records a revoke of ALL from the
// owner as ALL, and on CockroachDB the owner's ALL covers privileges the list
// does not name.
//
// On CockroachDB the read cannot say which of its own privileges an owner took
// away when it took away some and not all, and it reports the owner's part of
// that default as undescribed. Two statements converge from there whatever the
// owner holds, and both leave a state the read describes: REVOKE ALL, and
// GRANT ALL, which restores the built-in default. So a declared revoke of ALL
// plans the first, and a declared ALL, or nothing declared for a grantor the
// declaration manages, plans the second. Anything else the declaration says
// about the owner there is withheld as undecided: planned, it would be planned
// again on every run, because the read never shows it done.
//
// # What a read that did not look plans
//
// A read that records [coverage.DefaultPrivilege] as not described reports no
// default privilege, and that silence is not their absence. CockroachDB v26.2.7
// refuses every read of pg_default_acl while a default privilege names a role
// whose name needs quoting, and the reader records the refusal rather than
// failing the whole description (stokaro/ptah#3816). A declared default is then
// withheld as undecided rather than planned: ALTER DEFAULT PRIVILEGES ...
// GRANT does not converge the grant option of a privilege already held, so the
// statement is not safe to repeat blind. A document that records the kind as
// not described keeps its silence from becoming a revoke, as for every other
// kind.
func DefaultPrivilegesWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
	cov Coverage,
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

	revoked := revokedDefaultPrivileges(desired, semantics)

	managedGrantors := make(map[string]bool, len(desired.Roles))
	for _, role := range desired.Roles {
		managedGrantors[semantics.TableIdentityKey(strings.TrimSpace(role.Name))] = true
	}

	current := currentDefaultPrivileges{
		described:      make(map[defaultPrivilegeIdentity]difftypes.DefaultPrivilegeRef, len(database.DefaultPrivileges)),
		revoked:        make(map[defaultPrivilegeIdentity]difftypes.DefaultPrivilegeRef),
		revokedObjects: make(map[defaultPrivilegeObject]bool),
	}
	removable := make(map[defaultPrivilegeIdentity]difftypes.DefaultPrivilegeRef)
	for _, privilege := range database.DefaultPrivileges {
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
			diff.DefaultPrivilegesRemoved = append(diff.DefaultPrivilegesRemoved, ref)
		}
	}
	planBuiltinRevokes(desired, semantics, current, diff)
	for key, ref := range current.revoked {
		owned := managedGrantors[key.object.grantor] || declaredObjects[key.object]
		if !owned || revokes(revoked, key) || declaredDefaultPrivilege(key, declared) {
			continue
		}
		diff.DefaultPrivilegesAdded = append(diff.DefaultPrivilegesAdded, ref)
	}
	cov.recordUndecidedAdditions(resolveUndescribedOwners(database, semantics, declaredDefaults{
		granted: declared, revoked: revoked, managedGrantors: managedGrantors,
	}, diff))

	kept, withheld := keepPlannedAdditions(cov, coverage.DefaultPrivilege, diff.DefaultPrivilegesAdded,
		defaultPrivilegeSpelling, difftypes.DefaultPrivilegeRef.String, unguardedCreations(),
	)
	diff.DefaultPrivilegesAdded = kept
	cov.recordUndecidedAdditions(withheld)
	diff.DefaultPrivilegesRemoved = keepPlannedRemovals(cov, coverage.DefaultPrivilege,
		diff.DefaultPrivilegesRemoved, defaultPrivilegeSpelling,
	)

	// Every list above is built by ranging over a map, whose order Go
	// randomizes. Unsorted, the same two schemas produce a different migration
	// file on the next run.
	sortDefaultPrivilegeRefs(diff.DefaultPrivilegesAdded)
	sortDefaultPrivilegeRefs(diff.DefaultPrivilegesRemoved)
	sortDefaultPrivilegeRefs(diff.DefaultPrivilegeOptionsAdded)
	sortDefaultPrivilegeRefs(diff.DefaultPrivilegeOptionsRevoked)
}

// currentDefaultPrivileges is what the read reported: the privileges held
// beyond the built-in default, and the built-in privileges global rows took
// away.
type currentDefaultPrivileges struct {
	described map[defaultPrivilegeIdentity]difftypes.DefaultPrivilegeRef
	revoked   map[defaultPrivilegeIdentity]difftypes.DefaultPrivilegeRef
	// revokedObjects are the objects revoked holds at least one entry of.
	revokedObjects map[defaultPrivilegeObject]bool
}

// builtinHeld reports whether the database holds key through the built-in
// default: key is global, the built-in default gives it, and the database did
// not take it away, by its own name or by ALL. ALL itself is held only while
// nothing of the object was taken away.
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

// planDefaultPrivilegeAdditions plans each declared default privilege the
// database does not hold, and the grant options the two disagree on.
func planDefaultPrivilegeAdditions(
	declared map[defaultPrivilegeIdentity]difftypes.DefaultPrivilegeRef,
	current currentDefaultPrivileges,
	diff *difftypes.SchemaDiff,
) {
	for key, ref := range declared {
		held, exists := heldDefaultPrivileges(key, current)
		if !exists {
			diff.DefaultPrivilegesAdded = append(diff.DefaultPrivilegesAdded, ref)
			continue
		}
		if ref.WithOption && slices.ContainsFunc(held, func(held difftypes.DefaultPrivilegeRef) bool { return !held.WithOption }) {
			diff.DefaultPrivilegeOptionsAdded = append(diff.DefaultPrivilegeOptionsAdded, ref)
		}
		if ref.WithOption {
			continue
		}
		for _, current := range held {
			if current.WithOption {
				diff.DefaultPrivilegeOptionsRevoked = append(diff.DefaultPrivilegeOptionsRevoked, current)
			}
		}
	}
}

// heldDefaultPrivileges answers whether the database holds the default
// privilege key names, and with which rows. ALL is held when a row says ALL,
// or when the database reports every privilege ALL names on the object class
// on every supported release, one row each; see [heldPrivileges] for why
// MAINTAIN is not required. Without this a declared ALL matched no row, so the
// comparison granted ALL and revoked each row it stood for on every run
// (stokaro/ptah#3579). A privilege the built-in default holds for a global key
// counts as held, as a row without the grant option.
func heldDefaultPrivileges(
	key defaultPrivilegeIdentity,
	current currentDefaultPrivileges,
) ([]difftypes.DefaultPrivilegeRef, bool) {
	if ref, exists := current.held(key); exists {
		return []difftypes.DefaultPrivilegeRef{ref}, true
	}
	if key.privilege != allPrivilege {
		return nil, false
	}
	portable := pgprivilege.Portable(key.object.objectType)
	if len(portable) == 0 {
		return nil, false
	}
	held := make([]difftypes.DefaultPrivilegeRef, 0, len(portable))
	for _, privilege := range portable {
		ref, exists := current.held(defaultPrivilegeIdentity{object: key.object, privilege: privilege})
		if !exists {
			return nil, false
		}
		held = append(held, ref)
	}
	return held, true
}

// held answers one privilege: the row the read reported for it, or, for a
// built-in privilege of a global key the database did not take away, a row
// without the grant option, which is how the built-in default holds it.
func (c currentDefaultPrivileges) held(key defaultPrivilegeIdentity) (difftypes.DefaultPrivilegeRef, bool) {
	if ref, exists := c.described[key]; exists {
		return ref, true
	}
	if !c.builtinHeld(key) {
		return difftypes.DefaultPrivilegeRef{}, false
	}
	return difftypes.DefaultPrivilegeRef{Privilege: key.privilege}, true
}

// planBuiltinRevokes plans the revoke of each built-in privilege a global
// declaration revokes and the database still holds. A revoke of a privilege
// the database holds beyond the built-in default is the removal loop's, so it
// is not planned twice.
func planBuiltinRevokes(
	desired *schemamodel.Database,
	semantics identifier.Semantics,
	current currentDefaultPrivileges,
	diff *difftypes.SchemaDiff,
) {
	for _, declaration := range desired.DefaultPrivileges {
		declaration.Canonicalize()
		if declaration.Schema != "" {
			continue
		}
		for _, name := range revokedNames(declaration) {
			base := difftypes.DefaultPrivilegeRef{
				Grantor: declaration.Grantor, ObjectType: declaration.ObjectType, Grantee: declaration.Grantee,
			}
			diff.DefaultPrivilegesRemoved = append(diff.DefaultPrivilegesRemoved,
				builtinRevokesOf(base, name, semantics, current)...)
		}
	}
}

// builtinRevokesOf is the revoke one revoked name plans for one global
// declaration, base carrying its identity, or nothing when the database holds
// none of it through the built-in default.
//
// A revoked ALL is one statement whenever the database still holds any of the
// built-in default of the grantee, which is also what CockroachDB records for
// the owner. It is decided on the privileges every release has: a release
// without MAINTAIN never reports it taken away, so counting it would plan the
// revoke on every run there, and REVOKE ALL takes MAINTAIN too where it exists.
func builtinRevokesOf(
	base difftypes.DefaultPrivilegeRef,
	name string,
	semantics identifier.Semantics,
	current currentDefaultPrivileges,
) []difftypes.DefaultPrivilegeRef {
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
		return []difftypes.DefaultPrivilegeRef{ref}
	}
	return nil
}

// revokedNames is what a canonical declaration revokes, with a global revoke
// from the owner that names every privilege of its object class read as ALL.
// Another grantee's list stays a list: ALL names the same privileges for it
// on every engine, and the plan keeps the spelling the author wrote.
func revokedNames(declaration schemamodel.DefaultPrivilege) []string {
	if declaration.Schema == "" && declaration.Grantee == declaration.Grantor &&
		pgprivilege.NamesAll(declaration.ObjectType, declaration.Revoked) {
		return []string{allPrivilege}
	}
	return declaration.Revoked
}

// declaredDefaults is what the declaration says, keyed for the comparison.
type declaredDefaults struct {
	granted         map[defaultPrivilegeIdentity]difftypes.DefaultPrivilegeRef
	revoked         map[defaultPrivilegeIdentity]bool
	managedGrantors map[string]bool
}

// resolveUndescribedOwners replans the owner's part of each global default the
// read reported as undescribed, and returns what it cannot decide there as
// undecided objects. See [DefaultPrivilegesWithSemantics] for the rule.
func resolveUndescribedOwners(
	database *catalog.Database,
	semantics identifier.Semantics,
	declaration declaredDefaults,
	diff *difftypes.SchemaDiff,
) []coverage.Object {
	var undecided []coverage.Object
	for _, privilege := range database.UndescribedDefaultPrivileges {
		if privilege.Grantor == "" || privilege.Schema != "" {
			continue
		}
		owner := difftypes.DefaultPrivilegeRef{
			Grantor:    strings.TrimSpace(privilege.Grantor),
			ObjectType: strings.ToUpper(strings.TrimSpace(privilege.ObjectType)),
			Grantee:    strings.TrimSpace(privilege.Grantor),
		}
		object := newDefaultPrivilegeIdentity(owner, semantics).object
		for _, planned := range []*[]difftypes.DefaultPrivilegeRef{
			&diff.DefaultPrivilegesAdded, &diff.DefaultPrivilegesRemoved,
			&diff.DefaultPrivilegeOptionsAdded, &diff.DefaultPrivilegeOptionsRevoked,
		} {
			*planned = slices.DeleteFunc(*planned, func(ref difftypes.DefaultPrivilegeRef) bool {
				return newDefaultPrivilegeIdentity(ref, semantics).object == object
			})
		}
		granted, revoked := declaration.about(object)
		all := owner
		all.Privilege = allPrivilege
		switch {
		case len(granted) == 0 && slices.Equal(revoked, []string{allPrivilege}):
			diff.DefaultPrivilegesRemoved = append(diff.DefaultPrivilegesRemoved, all)
		case len(granted) == 0 && len(revoked) == 0:
			if declaration.managedGrantors[object.grantor] {
				diff.DefaultPrivilegesAdded = append(diff.DefaultPrivilegesAdded, all)
			}
		case len(revoked) == 0 && len(granted) == 1 && granted[0].Privilege == allPrivilege:
			diff.DefaultPrivilegesAdded = append(diff.DefaultPrivilegesAdded, granted[0])
		default:
			undecided = append(undecided, undecidedOwnerPart(owner, granted, revoked)...)
		}
	}
	return undecided
}

// about returns what the declaration grants on object and the privileges it
// revokes there, each sorted.
func (d declaredDefaults) about(object defaultPrivilegeObject) ([]difftypes.DefaultPrivilegeRef, []string) {
	var granted []difftypes.DefaultPrivilegeRef
	for key, ref := range d.granted {
		if key.object == object {
			granted = append(granted, ref)
		}
	}
	var revoked []string
	for key := range d.revoked {
		if key.object == object {
			revoked = append(revoked, key.privilege)
		}
	}
	sortDefaultPrivilegeRefs(granted)
	slices.Sort(revoked)
	return granted, revoked
}

// undecidedOwnerPart names each privilege the declaration grants or revokes
// on an owner's part the read could not describe.
func undecidedOwnerPart(
	owner difftypes.DefaultPrivilegeRef,
	granted []difftypes.DefaultPrivilegeRef,
	revoked []string,
) []coverage.Object {
	refs := slices.Clone(granted)
	for _, name := range revoked {
		ref := owner
		ref.Privilege = name
		refs = append(refs, ref)
	}
	undecided := make([]coverage.Object, 0, len(refs))
	for _, ref := range refs {
		undecided = append(undecided, coverage.Object{
			Kind:       coverage.DefaultPrivilege,
			Name:       ref.String(),
			Reason:     coverage.Unsupported,
			Provenance: coverage.DerivedFromTarget,
		})
	}
	return undecided
}

// declaredDefaultPrivilege answers whether the declaration grants the default
// privilege key names: by its own name, or by an ALL on the same object that
// names it.
func declaredDefaultPrivilege(
	key defaultPrivilegeIdentity,
	declared map[defaultPrivilegeIdentity]difftypes.DefaultPrivilegeRef,
) bool {
	if _, exists := declared[key]; exists {
		return true
	}
	if _, exists := declared[defaultPrivilegeIdentity{object: key.object, privilege: allPrivilege}]; !exists {
		return false
	}
	return slices.Contains(pgprivilege.All(key.object.objectType), key.privilege)
}

// revokedDefaultPrivileges keys every default privilege the declaration
// revokes. A revoked privilege is removed wherever the database holds it,
// whoever the grantor is: it says the privilege is absent, which a
// declaration that only leaves it out does not. A revoked ALL is keyed as ALL
// and matches every privilege of its object.
func revokedDefaultPrivileges(
	desired *schemamodel.Database,
	semantics identifier.Semantics,
) map[defaultPrivilegeIdentity]bool {
	revoked := make(map[defaultPrivilegeIdentity]bool)
	for _, privilege := range desired.DefaultPrivileges {
		privilege.Canonicalize()
		for _, name := range revokedNames(privilege) {
			ref := difftypes.DefaultPrivilegeRef{
				Grantor: privilege.Grantor, Schema: privilege.Schema, ObjectType: privilege.ObjectType,
				Grantee: privilege.Grantee, Privilege: name,
			}
			revoked[newDefaultPrivilegeIdentity(ref, semantics)] = true
		}
	}
	return revoked
}

// revokes reports whether the declaration revokes the privilege key names,
// by its own name or by ALL on the same object.
func revokes(revoked map[defaultPrivilegeIdentity]bool, key defaultPrivilegeIdentity) bool {
	return revoked[key] || revoked[defaultPrivilegeIdentity{object: key.object, privilege: allPrivilege}]
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

// defaultPrivilegeSpelling names one default privilege for a coverage record:
// its schema, and the one spelling a record can name it by, the phrase
// [difftypes.DefaultPrivilegeRef.String] writes.
func defaultPrivilegeSpelling(ref difftypes.DefaultPrivilegeRef) (schema string, spellings []string) {
	return ref.Schema, []string{ref.String()}
}
