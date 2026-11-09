package compare

import (
	"cmp"
	"slices"
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgprivilege"
	"ptah.run/internal/routineargs"
)

// grantObjectTypeSchema is the object type whose target is a schema rather
// than a table: on YDB, a directory.
const grantObjectTypeSchema = "SCHEMA"

// grantObjectTypeDatabase is the object type whose target is the database
// itself, YDB's database root, which has no name a comparison could tell
// apart: it is the database being compared.
const grantObjectTypeDatabase = "DATABASE"

// Grants compares PostgreSQL role privilege grants using the identifier rules
// its dialect name implies.
//
// Callers holding a live connection should use [GrantsWithSemantics] instead:
// on MySQL and MariaDB a schema is a database, so nothing offline can name the
// one that owns an unqualified target.
func Grants(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff) {
	GrantsWithSemantics(desired, current, diff, identifier.ForDialect(""))
}

// GrantsWithSemantics is [Grants] told which identifier rules the target has.
//
// The two sides do not spell a target the same way, and until they were
// normalized they could not match. A grant read from the catalog reports the
// object through [catalog.Grant.QualifiedTarget], which qualifies it with the
// schema the reader found -- `"public"."granted"`. A grant declared in Go
// annotations or HCL carries whatever the author wrote, which is normally the
// bare `granted`. Keyed raw, one grant became two: the declared one absent from
// the database and therefore GRANTed, and the database one absent from the
// declaration and therefore REVOKEd, on every run of an unchanged schema.
//
// This is [tableMemberKey]'s defect (stokaro/ptah#1232) in a comparator that
// builds its own key, and one of the instances collected in stokaro/ptah#1276.
func GrantsWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) {
	GrantsWithObjects(desired, database, diff, semantics, OwnedObjects{})
}

// GrantObjects names the object a grant is on when an owner manages it, such
// as a YDB topic: the grant's object type is one the owner declares.
// *ptah.run/core/selectext.Set implements it.
type GrantObjects interface {
	GrantObject(objectType, objectName string) (objectidentity.ID, bool)
}

// OwnedObjects is what a grant comparison knows of the objects owners manage:
// how a grant names one, what the desired source says about them, and which
// ones the owners keep. The zero value names none, so every grant is on a
// common object.
type OwnedObjects struct {
	// Names reads the owner's object a grant names. Nil names none.
	Names GrantObjects
	// Desired is the desired source's own account of the owners' objects:
	// the objects it declares and the knowledge it claims about the rest.
	Desired schemaext.FeatureState
	// Effective holds the objects of the desired state the owners made of
	// the source. An object here that Desired does not declare is one an
	// owner keeps as the database holds it, such as a YDB resource pool,
	// which belongs to the whole database and which a source that leaves it
	// out does not remove. The zero value holds none.
	Effective schemaext.Objects
}

// describes reports whether the desired source makes a claim about the
// object grant is on, so that leaving the grant out of it is a removal. A
// grant on a common object is described as the object is. A grant on an
// owner's object is described when the source declares the object, or claims
// to know the object's state: it describes the object's kind there, asserts
// the object absent, or asks for its defaults. A source that does not
// describe the kind, by a `ptah:not-described` record or by claiming nothing
// about it, makes no claim about who may use such an object, and the entry
// stays as the database holds it. Neither does a source that leaves out an
// object its owner keeps whatever the source claims about the kind: the
// object stays as the database holds it, and so does the entry on it.
func (o OwnedObjects) describes(grant catalog.Grant) bool {
	if o.Names == nil {
		return true
	}
	ref, owned := o.Names.GrantObject(grant.ObjectType, grant.QualifiedTarget())
	if !owned {
		return true
	}
	if _, declared, _ := o.Desired.Objects.Get(ref); declared {
		return true
	}
	if _, kept, _ := o.Effective.Get(ref); kept {
		return false
	}
	switch o.Desired.Coverage.Lookup(schemaext.Kind(ref.Kind), ref).State {
	case schemaext.Uninspected, schemaext.Unrepresentable:
		return false
	}
	return true
}

// GrantsWithObjects is [GrantsWithSemantics] told how a grant names an
// owner's object and what the desired source says about those objects. A
// grant on an owner's object the desired source does not describe is not a
// removal (see [OwnedObjects]), as a grant in a schema it does not describe is
// not; a revoke the source writes out is still planned.
func GrantsWithObjects(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
	objects OwnedObjects,
) {
	GrantsWithOptions(desired, database, diff, semantics, GrantOptions{Objects: objects})
}

// GrantOptions are the facts about the two sides a grant comparison reads.
type GrantOptions struct {
	// Objects is what the comparison knows of the objects owners manage.
	Objects OwnedObjects
	// DesiredComplete says the desired state holds every grant on what it
	// describes; see [ptah.run/config.CompareOptions.DesiredGrantsComplete].
	DesiredComplete bool
	// DesiredRead is the grants of the read the desired state was made from;
	// see [ptah.run/config.CompareOptions.DesiredReadGrants].
	DesiredRead []catalog.Grant
}

// GrantsWithOptions is [GrantsWithObjects] told what the two sides are.
func GrantsWithOptions(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
	options GrantOptions,
) {
	objects := options.Objects
	diff.ConnectedRole = database.ConnectedRole
	sequences := newColumnSequences(desired, database, semantics)
	generatedGrantMap := make(map[grantIdentity]difftypes.GrantRef)
	generatedGrantRoles := make(map[string]bool)
	for _, grant := range desired.Grants {
		generatedGrantRoles[grant.Role] = true
		for _, ref := range grantRefsFromGenerated(grant) {
			generatedGrantMap[sequences.desiredKey(newGrantIdentity(ref, semantics))] = ref
		}
	}
	// A revoked grant is asserted absent whoever holds it, so it reaches a
	// grantee the removal map below never looks at: PUBLIC, and a role the
	// schema does not manage.
	revokedGrants := revokedGrantIdentities(desired, sequences, semantics)

	managedRoles := make(map[string]bool)
	for _, role := range desired.Roles {
		managedRoles[role.Name] = true
	}

	removable := func(ref difftypes.GrantRef) bool { return managedRoles[ref.Role] }
	if options.DesiredComplete {
		removable = func(ref difftypes.GrantRef) bool {
			return managedRoles[ref.Role] || !publicExecuteByDefault(ref)
		}
	}

	read := grantRead{
		database: database, sequences: sequences, semantics: semantics,
		managedRoles: managedRoles, generatedRoles: generatedGrantRoles,
		revoked: revokedGrants, removable: removable,
		coverage: removalCoverage(desired, semantics), objects: objects,
	}
	databaseGrantMapForAdditions, databaseGrantMapForRemovals := read.maps()

	planGrantAdditions(generatedGrantMap, databaseGrantMapForAdditions, managedRoles, diff)
	diff.GrantsAdded = append(diff.GrantsAdded, ownerPrivilegesToGrantBack(options.DesiredRead, database, sequences, semantics)...)
	if options.DesiredRead == nil {
		diff.UnmanagedOwnerPrivileges = unmanagedOwnerPrivileges(desired, database, generatedGrantMap, revokedGrants, sequences, semantics)
	}
	sequences.nameAsDatabase(diff.GrantsAdded, semantics)
	sequences.nameAsDatabase(diff.GrantOptionsAdded, semantics)
	for key, ref := range databaseGrantMapForRemovals {
		if !declaredGrant(key, generatedGrantMap) {
			diff.GrantsRemoved = append(diff.GrantsRemoved, ref)
		}
	}
	revokeOnCreatedTargets(desired, database, databaseGrantMapForRemovals, diff, semantics)

	sortGrantRefs(diff.GrantsAdded)
	sortGrantRefs(diff.GrantsRemoved)
	sortGrantRefs(diff.GrantOptionsAdded)
	sortGrantRefs(diff.GrantOptionsRevoked)
}

type grantRead struct {
	database       *catalog.Database
	sequences      columnSequences
	semantics      identifier.Semantics
	managedRoles   map[string]bool
	generatedRoles map[string]bool
	revoked        map[grantIdentity]bool
	removable      func(difftypes.GrantRef) bool
	coverage       Coverage
	objects        OwnedObjects
}

func (read grantRead) maps() (additions, removals map[grantIdentity]difftypes.GrantRef) {
	additions = make(map[grantIdentity]difftypes.GrantRef)
	removals = make(map[grantIdentity]difftypes.GrantRef)
	for _, grant := range read.database.Grants {
		if grant.Privilege == "" {
			// An implicit routine-owner row with no privilege records that
			// its ACL was inspected after the owner revoked EXECUTE.
			continue
		}
		if grant.IsPartialRevoke {
			// Not a grant. The row SUBTRACTS a privilege from a broader one --
			// ClickHouse's partial revoke, SQL Server's DENY -- and entering it
			// in either map states the opposite of what it says. In the removal
			// map it becomes a REVOKE of a privilege the role already does not
			// hold; in the addition map it makes a declaration asking for that
			// privilege look satisfied. [dbschematogo.convertGrants] skips it
			// for the same reason and spells the reasoning out.
			//
			// The row is left in place on the server. Ptah's grant model has no
			// shape for "this privilege except there", so a declaration that
			// asks for a denied privilege still plans the GRANT, and on a
			// target where the exception wins that plan does not change what
			// the role can do (stokaro/ptah#1698).
			continue
		}
		ref := grantRefFromDatabase(grant)
		key := read.sequences.databaseKey(newGrantIdentity(ref, read.semantics))
		if read.managedRoles[ref.Role] || read.generatedRoles[ref.Role] {
			previous, exists := additions[key]
			if !exists || (ref.WithOption && !previous.WithOption) ||
				(ref.WithOption == previous.WithOption && preferForeignGrantor(previous, ref, read.database.ConnectedRole)) {
				additions[key] = ref
			}
		}
		// A revoke of a table privilege takes it off every column too, as
		// PostgreSQL does, so it reaches the column rows. A column the schema
		// grants the privilege on is kept by the check below.
		//
		// An implicit row is a privilege the object holds by default, which a
		// declaration that leaves it out does not ask to take away; only a
		// revoke does. A CockroachDB routine's owner is one: it keeps EXECUTE
		// through REVOKE ALL, so revoking it from a managed owner the
		// declaration did not name would be planned again on every run.
		//
		// A grant on an object in a schema the desired state does not
		// describe, or on that schema, is left alone for the same reason the
		// object is, and so is a grant on an owner's object the desired
		// source does not describe. A revoke the desired state writes out is
		// still planned.
		if (read.removable(ref) && !grant.Implicit && grantRemovalPlanned(read.coverage, grant) && read.objects.describes(grant)) ||
			read.revoked[key] || read.revoked[key.onWholeObject()] {
			previous, exists := removals[key]
			if !exists || preferForeignGrantor(previous, ref, read.database.ConnectedRole) {
				removals[key] = ref
			}
		}
	}
	return additions, removals
}

// publicExecuteByDefault reports whether the grant ref is PUBLIC's EXECUTE on a
// routine, which the other side of a comparison of two server reads may hold
// without a row saying so, so that its absence there is no difference.
//
// PostgreSQL stores no ACL until a routine's first GRANT or REVOKE, and the first
// one writes out the defaults: PUBLIC's EXECUTE reads as a plain row on one side
// and as an implicit one on the other. Revoking it to match would take away what
// the other side holds. A read of a routine whose PUBLIC EXECUTE was revoked
// declares the revoke, which still reaches this grant.
func publicExecuteByDefault(ref difftypes.GrantRef) bool {
	return ref.Role == "PUBLIC" && grantRoutineKinds[ref.ObjectType]
}

// ownerPrivilegesToGrantBack plans a privilege the database's owner of an object
// revoked from itself and the same role still holds on the same object on the
// other side, which desiredRead reports when that side is a server read.
//
// The privilege has to be one the other side's owner holds, rather than one it
// does not revoke. Ptah describes no ownership, so another role owning the object
// there says nothing about this role, and PostgreSQL 16 has no MAINTAIN for a
// PostgreSQL 17 owner to get back.
func ownerPrivilegesToGrantBack(
	desiredRead []catalog.Grant,
	database *catalog.Database,
	sequences columnSequences,
	semantics identifier.Semantics,
) []difftypes.GrantRef {
	held := make(map[grantIdentity]bool, len(desiredRead))
	for _, grant := range desiredRead {
		if grant.OwnerDefault() {
			held[sequences.databaseKey(newGrantIdentity(grantRefFromDatabase(grant), semantics))] = true
		}
	}
	var refs []difftypes.GrantRef
	for _, grant := range database.Grants {
		if !grant.OwnerRevoked() {
			continue
		}
		ref := grantRefFromDatabase(grant)
		if held[sequences.databaseKey(newGrantIdentity(ref, semantics))] {
			refs = append(refs, ref)
		}
	}
	return refs
}

// unmanagedOwnerPrivileges reports, against a desired state that is not a
// server read, each declared table, view, sequence or routine whose owner revoked from
// itself a privilege the object type gives its owner by default, where the
// desired state neither grants that privilege to the owner nor revokes it.
// The desired state does not manage such a privilege, so nothing is planned
// for it: an absent declaration leaves a privilege alone rather than granting
// it, and restoring what PostgreSQL gives a new object's owner would overwrite
// a revoke somebody made on purpose. The report says what the owner lacks, so
// an owner refused its own table does not read as converged without a word
// (stokaro/ptah#5765). A privilege the desired state does name is a change
// the plan carries and is not reported here.
func unmanagedOwnerPrivileges(
	desired *schemamodel.Database,
	database *catalog.Database,
	declared map[grantIdentity]difftypes.GrantRef,
	revoked map[grantIdentity]bool,
	sequences columnSequences,
	semantics identifier.Semantics,
) []difftypes.OwnerPrivilegeDeviation {
	type object struct{ objectType, name, owner string }
	var order []object
	missing := make(map[object][]string)
	for _, grant := range database.Grants {
		if !grant.OwnerRevoked() {
			continue
		}
		ref := grantRefFromDatabase(grant)
		key := sequences.databaseKey(newGrantIdentity(ref, semantics))
		if declaredGrant(key, declared) || revoked[key] || !declaresGrantObject(desired, ref, semantics) {
			continue
		}
		at := object{objectType: strings.ToUpper(ref.ObjectType), name: ref.ObjectName, owner: ref.Role}
		if _, seen := missing[at]; !seen {
			order = append(order, at)
		}
		missing[at] = append(missing[at], strings.ToUpper(ref.Privilege))
	}
	deviations := make([]difftypes.OwnerPrivilegeDeviation, 0, len(order))
	for _, at := range order {
		privileges := missing[at]
		slices.Sort(privileges)
		deviations = append(deviations, difftypes.OwnerPrivilegeDeviation{ObjectType: at.objectType, Object: at.name, Owner: at.owner,
			Missing: slices.Compact(privileges)})
	}
	slices.SortFunc(deviations, func(a, b difftypes.OwnerPrivilegeDeviation) int {
		return cmp.Or(cmp.Compare(a.Object, b.Object), cmp.Compare(a.ObjectType, b.ObjectType), cmp.Compare(a.Owner, b.Owner))
	})
	return deviations
}

// declaresGrantObject reports whether desired declares the table, view,
// sequence or routine ref is on. PostgreSQL names a view's privileges as a table's.
func declaresGrantObject(desired *schemamodel.Database, ref difftypes.GrantRef, semantics identifier.Semantics) bool {
	target := newQualifiedTableIdentity(ref.ObjectName, semantics)
	named := func(name string) bool { return newQualifiedTableIdentity(name, semantics) == target }
	switch strings.ToUpper(strings.TrimSpace(ref.ObjectType)) {
	case "TABLE":
		return slices.ContainsFunc(desired.Tables, func(table schemamodel.Table) bool { return named(table.QualifiedName()) }) ||
			slices.ContainsFunc(desired.Views, func(view schemamodel.View) bool { return named(view.Name) })
	case "SEQUENCE":
		return slices.ContainsFunc(desired.Sequences, func(sequence schemamodel.Sequence) bool { return named(sequence.QualifiedName()) })
	case "FUNCTION", "PROCEDURE", "ROUTINE":
		arguments := routineargs.InputTypes(ref.Arguments)
		return slices.ContainsFunc(desired.Functions, func(function schemamodel.Function) bool {
			return named(function.Name) && routineargs.InputTypes(function.Parameters) == arguments &&
				(strings.EqualFold(ref.ObjectType, "ROUTINE") || function.IsProcedure() == strings.EqualFold(ref.ObjectType, "PROCEDURE"))
		})
	default:
		return false
	}
}

// planGrantAdditions plans each declared grant the database does not hold,
// and the grant options a declaration and the database disagree on. An option
// the database has and the declaration lacks is taken back only from a role
// the schema manages. A declared grant the database holds is a pair, whether
// or not its option differs.
func planGrantAdditions(
	declared, database map[grantIdentity]difftypes.GrantRef,
	managedRoles map[string]bool,
	diff *difftypes.SchemaDiff,
) {
	for key, ref := range declared {
		held, exists := heldPrivileges(key, database)
		if !exists {
			diff.GrantsAdded = append(diff.GrantsAdded, ref)
			continue
		}
		diff.Matched.Grants++
		if ref.WithOption && slices.ContainsFunc(held, func(held difftypes.GrantRef) bool { return !held.WithOption }) {
			diff.GrantOptionsAdded = append(diff.GrantOptionsAdded, ref)
		}
		if ref.WithOption || !managedRoles[ref.Role] {
			continue
		}
		for _, databaseRef := range held {
			if databaseRef.WithOption {
				diff.GrantOptionsRevoked = append(diff.GrantOptionsRevoked, databaseRef)
			}
		}
	}
}

// allPrivilege is the keyword that names every privilege of an object kind.
const allPrivilege = "ALL"

// withPrivilege is key naming another privilege on the same object.
func (key grantIdentity) withPrivilege(privilege string) grantIdentity {
	key.privilege = privilege
	return key
}

// heldPrivileges answers whether the database holds the privilege key names,
// and with which rows.
//
// A privilege other than ALL is held when its own row is there. ALL is held
// when a row says ALL -- a file compared with a file carries the keyword as it
// was written -- or when the database reports every privilege ALL names on the
// object kind, one row each, which is how a catalog read reports GRANT ALL.
// Without this a declared ALL matched no row, so it was planned again on every
// run (stokaro/ptah#3579).
//
// For a table the rows asked for are [pgprivilege.Portable]: MAINTAIN, which
// PostgreSQL 17 added, is not required. The catalog read carries no server
// version, so a comparison cannot tell a PostgreSQL 16 server, which has no
// MAINTAIN to report, from a PostgreSQL 17 one where MAINTAIN was revoked on
// its own. Requiring it would plan GRANT ALL on every run against every
// PostgreSQL 16 server; not requiring it misses that one revoke.
func heldPrivileges(key grantIdentity, database map[grantIdentity]difftypes.GrantRef) ([]difftypes.GrantRef, bool) {
	if ref, exists := database[key]; exists {
		return []difftypes.GrantRef{ref}, true
	}
	if key.privilege != allPrivilege {
		return nil, false
	}
	portable := pgprivilege.Portable(key.objectType)
	if len(portable) == 0 {
		return nil, false
	}
	held := make([]difftypes.GrantRef, 0, len(portable))
	for _, privilege := range portable {
		ref, exists := database[key.withPrivilege(privilege)]
		if !exists {
			return nil, false
		}
		held = append(held, ref)
	}
	return held, true
}

// declaredGrant answers whether the desired schema grants the privilege key
// names: by its own name, or by an ALL on the same object that names it.
func declaredGrant(key grantIdentity, declared map[grantIdentity]difftypes.GrantRef) bool {
	if _, exists := declared[key]; exists {
		return true
	}
	if _, exists := declared[key.withPrivilege(allPrivilege)]; !exists {
		return false
	}
	return slices.Contains(pgprivilege.All(key.objectType), key.privilege)
}

// revokedGrantIdentities keys every privilege the desired schema revokes. A
// revoked ALL is keyed as itself and as each privilege it names, so it matches
// a catalog read's per-privilege rows as well as a file's ALL.
func revokedGrantIdentities(
	desired *schemamodel.Database,
	sequences columnSequences,
	semantics identifier.Semantics,
) map[grantIdentity]bool {
	revoked := make(map[grantIdentity]bool)
	for _, grant := range desired.RevokedGrants {
		for _, ref := range grantRefsFromGenerated(grant) {
			key := sequences.desiredKey(newGrantIdentity(ref, semantics))
			revoked[key] = true
			if key.privilege != allPrivilege {
				continue
			}
			for _, privilege := range pgprivilege.All(key.objectType) {
				revoked[key.withPrivilege(privilege)] = true
			}
		}
	}
	return revoked
}

// revokeOnCreatedTargets plans a revoked grant on an object this plan creates,
// although the database holds nothing to match it yet: PostgreSQL gives PUBLIC
// EXECUTE on a function the moment CREATE FUNCTION runs, and ALTER DEFAULT
// PRIVILEGES hands a new table's privileges to a role. Without the REVOKE in
// the same plan the created object holds what the schema revokes, and the next
// comparison plans it again. A REVOKE of a privilege not held changes nothing,
// measured on PostgreSQL 18, so the statement is safe whether or not the
// privilege arrives. planned holds the removals already decided, which this
// does not repeat.
//
// A grantee whose revoked privileges on one created object cover everything
// ALL names there gets one REVOKE ALL instead of one statement each. Spelled
// out, the list would name MAINTAIN, and PostgreSQL 16 refuses the word
// (`unrecognized privilege type "maintain"`), so a schema file revoking ALL
// could not be applied there; ALL means what the server has.
func revokeOnCreatedTargets(
	desired *schemamodel.Database,
	database *catalog.Database,
	planned map[grantIdentity]difftypes.GrantRef,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) {
	// The column is part of the group: a column privilege and the table
	// privilege of the same name are two privileges, so a column revoke never
	// counts towards a REVOKE ALL on the table.
	type target struct{ role, objectType, object, arguments, column string }
	var order []target
	revoked := make(map[target][]difftypes.GrantRef)
	for _, grant := range desired.RevokedGrants {
		for _, ref := range grantRefsFromGenerated(grant) {
			if _, exists := planned[newGrantIdentity(ref, semantics)]; exists {
				continue
			}
			if !revokedTargetCreated(ref, desired, database, semantics) {
				continue
			}
			key := target{ref.Role, ref.ObjectType, ref.ObjectName, ref.Arguments, ref.Column}
			if _, seen := revoked[key]; !seen {
				order = append(order, key)
			}
			revoked[key] = append(revoked[key], ref)
		}
	}
	for _, key := range order {
		refs := revoked[key]
		if coversAll(refs) {
			all := refs[0]
			all.Privilege = allPrivilege
			diff.GrantsRemoved = append(diff.GrantsRemoved, all)
			continue
		}
		diff.GrantsRemoved = append(diff.GrantsRemoved, refs...)
	}
}

// coversAll reports whether refs, all about one grantee and one object, have
// to be spelled ALL: they name it, or they name every privilege it names on a
// kind whose list depends on the server version. Where the list is the same on
// every release the privileges are written out as declared.
func coversAll(refs []difftypes.GrantRef) bool {
	named := make(map[string]bool, len(refs))
	for _, ref := range refs {
		named[ref.Privilege] = true
	}
	if named[allPrivilege] {
		return true
	}
	all := pgprivilege.All(refs[0].ObjectType)
	if len(all) == len(pgprivilege.Portable(refs[0].ObjectType)) {
		return false
	}
	return !slices.ContainsFunc(all, func(privilege string) bool { return !named[privilege] })
}

func grantRefsFromGenerated(grant schemamodel.Grant) []difftypes.GrantRef {
	grant.Canonicalize()
	objectType := "TABLE"
	objectName := grant.OnTable
	arguments := ""
	switch {
	case grant.OnDatabase:
		objectType = grantObjectTypeDatabase
		objectName = ""
	case grant.OnObject != "":
		objectType = grant.ObjectKind
		objectName = grant.OnObject
	case grant.OnSchema != "":
		objectType = grantObjectTypeSchema
		objectName = grant.OnSchema
	case grant.OnSequence != "":
		objectType = "SEQUENCE"
		objectName = grant.OnSequence
	case grant.OnRoutine != "":
		objectType = grant.RoutineKind
		objectName = grant.OnRoutine
		arguments = grant.RoutineArguments
	}
	columns := grant.Columns
	if len(columns) == 0 {
		columns = []string{""}
	}
	refs := make([]difftypes.GrantRef, 0, len(grant.Privileges)*len(columns))
	for _, privilege := range grant.Privileges {
		for _, column := range columns {
			refs = append(refs, difftypes.GrantRef{
				Role:       grant.Role,
				Privilege:  strings.ToUpper(strings.TrimSpace(privilege)),
				ObjectType: objectType,
				ObjectName: objectName,
				Arguments:  arguments,
				Column:     column,
				WithOption: grant.WithOption,
			})
		}
	}
	return refs
}

// CurrentGrants returns every grant the read of the database reported, as the
// references a plan names a grant with, sorted. A partial revoke is left out: it
// subtracts a privilege rather than holding one. So is what an object's owner
// holds by default, which the object has again when a plan creates it again.
func CurrentGrants(database *catalog.Database) []difftypes.GrantRef {
	if database == nil || len(database.Grants) == 0 {
		return nil
	}
	refs := make([]difftypes.GrantRef, 0, len(database.Grants))
	for _, grant := range database.Grants {
		if !grant.IsPartialRevoke && !grant.OwnerDefault() {
			refs = append(refs, grantRefFromDatabase(grant))
		}
	}
	sortGrantRefs(refs)
	return refs
}

func grantRefFromDatabase(grant catalog.Grant) difftypes.GrantRef {
	objectType := strings.ToUpper(strings.TrimSpace(grant.ObjectType))
	objectName := grant.QualifiedTarget()
	if objectType == grantObjectTypeSchema {
		objectName = grant.ObjectName
	}
	return difftypes.GrantRef{
		Role:       strings.TrimSpace(grant.Role),
		Privilege:  strings.ToUpper(strings.TrimSpace(grant.Privilege)),
		ObjectType: objectType,
		ObjectName: objectName,
		Arguments:  grant.Arguments,
		Column:     strings.TrimSpace(grant.Column),
		WithOption: grant.WithOption,
		GrantedBy:  strings.TrimSpace(grant.GrantedBy),
	}
}

// preferForeignGrantor keeps a row the connected role cannot reliably revoke
// when several grantors gave the same privilege to one grantee. The planner
// must see that row and refuse before writing a REVOKE that leaves it in place.
func preferForeignGrantor(previous, next difftypes.GrantRef, connectedRole string) bool {
	foreign := func(ref difftypes.GrantRef) bool {
		return ref.GrantedBy != "" && ref.GrantedBy != connectedRole
	}
	return foreign(next) && !foreign(previous)
}

// grantRemovalPlanned reports whether a grant the desired state leaves out is
// a removal: false when the object it is on lives in a schema the desired
// state does not describe, or is that schema. A grant on the database itself
// lives in no schema.
func grantRemovalPlanned(cov Coverage, grant catalog.Grant) bool {
	switch strings.ToUpper(strings.TrimSpace(grant.ObjectType)) {
	case grantObjectTypeDatabase:
		return true
	case grantObjectTypeSchema:
		return cov.PlansRemovalIn(grant.ObjectName)
	default:
		return cov.PlansRemovalIn(grant.Schema)
	}
}

// grantRoutineKinds are the object types that name a routine. They are one
// kind in a grant's identity: FUNCTION, PROCEDURE and ROUTINE are three
// spellings of the target, and PostgreSQL resolves ROUTINE to either.
var grantRoutineKinds = map[string]bool{"FUNCTION": true, "PROCEDURE": true, "ROUTINE": true}

// grantIdentity is what makes two grants the same grant: a role, a privilege,
// and the object they are about.
//
// The object is a normalized [tableIdentity] rather than the string it was
// written as, for the reason given on [GrantsWithSemantics]. A SCHEMA grant is
// the exception -- its target is a schema, so there is no owning schema to
// resolve -- and it goes in the schema slot with the table slot left empty,
// which also keeps `GRANT ... ON SCHEMA app` from colliding with
// `GRANT ... ON TABLE app`.
//
// A routine is named by its schema, its name and its argument types, and the
// types go in a field of their own: PostgreSQL overloads a name by them.
type grantIdentity struct {
	role       string
	privilege  string
	objectType string
	object     tableIdentity
	arguments  string
	// column is the column a table privilege is limited to, empty for the
	// whole object. A column privilege and the table privilege of the same
	// name are two rows in the catalog, so they are two identities.
	column string
}

// onWholeObject is key without its column: the table privilege a revoke of
// which also takes the column privilege.
func (key grantIdentity) onWholeObject() grantIdentity {
	key.column = ""
	return key
}

func newGrantIdentity(ref difftypes.GrantRef, semantics identifier.Semantics) grantIdentity {
	objectType := strings.ToUpper(strings.TrimSpace(ref.ObjectType))
	object := newQualifiedTableIdentity(ref.ObjectName, semantics)
	arguments := ""
	switch {
	case objectType == grantObjectTypeSchema:
		// A schema grant names a schema, not a table in one, so the schema
		// component carries the name and the table component stays empty.
		object = objectidentity.NewBuilder(semantics).TableParts(ref.ObjectName, "").Key()
	case grantRoutineKinds[objectType]:
		objectType = "ROUTINE"
		arguments = routineargs.InputTypes(ref.Arguments)
	}
	return grantIdentity{
		role:       strings.TrimSpace(ref.Role),
		privilege:  strings.ToUpper(strings.TrimSpace(ref.Privilege)),
		objectType: objectType,
		object:     object,
		arguments:  arguments,
		column:     strings.TrimSpace(ref.Column),
	}
}

func sortGrantRefs(refs []difftypes.GrantRef) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].ObjectType != refs[j].ObjectType {
			return refs[i].ObjectType < refs[j].ObjectType
		}
		if refs[i].ObjectName != refs[j].ObjectName {
			return refs[i].ObjectName < refs[j].ObjectName
		}
		if refs[i].Arguments != refs[j].Arguments {
			return refs[i].Arguments < refs[j].Arguments
		}
		if refs[i].Column != refs[j].Column {
			return refs[i].Column < refs[j].Column
		}
		if refs[i].Role != refs[j].Role {
			return refs[i].Role < refs[j].Role
		}
		if refs[i].Privilege != refs[j].Privilege {
			return refs[i].Privilege < refs[j].Privilege
		}
		return !refs[i].WithOption && refs[j].WithOption
	})
}

// revokedTargetCreated reports whether a revoked grant names a table or a
// routine the desired schema declares and the database does not have, which is
// an object the plan creates.
//
// Tables and routines are the targets that can hold a privilege the moment
// they exist; a schema or a sequence created by the plan holds none a
// declaration could revoke.
func revokedTargetCreated(
	ref difftypes.GrantRef,
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) bool {
	target := newQualifiedTableIdentity(ref.ObjectName, semantics)
	objectType := strings.ToUpper(strings.TrimSpace(ref.ObjectType))
	switch {
	case grantRoutineKinds[objectType]:
		arguments := routineargs.InputTypes(ref.Arguments)
		declared := slices.ContainsFunc(desired.Functions, func(function schemamodel.Function) bool {
			return newQualifiedTableIdentity(function.Name, semantics) == target &&
				routineargs.InputTypes(function.Parameters) == arguments
		})
		present := slices.ContainsFunc(database.Functions, func(function catalog.Function) bool {
			return newQualifiedTableIdentity(function.QualifiedName(), semantics) == target &&
				routineargs.InputTypes(function.Signature()) == arguments
		})
		return declared && !present
	case objectType == "TABLE":
		declared := slices.ContainsFunc(desired.Tables, func(table schemamodel.Table) bool {
			return newQualifiedTableIdentity(table.QualifiedName(), semantics) == target
		})
		present := slices.ContainsFunc(database.Tables, func(table catalog.Table) bool {
			return newQualifiedTableIdentity(table.QualifiedName(), semantics) == target
		})
		return declared && !present
	default:
		return false
	}
}
