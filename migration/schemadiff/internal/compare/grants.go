package compare

import (
	"slices"
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/objectidentity"
	"ptah.run/internal/pgprivilege"
	"ptah.run/internal/routineargs"
	"ptah.run/migration/schemadiff/difftypes"
)

// grantObjectTypeSchema is the one object type whose target is not a table.
const grantObjectTypeSchema = "SCHEMA"

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
	generatedGrantMap := make(map[grantIdentity]difftypes.GrantRef)
	generatedGrantRoles := make(map[string]bool)
	for _, grant := range desired.Grants {
		generatedGrantRoles[grant.Role] = true
		for _, ref := range grantRefsFromGenerated(grant) {
			generatedGrantMap[newGrantIdentity(ref, semantics)] = ref
		}
	}
	// A revoked grant is asserted absent whoever holds it, so it reaches a
	// grantee the removal map below never looks at: PUBLIC, and a role the
	// schema does not manage.
	revokedGrants := revokedGrantIdentities(desired, semantics)

	managedRoles := make(map[string]bool)
	for _, role := range desired.Roles {
		managedRoles[role.Name] = true
	}

	databaseGrantMapForAdditions := make(map[grantIdentity]difftypes.GrantRef)
	databaseGrantMapForRemovals := make(map[grantIdentity]difftypes.GrantRef)
	for _, grant := range database.Grants {
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
		key := newGrantIdentity(ref, semantics)
		if managedRoles[ref.Role] || generatedGrantRoles[ref.Role] {
			databaseGrantMapForAdditions[key] = ref
		}
		// A revoke of a table privilege takes it off every column too, as
		// PostgreSQL does, so it reaches the column rows. A column the schema
		// grants the privilege on is kept by the check below.
		if managedRoles[ref.Role] || revokedGrants[key] || revokedGrants[key.onWholeObject()] {
			databaseGrantMapForRemovals[key] = ref
		}
	}

	planGrantAdditions(generatedGrantMap, databaseGrantMapForAdditions, managedRoles, diff)
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

// planGrantAdditions plans each declared grant the database does not hold,
// and the grant options a declaration and the database disagree on. An option
// the database has and the declaration lacks is taken back only from a role
// the schema manages.
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
func revokedGrantIdentities(desired *schemamodel.Database, semantics identifier.Semantics) map[grantIdentity]bool {
	revoked := make(map[grantIdentity]bool)
	for _, grant := range desired.RevokedGrants {
		for _, ref := range grantRefsFromGenerated(grant) {
			key := newGrantIdentity(ref, semantics)
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
