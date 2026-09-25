package compare

import (
	"slices"
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/objectidentity"
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
		if managedRoles[ref.Role] || revokedGrants[key] {
			databaseGrantMapForRemovals[key] = ref
		}
	}

	for key, ref := range generatedGrantMap {
		databaseRef, exists := databaseGrantMapForAdditions[key]
		if !exists {
			diff.GrantsAdded = append(diff.GrantsAdded, ref)
			continue
		}
		if ref.WithOption && !databaseRef.WithOption {
			diff.GrantOptionsAdded = append(diff.GrantOptionsAdded, ref)
		}
		if !ref.WithOption && databaseRef.WithOption && managedRoles[ref.Role] {
			diff.GrantOptionsRevoked = append(diff.GrantOptionsRevoked, databaseRef)
		}
	}
	for key, ref := range databaseGrantMapForRemovals {
		if _, exists := generatedGrantMap[key]; !exists {
			diff.GrantsRemoved = append(diff.GrantsRemoved, ref)
		}
	}
	revokeOnCreatedTargets(desired, database, databaseGrantMapForRemovals, diff, semantics)

	sortGrantRefs(diff.GrantsAdded)
	sortGrantRefs(diff.GrantsRemoved)
	sortGrantRefs(diff.GrantOptionsAdded)
	sortGrantRefs(diff.GrantOptionsRevoked)
}

// revokedGrantIdentities keys every privilege the desired schema revokes.
func revokedGrantIdentities(desired *schemamodel.Database, semantics identifier.Semantics) map[grantIdentity]bool {
	revoked := make(map[grantIdentity]bool)
	for _, grant := range desired.RevokedGrants {
		for _, ref := range grantRefsFromGenerated(grant) {
			revoked[newGrantIdentity(ref, semantics)] = true
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
func revokeOnCreatedTargets(
	desired *schemamodel.Database,
	database *catalog.Database,
	planned map[grantIdentity]difftypes.GrantRef,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) {
	for _, grant := range desired.RevokedGrants {
		for _, ref := range grantRefsFromGenerated(grant) {
			if _, exists := planned[newGrantIdentity(ref, semantics)]; exists {
				continue
			}
			if revokedTargetCreated(ref, desired, database, semantics) {
				diff.GrantsRemoved = append(diff.GrantsRemoved, ref)
			}
		}
	}
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
	refs := make([]difftypes.GrantRef, 0, len(grant.Privileges))
	for _, privilege := range grant.Privileges {
		refs = append(refs, difftypes.GrantRef{
			Role:       grant.Role,
			Privilege:  strings.ToUpper(strings.TrimSpace(privilege)),
			ObjectType: objectType,
			ObjectName: objectName,
			Arguments:  arguments,
			WithOption: grant.WithOption,
		})
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
