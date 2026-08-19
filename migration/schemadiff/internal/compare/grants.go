package compare

import (
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/objectidentity"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// grantObjectTypeSchema is the one object type whose target is not a table.
const grantObjectTypeSchema = "SCHEMA"

// Grants compares PostgreSQL role privilege grants using the identifier rules
// its dialect name implies.
//
// Callers holding a live connection should use [GrantsWithSemantics] instead:
// on MySQL and MariaDB a schema is a database, so nothing offline can name the
// one that owns an unqualified target.
func Grants(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	GrantsWithSemantics(generated, database, diff, identifier.ForDialect(""))
}

// GrantsWithSemantics is [Grants] told which identifier rules the target has.
//
// The two sides do not spell a target the same way, and until they were
// normalized they could not match. A grant read from the catalog reports the
// object through [types.DBGrant.QualifiedTarget], which qualifies it with the
// schema the reader found -- `"public"."granted"`. A grant declared in Go
// annotations or HCL carries whatever the author wrote, which is normally the
// bare `granted`. Keyed raw, one grant became two: the declared one absent from
// the database and therefore GRANTed, and the database one absent from the
// declaration and therefore REVOKEd, on every run of an unchanged schema.
//
// This is [tableMemberKey]'s defect (stokaro/ptah#1232) in a comparator that
// builds its own key, and one of the instances collected in stokaro/ptah#1276.
func GrantsWithSemantics(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) {
	generatedGrantMap := make(map[grantIdentity]difftypes.GrantRef)
	generatedGrantRoles := make(map[string]bool)
	for _, grant := range generated.Grants {
		generatedGrantRoles[grant.Role] = true
		for _, ref := range grantRefsFromGenerated(grant) {
			generatedGrantMap[newGrantIdentity(ref, semantics)] = ref
		}
	}

	managedRoles := make(map[string]bool)
	for _, role := range generated.Roles {
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
		if managedRoles[ref.Role] {
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

	sortGrantRefs(diff.GrantsAdded)
	sortGrantRefs(diff.GrantsRemoved)
	sortGrantRefs(diff.GrantOptionsAdded)
	sortGrantRefs(diff.GrantOptionsRevoked)
}

func grantRefsFromGenerated(grant goschema.Grant) []difftypes.GrantRef {
	grant.Canonicalize()
	objectType := "TABLE"
	objectName := grant.OnTable
	switch {
	case grant.OnSchema != "":
		objectType = grantObjectTypeSchema
		objectName = grant.OnSchema
	case grant.OnSequence != "":
		objectType = "SEQUENCE"
		objectName = grant.OnSequence
	}
	refs := make([]difftypes.GrantRef, 0, len(grant.Privileges))
	for _, privilege := range grant.Privileges {
		refs = append(refs, difftypes.GrantRef{
			Role:       grant.Role,
			Privilege:  strings.ToUpper(strings.TrimSpace(privilege)),
			ObjectType: objectType,
			ObjectName: objectName,
			WithOption: grant.WithOption,
		})
	}
	return refs
}

func grantRefFromDatabase(grant types.DBGrant) difftypes.GrantRef {
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
		WithOption: grant.WithOption,
	}
}

// grantIdentity is what makes two grants the same grant: a role, a privilege,
// and the object they are about.
//
// The object is a normalized [tableIdentity] rather than the string it was
// written as, for the reason given on [GrantsWithSemantics]. A SCHEMA grant is
// the exception -- its target is a schema, so there is no owning schema to
// resolve -- and it goes in the schema slot with the table slot left empty,
// which also keeps `GRANT ... ON SCHEMA app` from colliding with
// `GRANT ... ON TABLE app`.
type grantIdentity struct {
	role       string
	privilege  string
	objectType string
	object     tableIdentity
}

func newGrantIdentity(ref difftypes.GrantRef, semantics identifier.Semantics) grantIdentity {
	objectType := strings.ToUpper(strings.TrimSpace(ref.ObjectType))
	object := newQualifiedTableIdentity(ref.ObjectName, semantics)
	if objectType == grantObjectTypeSchema {
		// A schema grant names a schema, not a table in one, so the schema
		// component carries the name and the table component stays empty.
		object = objectidentity.NewBuilder(semantics).TableParts(ref.ObjectName, "").Key()
	}
	return grantIdentity{
		role:       strings.TrimSpace(ref.Role),
		privilege:  strings.ToUpper(strings.TrimSpace(ref.Privilege)),
		objectType: objectType,
		object:     object,
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
		if refs[i].Role != refs[j].Role {
			return refs[i].Role < refs[j].Role
		}
		if refs[i].Privilege != refs[j].Privilege {
			return refs[i].Privilege < refs[j].Privilege
		}
		return !refs[i].WithOption && refs[j].WithOption
	})
}
