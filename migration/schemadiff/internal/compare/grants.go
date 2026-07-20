package compare

import (
	"sort"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// Grants compares PostgreSQL role privilege grants.
func Grants(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	generatedGrantMap := make(map[string]difftypes.GrantRef)
	generatedGrantRoles := make(map[string]bool)
	for _, grant := range generated.Grants {
		generatedGrantRoles[grant.Role] = true
		for _, ref := range grantRefsFromGenerated(grant) {
			generatedGrantMap[grantRefIdentityKey(ref)] = ref
		}
	}

	managedRoles := make(map[string]bool)
	for _, role := range generated.Roles {
		managedRoles[role.Name] = true
	}

	databaseGrantMapForAdditions := make(map[string]difftypes.GrantRef)
	databaseGrantMapForRemovals := make(map[string]difftypes.GrantRef)
	for _, grant := range database.Grants {
		ref := grantRefFromDatabase(grant)
		key := grantRefIdentityKey(ref)
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
	if grant.OnSchema != "" {
		objectType = "SCHEMA"
		objectName = grant.OnSchema
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
	if objectType == "SCHEMA" {
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

func grantRefIdentityKey(ref difftypes.GrantRef) string {
	return strings.Join([]string{
		strings.TrimSpace(ref.Role),
		strings.ToUpper(strings.TrimSpace(ref.ObjectType)),
		strings.TrimSpace(ref.ObjectName),
		strings.ToUpper(strings.TrimSpace(ref.Privilege)),
	}, "\x00")
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
