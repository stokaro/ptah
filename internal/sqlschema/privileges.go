package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgprivilege"
	"ptah.run/internal/privilegefold"
)

// expandGrantedAll replaces ALL in a GRANT with the privileges it names on a
// routine, which is EXECUTE on every server. A table's ALL depends on the
// server version, so a GRANT ALL on any other kind keeps the spelling.
func expandGrantedAll(privileges []string, objectType string) []string {
	if !routineObjectTypes[strings.ToUpper(objectType)] {
		return privileges
	}
	return expandAll(privileges, objectType)
}

// expandAll replaces ALL with the privileges it names on objectType. A REVOKE
// expands every kind, because asserting the absence of a privilege the server
// does not have is harmless.
func expandAll(privileges []string, objectType string) []string {
	expanded := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		if strings.EqualFold(privilege, "ALL") {
			expanded = append(expanded, pgprivilege.All(objectType)...)
			continue
		}
		expanded = append(expanded, privilege)
	}
	return expanded
}

// routineObjectTypes are the target kinds that name a function or procedure.
var routineObjectTypes = map[string]bool{"FUNCTION": true, "PROCEDURE": true, "ROUTINE": true}

// normalizeGrantee spells PUBLIC the way the catalog reports it. PUBLIC is a
// keyword for every role rather than a role. An unquoted spelling in any case
// means the keyword, and so does the quoted lower-case "public": measured on
// PostgreSQL 18, CREATE ROLE "public" is refused as reserved and GRANT ... TO
// "public" writes the PUBLIC entry of the ACL.
//
// On the PostgreSQL family that is every spelling [roleName] folds to
// `public`, which also covers CockroachDB 26.3.2 reading GRANT ... TO "PUBLIC"
// as the keyword. Any other name there is the role [roleName] resolves.
func normalizeGrantee(sourcePlatform, role string) string {
	if platform.IsPostgresFamily(sourcePlatform) {
		name := roleName(sourcePlatform, role)
		if name == "public" {
			return "PUBLIC"
		}
		return name
	}
	normalized := normalizeSQLIdentifier(role)
	quoted := strings.HasPrefix(strings.TrimSpace(role), `"`)
	if (!quoted && strings.EqualFold(normalized, "PUBLIC")) || (quoted && normalized == "public") {
		return "PUBLIC"
	}
	return normalized
}

// roleTarget reads a role in a position that also takes the PUBLIC keyword: a
// policy's TO list and the grantee of a default privilege. On the PostgreSQL
// family it is [normalizeGrantee], so the keyword keeps the catalog's spelling
// while a role name is folded; elsewhere the name is kept as written.
func roleTarget(sourcePlatform, value string) string {
	if platform.IsPostgresFamily(sourcePlatform) {
		return normalizeGrantee(sourcePlatform, value)
	}
	return normalizeSQLIdentifier(strings.TrimSpace(value))
}

// setGrantTarget sets the target of a grant from a statement's object type and
// name, which GRANT and REVOKE share.
func setGrantTarget(grant *schemamodel.Grant, objectType, objectName, arguments string) {
	switch kind := strings.ToUpper(objectType); {
	case kind == "SCHEMA":
		grant.OnSchema = normalizeSQLIdentifier(objectName)
	case kind == "SEQUENCE":
		grant.OnSequence = normalizeSQLTableReference(objectName)
	case routineObjectTypes[kind]:
		grant.OnRoutine = normalizeSQLTableReference(objectName)
		grant.RoutineArguments = strings.TrimSpace(arguments)
		grant.RoutineKind = kind
	default:
		grant.OnTable = normalizeSQLTableReference(objectName)
	}
}

func toGrant(node *ast.GrantPrivilegeNode, sourcePlatform string) schemamodel.Grant {
	grant := schemamodel.Grant{
		Role:       normalizeGrantee(sourcePlatform, node.Role),
		Privileges: expandGrantedAll(node.Privileges, node.ObjectType),
		WithOption: node.WithOption,
		Comment:    node.Comment,
	}
	setGrantTarget(&grant, node.ObjectType, node.ObjectName, node.Arguments)
	grant.Canonicalize()
	return grant
}

// appendGrant records a GRANT, taking the privileges it names out of the
// revoked set first: a GRANT after a REVOKE of the same privilege leaves it
// held.
func appendGrant(database *schemamodel.Database, node *ast.GrantPrivilegeNode, sourcePlatform string) {
	privilegefold.Merge(database, &schemamodel.Database{Grants: []schemamodel.Grant{toGrant(node, sourcePlatform)}})
}

// appendRevoke records a REVOKE.
//
// Each privilege it names leaves every earlier grant of it to the same grantee
// on the same object and joins the revoked set. REVOKE GRANT OPTION FOR keeps
// the privilege and takes only the right to pass it on, which the model can say
// only about a grant this file made: it has no spelling for "held, but without
// the option" on its own, so that form is refused when no earlier GRANT here
// carries the privilege.
func appendRevoke(database *schemamodel.Database, node *ast.RevokePrivilegeNode, sourcePlatform string) error {
	revoked := schemamodel.Grant{Role: normalizeGrantee(sourcePlatform, node.Role), Comment: node.Comment}
	setGrantTarget(&revoked, node.ObjectType, node.ObjectName, node.Arguments)
	revoked.Canonicalize()
	target := revoked.TargetKey()
	privileges := expandAll(node.Privileges, node.ObjectType)
	if privilegefold.HeldAsAll(database.Grants, revoked.Role, target) {
		return fmt.Errorf(
			"REVOKE ON %s FROM %s follows a GRANT ALL on it, which this schema keeps as ALL rather than as the "+
				"privileges ALL names on a given server, so it cannot take one privilege out of it; "+
				"name the privileges in the GRANT", target, revoked.Role)
	}
	if node.GrantOptionFor {
		return revokeGrantOption(database, revoked.Role, target, privileges)
	}
	revoked.Privileges = make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		revoked.Privileges = append(revoked.Privileges, strings.ToUpper(strings.TrimSpace(privilege)))
	}
	privilegefold.Merge(database, &schemamodel.Database{RevokedGrants: []schemamodel.Grant{revoked}})
	return nil
}

// revokeGrantOption clears WITH GRANT OPTION from privileges an earlier GRANT
// in this file made, splitting a grant whose other privileges keep it.
func revokeGrantOption(database *schemamodel.Database, role, target string, privileges []string) error {
	for _, privilege := range privileges {
		privilege = strings.ToUpper(strings.TrimSpace(privilege))
		index := slices.IndexFunc(database.Grants, func(grant schemamodel.Grant) bool {
			return grant.Role == role && grant.TargetKey() == target && slices.Contains(grant.Privileges, privilege)
		})
		if index < 0 {
			return fmt.Errorf(
				"REVOKE GRANT OPTION FOR %s ON %s FROM %s names a grant this schema does not make: "+
					"the schema can say a privilege is held or not held, not that it is held without the option; "+
					"declare the GRANT without WITH GRANT OPTION instead", privilege, target, role)
		}
		grant := database.Grants[index]
		if !grant.WithOption {
			continue
		}
		plain := grant
		plain.Privileges = []string{privilege}
		plain.WithOption = false
		database.Grants = privilegefold.Without(database.Grants, role, target, privilege)
		database.Grants = append(database.Grants, plain)
	}
	return nil
}

// appendDefaultPrivilegeRevoke records an ALTER DEFAULT PRIVILEGES ... REVOKE.
//
// Like a REVOKE on an object, it composes with the statements before it: each
// privilege it names leaves an earlier ALTER DEFAULT PRIVILEGES ... GRANT of
// the same identity and joins that identity's revoked set, and a revoke with
// no grant before it is kept as a statement that the privilege is absent. A
// schema-scoped default privilege is only ever added to the global ones, so on
// the server the revoke takes back what a schema-scoped grant gave, which is
// what a comparison plans it for. ALL names every privilege of the object
// class.
//
// REVOKE GRANT OPTION FOR keeps the privilege and takes the right to pass it
// on, which the model can say only about a grant this file made; with no such
// grant it is refused, as it is for an object privilege.
//
// The grantor and the grantee are read with the functions [toDefaultPrivilege]
// uses, so a revoke names the same identity as the grant it takes back whatever
// case the file writes the role in.
func appendDefaultPrivilegeRevoke(
	database *schemamodel.Database, node *ast.RevokeDefaultPrivilegeNode, sourcePlatform string,
) error {
	revoked := schemamodel.DefaultPrivilege{
		Grantor:    roleName(sourcePlatform, node.Grantor),
		Schema:     normalizeSQLIdentifier(node.Schema),
		ObjectType: node.ObjectType,
		Grantee:    roleTarget(sourcePlatform, node.Grantee),
		Revoked:    expandAll(node.Privileges, node.ObjectType),
		Comment:    node.Comment,
	}
	revoked.Canonicalize()
	if !node.GrantOptionFor {
		database.DefaultPrivileges = privilegefold.MergeDefaultPrivileges(
			database.DefaultPrivileges, []schemamodel.DefaultPrivilege{revoked},
		)
		return nil
	}
	return revokeDefaultPrivilegeOption(database, revoked)
}

// revokeDefaultPrivilegeOption clears the grant option of privileges an
// earlier ALTER DEFAULT PRIVILEGES ... GRANT of the same identity carries.
func revokeDefaultPrivilegeOption(database *schemamodel.Database, revoked schemamodel.DefaultPrivilege) error {
	index := slices.IndexFunc(database.DefaultPrivileges, func(existing schemamodel.DefaultPrivilege) bool {
		existing.Canonicalize()
		return existing.Grantor == revoked.Grantor && existing.Schema == revoked.Schema &&
			existing.ObjectType == revoked.ObjectType && existing.Grantee == revoked.Grantee
	})
	for _, privilege := range revoked.Revoked {
		held := -1
		if index >= 0 {
			held = slices.IndexFunc(database.DefaultPrivileges[index].Privileges, func(grant schemamodel.PrivilegeGrant) bool {
				return grant.Privilege == privilege
			})
		}
		if held < 0 {
			return fmt.Errorf(
				"ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s REVOKE GRANT OPTION FOR %s ON %s FROM %s names a "+
					"default privilege this schema does not grant: the schema can say a privilege is held or not held, "+
					"not that it is held without the option; declare the GRANT without WITH GRANT OPTION instead",
				revoked.Grantor, revoked.Schema, privilege, revoked.ObjectType, revoked.Grantee)
		}
		database.DefaultPrivileges[index].Privileges[held].WithOption = false
	}
	return nil
}
