package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/privilegefold"
)

// allPrivileges is what ALL names for each target kind, measured on
// PostgreSQL 18 with aclexplode after GRANT ALL. MAINTAIN exists from
// PostgreSQL 17; naming it in a REVOKE on an older server asserts the absence
// of a privilege that server cannot hold, which is true.
var allPrivileges = map[string][]string{
	"TABLE":     {"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"},
	"SCHEMA":    {"USAGE", "CREATE"},
	"SEQUENCE":  {"USAGE", "SELECT", "UPDATE"},
	"FUNCTION":  {"EXECUTE"},
	"PROCEDURE": {"EXECUTE"},
	"ROUTINE":   {"EXECUTE"},
}

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
	kind := strings.ToUpper(objectType)
	expanded := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		if strings.EqualFold(privilege, "ALL") {
			expanded = append(expanded, allPrivileges[kind]...)
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
func normalizeGrantee(role string) string {
	normalized := normalizeSQLIdentifier(role)
	quoted := strings.HasPrefix(strings.TrimSpace(role), `"`)
	if (!quoted && strings.EqualFold(normalized, "PUBLIC")) || (quoted && normalized == "public") {
		return "PUBLIC"
	}
	return normalized
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

func toGrant(node *ast.GrantPrivilegeNode) schemamodel.Grant {
	grant := schemamodel.Grant{
		Role:       normalizeGrantee(node.Role),
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
func appendGrant(database *schemamodel.Database, node *ast.GrantPrivilegeNode) {
	privilegefold.Merge(database, &schemamodel.Database{Grants: []schemamodel.Grant{toGrant(node)}})
}

// appendRevoke records a REVOKE.
//
// Each privilege it names leaves every earlier grant of it to the same grantee
// on the same object and joins the revoked set. REVOKE GRANT OPTION FOR keeps
// the privilege and takes only the right to pass it on, which the model can say
// only about a grant this file made: it has no spelling for "held, but without
// the option" on its own, so that form is refused when no earlier GRANT here
// carries the privilege.
func appendRevoke(database *schemamodel.Database, node *ast.RevokePrivilegeNode) error {
	revoked := schemamodel.Grant{Role: normalizeGrantee(node.Role), Comment: node.Comment}
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
