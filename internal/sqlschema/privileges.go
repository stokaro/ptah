package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgprivilege"
	"ptah.run/internal/privilegefold"
	"ptah.run/internal/tableref"
)

// expandGrantedAll replaces ALL in a GRANT with the privileges it names on a
// routine, which is EXECUTE on every server. A table's ALL depends on the
// server version, so a GRANT ALL on any other kind keeps the spelling.
func expandGrantedAll(privileges []string, objectType string) []string {
	if !routineObjectTypes[strings.ToUpper(objectType)] {
		return privileges
	}
	expanded := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		if strings.EqualFold(privilege, allPrivileges) {
			expanded = append(expanded, pgprivilege.All(objectType)...)
			continue
		}
		expanded = append(expanded, privilege)
	}
	return expanded
}

// allPrivileges is the keyword GRANT and REVOKE spell for every privilege on
// their object.
const allPrivileges = "ALL"

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
	if factsOf(sourcePlatform).names.PublicKeyword {
		name := roleName(sourcePlatform, role)
		if name == "public" {
			return "PUBLIC"
		}
		return name
	}
	normalized := normalizeSQLIdentifier(sourcePlatform, role)
	quoted := strings.HasPrefix(strings.TrimSpace(role), `"`)
	if (!quoted && strings.EqualFold(normalized, "PUBLIC")) || (quoted && normalized == "public") {
		return "PUBLIC"
	}
	return normalized
}

// setGrantTarget sets the target of a grant from a statement's object type and
// name, which GRANT and REVOKE share.
func setGrantTarget(grant *schemamodel.Grant, objectType, objectName, arguments, sourcePlatform string) {
	switch kind := strings.ToUpper(objectType); {
	case kind == "SCHEMA":
		grant.OnSchema = normalizeSQLIdentifier(sourcePlatform, objectName)
	case kind == "SEQUENCE":
		grant.OnSequence = normalizeSQLTableReference(sourcePlatform, objectName)
	case routineObjectTypes[kind]:
		grant.OnRoutine = normalizeSQLTableReference(sourcePlatform, objectName)
		grant.RoutineArguments = strings.TrimSpace(arguments)
		grant.RoutineKind = kind
	default:
		grant.OnTable = normalizeSQLTableReference(sourcePlatform, objectName)
	}
}

func toGrant(node *ast.GrantPrivilegeNode, sourcePlatform string) schemamodel.Grant {
	grant := schemamodel.Grant{
		Role:       normalizeGrantee(sourcePlatform, node.Role),
		Privileges: expandGrantedAll(node.Privileges, node.ObjectType),
		WithOption: node.WithOption,
		Comment:    node.Comment,
	}
	setGrantTarget(&grant, node.ObjectType, node.ObjectName, node.Arguments, sourcePlatform)
	grant.Columns = normalizeColumns(sourcePlatform, node.Columns)
	grant.Canonicalize()
	return grant
}

// normalizeColumns unquotes a column list, as a column name is compared with
// the name the catalog reports.
func normalizeColumns(sourcePlatform string, columns []string) []string {
	if len(columns) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(columns))
	for _, column := range columns {
		normalized = append(normalized, normalizeSQLIdentifier(sourcePlatform, column))
	}
	return normalized
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
//
// A REVOKE ALL is kept as ALL, the absence of every privilege on the object,
// on every kind of object and for every source platform. Spelled as a list,
// it would name the privileges ALL covers on one server and miss the rest
// elsewhere: written as PostgreSQL's table list, a REVOKE ALL on MySQL takes
// SELECT and TRIGGER and leaves ALTER, DROP and INDEX held, measured on MySQL
// 8.4.13. The comparison matches it against whatever the database reports on
// the object, so the list is never needed.
func appendRevoke(database *schemamodel.Database, node *ast.RevokePrivilegeNode, sourcePlatform string) error {
	revoked := schemamodel.Grant{Role: normalizeGrantee(sourcePlatform, node.Role), Comment: node.Comment}
	setGrantTarget(&revoked, node.ObjectType, node.ObjectName, node.Arguments, sourcePlatform)
	revoked.Columns = normalizeColumns(sourcePlatform, node.Columns)
	revoked.Canonicalize()
	target := revoked.TargetKey()
	privileges := make([]string, 0, len(node.Privileges))
	for _, privilege := range node.Privileges {
		privileges = append(privileges, strings.ToUpper(strings.TrimSpace(privilege)))
	}
	statement := "REVOKE"
	if node.GrantOptionFor {
		statement = "REVOKE GRANT OPTION FOR"
	}
	if err := refuseColumnRevokeUnderTableGrant(database, revoked, statement, privileges); err != nil {
		return err
	}
	revokesAll := slices.Contains(privileges, allPrivileges)
	if !revokesAll && privilegefold.HeldAsAll(database.Grants, revoked.Role, target) {
		return fmt.Errorf(
			"REVOKE ON %s FROM %s follows a GRANT ALL on it, which this schema keeps as ALL rather than as the "+
				"privileges ALL names on a given server, so it cannot take one privilege out of it; "+
				"name the privileges in the GRANT", target, revoked.Role)
	}
	if node.GrantOptionFor {
		if err := refuseOptionKeptOnColumns(database.Grants, revoked, privileges); err != nil {
			return err
		}
		if revokesAll {
			privileges = heldPrivileges(database.Grants, revoked.Role, target)
		}
		return revokeGrantOption(database, revoked.Role, target, privileges)
	}
	revoked.Privileges = privileges
	privilegefold.Merge(database, &schemamodel.Database{RevokedGrants: []schemamodel.Grant{revoked}})
	return nil
}

// refuseOptionKeptOnColumns refuses a REVOKE GRANT OPTION FOR on a whole table
// when an earlier GRANT in this file gives the role one of the privileges it
// names on columns of that table WITH GRANT OPTION. A revoke of ALL names every
// privilege, and a column grant of ALL carries every privilege a revoke names.
// The option is cleared on the table grants alone, so the column grant would
// keep it: a member could still grant the column privilege onward, and the
// next plan would read as synced.
//
// The servers' own statement reaches the column grants too: measured on
// PostgreSQL 18, MySQL 8.4 and SQL Server 2025, a table-level revoke takes the
// option from them. This reader cannot fold the option exactly as each server
// does, a table ALL whose members depend on the server version among the
// cases, so it refuses rather than guess, and the declaration says which grant
// loses the option.
//
// The table is matched by [mayNameOneTable], so a bare name and a qualified one
// with the same relation name count as one table.
func refuseOptionKeptOnColumns(grants []schemamodel.Grant, revoked schemamodel.Grant, privileges []string) error {
	table := strings.TrimSpace(revoked.OnTable)
	if table == "" || len(revoked.Columns) > 0 || revoked.TargetKey() != "TABLE "+table {
		return nil
	}
	revokesAll := slices.Contains(privileges, allPrivileges)
	for _, grant := range grants {
		if grant.Role != revoked.Role || !grant.WithOption || len(grant.Columns) == 0 || !mayNameOneTable(grant.OnTable, table) {
			continue
		}
		for _, privilege := range grant.Privileges {
			if !revokesAll && privilege != allPrivileges && !slices.Contains(privileges, privilege) {
				continue
			}
			return fmt.Errorf(
				"REVOKE GRANT OPTION FOR %s ON %s FROM %s takes the option from grants on the whole table, and an "+
					"earlier GRANT %s (%s) ON %s gives it on columns, which keep it; %s",
				strings.Join(privileges, ", "), revoked.TargetKey(), revoked.Role,
				privilege, strings.Join(grant.Columns, ", "), grant.OnTable,
				optionKeptOnColumnsRemedy(grants, grant, privilege))
		}
	}
	return nil
}

// optionKeptOnColumnsRemedy is what the refusal of [refuseOptionKeptOnColumns]
// asks for. Declaring the column grant without WITH GRANT OPTION always works.
// Revoking the option on the columns works only before the table REVOKE, which
// is refused while the column grant keeps the option, and only where the role
// holds no grant of the privilege on the whole table: such a grant covers every
// column, and [refuseColumnRevokeUnderTableGrant] refuses the column revoke.
func optionKeptOnColumnsRemedy(grants []schemamodel.Grant, column schemamodel.Grant, privilege string) string {
	const plain = "declare that GRANT without WITH GRANT OPTION"
	if privilegefold.HeldOnTable(grants, column.Role, column.OnTable, privilege) {
		return plain
	}
	return plain + ", or revoke the option on those columns before this REVOKE"
}

// mayNameOneTable reports whether two table references in one file may name
// the same table. A bare name and a qualified name with the same relation name
// may: the schema the bare one resolves to is the server's search path, which
// this reader does not know. Two qualified names are one table only when they
// are equal. A caller that refuses on a match over-refuses rather than miss
// one.
func mayNameOneTable(a, b string) bool {
	a, b = strings.TrimSpace(a), strings.TrimSpace(b)
	if a == b {
		return true
	}
	left, leftOK := tableref.Parse(a)
	right, rightOK := tableref.Parse(b)
	return leftOK && rightOK && left.Name == right.Name && (!left.Qualified || !right.Qualified)
}

// heldPrivileges is every privilege an earlier grant in this file gives role on
// target, in the order the file grants them, which is what REVOKE GRANT OPTION
// FOR ALL names. It is ALL alone when the file grants nothing there, so the
// refusal for a grant this file does not make names what the statement says.
func heldPrivileges(grants []schemamodel.Grant, role, target string) []string {
	var held []string
	for _, grant := range grants {
		if grant.Role != role || grant.TargetKey() != target {
			continue
		}
		for _, privilege := range grant.Privileges {
			if !slices.Contains(held, privilege) {
				held = append(held, privilege)
			}
		}
	}
	if len(held) == 0 {
		return []string{allPrivileges}
	}
	return held
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

// refuseColumnRevokeUnderTableGrant refuses a revoke on columns of a privilege
// an earlier statement grants on the whole table. The table privilege covers
// every column and a column revoke does not take it away -- measured on
// PostgreSQL 18, has_column_privilege still answers true after GRANT UPDATE ON
// t and REVOKE UPDATE (a) ON t -- so the statement would change nothing. The
// option a table grant gives covers every column the same way. statement is
// how the refusal spells the revoke, `REVOKE` or `REVOKE GRANT OPTION FOR`.
func refuseColumnRevokeUnderTableGrant(
	database *schemamodel.Database,
	revoked schemamodel.Grant,
	statement string,
	privileges []string,
) error {
	if len(revoked.Columns) == 0 {
		return nil
	}
	for _, privilege := range privileges {
		if !privilegefold.HeldOnTable(database.Grants, revoked.Role, revoked.OnTable, privilege) {
			continue
		}
		granted := "a GRANT of " + privilege
		if privilege == allPrivileges {
			granted = "a GRANT"
		}
		return fmt.Errorf(
			"%s %s (%s) ON %s FROM %s follows %s on the whole table, which covers every column, "+
				"so revoking it on columns takes nothing away; grant it on the columns that keep it instead",
			statement, privilege, strings.Join(revoked.Columns, ", "), revoked.OnTable, revoked.Role, granted)
	}
	return nil
}
