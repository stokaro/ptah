package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
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
// the privilege and takes only the right to pass it on, which the model records
// on the grant ([schemamodel.Grant.GrantOptionRevoked]). The record needs a
// grant to sit on, so that form is refused when no earlier GRANT here carries
// the privilege, and the refusal asks for that GRANT. On a server with no
// statement for it, such as Oracle, the form is refused wherever it stands:
// the GRANT it follows would be kept without the option, and the option a
// grantee holds on the server would stay held while the plan reads as synced.
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
		if err := refuseGrantOptionRevoke(revoked, target, privileges, sourcePlatform); err != nil {
			return err
		}
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

// appendDeny records a DENY ([schemamodel.Database.DeniedGrants]), one
// privilege per entry, taking the privileges it names out of every earlier
// grant and revoke of them to the same grantee on the same object.
//
// A DENY of ALL is refused: what ALL names differs from one kind of object to
// the next, and SQL Server marks it deprecated. A DENY on columns of a
// privilege an earlier statement grants on the whole table is refused as well.
// Measured on SQL Server 2025, the server keeps both rows, and a read reports
// the table grant as a grant on each other column, which the file does not
// declare, so every plan would grant the table again and take the DENY with
// it.
func appendDeny(database *schemamodel.Database, node *ast.DenyPrivilegeNode, sourcePlatform string) error {
	denied := schemamodel.Grant{Role: normalizeGrantee(sourcePlatform, node.Role), Comment: node.Comment}
	setGrantTarget(&denied, node.ObjectType, node.ObjectName, "", sourcePlatform)
	denied.Columns = normalizeColumns(sourcePlatform, node.Columns)
	denied.Canonicalize()
	privileges := make([]string, 0, len(node.Privileges))
	for _, privilege := range node.Privileges {
		privileges = append(privileges, strings.ToUpper(strings.TrimSpace(privilege)))
	}
	if slices.Contains(privileges, allPrivileges) {
		return fmt.Errorf("DENY ALL ON %s TO %s: ALL names different permissions on each kind of object; "+
			"name the permissions to deny", denied.TargetKey(), denied.Role)
	}
	if len(denied.Columns) > 0 {
		for _, privilege := range privileges {
			table, held := privilegefold.TableGrantOf(database.Grants, denied.Role, privilege, oneTableWith(denied.OnTable))
			if !held {
				continue
			}
			return fmt.Errorf(
				"DENY %s (%s) ON %s TO %s follows a GRANT of %s on the whole table, which the server reads back "+
					"as a grant on each other column; grant it on the columns that keep it instead%s",
				privilege, strings.Join(denied.Columns, ", "), denied.OnTable, denied.Role, privilege,
				qualifyBoth(denied.OnTable, table.OnTable))
		}
	}
	denied.Privileges = privileges
	privilegefold.Merge(database, &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denied}})
	return nil
}

// refuseGrantOptionRevoke refuses a REVOKE GRANT OPTION FOR read for a
// server that has no statement taking the grant option alone; see
// [descriptor.Descriptor.GrantOptionRevokedWithPrivilege].
func refuseGrantOptionRevoke(revoked schemamodel.Grant, target string, privileges []string, sourcePlatform string) error {
	facts := factsOf(sourcePlatform)
	if !facts.grantOptionRevokedWithPrivilege {
		return nil
	}
	return fmt.Errorf(
		"%w: REVOKE GRANT OPTION FOR %s ON %s FROM %s: %s has no statement that takes the grant option and leaves "+
			"the privilege, and a REVOKE also takes the privilege from everyone %s granted it to; declare the GRANT "+
			"without WITH GRANT OPTION, and take an option the server holds by revoking the privilege and granting "+
			"it again by hand",
		ptaherr.ErrUnsupportedFeature, strings.Join(privileges, ", "), target, revoked.Role, facts.name, revoked.Role)
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
	if table == "" || revoked.TargetKey() != "TABLE "+table {
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
					"earlier GRANT %s (%s) ON %s gives it on columns, which keep it; %s%s",
				strings.Join(privileges, ", "), revoked.TargetKey(), revoked.Role,
				privilege, strings.Join(grant.Columns, ", "), grant.OnTable,
				optionKeptOnColumnsRemedy(grants, grant, privilege), qualifyBoth(table, grant.OnTable))
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
	if _, held := privilegefold.TableGrantOf(grants, column.Role, privilege, oneTableWith(column.OnTable)); held {
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

// oneTableWith is [mayNameOneTable] against table, in the form
// [privilegefold.TableGrantOf] asks for.
func oneTableWith(table string) func(string) bool {
	return func(other string) bool { return mayNameOneTable(table, other) }
}

// qualifyBoth is the advice a refusal over two spellings [mayNameOneTable]
// matched adds when one is qualified and the other is not: the reader cannot
// tell whether the bare one resolves to the qualified table, and qualifying
// both says which. Spellings that name one table, or two qualified ones, need
// no advice.
func qualifyBoth(a, b string) string {
	left, leftOK := tableref.Parse(strings.TrimSpace(a))
	right, rightOK := tableref.Parse(strings.TrimSpace(b))
	if !leftOK || !rightOK || left.Qualified == right.Qualified {
		return ""
	}
	return fmt.Sprintf("; if %s and %s are different tables, qualify both", strings.TrimSpace(a), strings.TrimSpace(b))
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
// in this file made, splitting a grant whose other privileges keep it, and
// records that the schema takes the option
// ([schemamodel.Grant.GrantOptionRevoked]). The record is what makes the
// statement reach a role the schema does not manage: a grant without the
// option alone leaves such a role's option held, and the next plan reads as
// synced while the role can still grant the privilege onward. It is kept on a
// grant made without the option too, since the statement asks the same of
// the database whatever the file granted.
func revokeGrantOption(database *schemamodel.Database, role, target string, privileges []string) error {
	for _, privilege := range privileges {
		privilege = strings.ToUpper(strings.TrimSpace(privilege))
		index := slices.IndexFunc(database.Grants, func(grant schemamodel.Grant) bool {
			return grant.Role == role && grant.TargetKey() == target && slices.Contains(grant.Privileges, privilege)
		})
		if index < 0 {
			return fmt.Errorf(
				"REVOKE GRANT OPTION FOR %s ON %s FROM %s names a grant this schema does not make: "+
					"declare `GRANT %s ON %s TO %s;` before this REVOKE", privilege, target, role, privilege, target, role)
		}
		plain := database.Grants[index]
		plain.Privileges = []string{privilege}
		plain.WithOption = false
		plain.GrantOptionRevoked = true
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
// The table is matched by [mayNameOneTable], as the grant-option refusal
// matches it.
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
		table, held := privilegefold.TableGrantOf(database.Grants, revoked.Role, privilege, oneTableWith(revoked.OnTable))
		if !held {
			continue
		}
		granted := "a GRANT of " + privilege
		if privilege == allPrivileges {
			granted = "a GRANT"
		}
		return fmt.Errorf(
			"%s %s (%s) ON %s FROM %s follows %s on the whole table, which covers every column, "+
				"so revoking it on columns takes nothing away; grant it on the columns that keep it instead%s",
			statement, privilege, strings.Join(revoked.Columns, ", "), revoked.OnTable, revoked.Role, granted,
			qualifyBoth(revoked.OnTable, table.OnTable))
	}
	return nil
}
