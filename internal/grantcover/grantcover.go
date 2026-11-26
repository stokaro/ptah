// Package grantcover finds the grants wider than one table that keep a
// privilege a declared REVOKE on the table names: the ones the database
// holds, and the grants on the table's database the schema declares and the
// plan adds. It serves targets where a REVOKE on a table takes only a grant
// on that table ([platform.TableRevokeTakesOnlyTableGrants]): there such a
// revoke cannot take the privilege, and the server refuses it when the role
// holds nothing on the table itself.
//
// The grant comparison asks [Find] before it leaves out a revoke on a table
// the plan creates, and the comparison's callers refuse, through [Validate], a
// declaration that revokes a privilege a wider grant keeps. Both read one
// predicate, so the revoke the comparison leaves out is never one the
// refusal would have named.
package grantcover

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
)

// publicGrantee is the grantee whose privileges every role holds.
const publicGrantee = "PUBLIC"

// Cover is a grant wider than one table that gives a role a privilege a
// REVOKE on the table names.
type Cover struct {
	// Grantee holds the grant: the revoke's role, or PUBLIC.
	Grantee string
	// Grant is what the grantee holds, in the words a REVOKE of it uses:
	// "SELECT ON d2.*", "SELECT ON *.*", "SELECT ANY TABLE" or "SELECT ANY
	// TABLE ON SCHEMA APP". A declared grant is spelled with its own
	// privileges, those the revoke names and the database does not hold yet:
	// "ALL ON d2.*" or "SELECT, INSERT ON d2.*".
	Grant string
	// Declared is true for a grant on the table's database that the schema
	// declares and the database does not hold yet. The plan grants it, so it
	// keeps the privilege as a held one does.
	Declared bool
	// AlsoDeclared is true for a held grant on the table's database that the
	// schema declares too. A revoke of it is no way out, since the plan would
	// grant it again.
	AlsoDeclared bool
}

// Find returns the grants wider than table that give role a privilege
// privileges names: a grant on the table's database, a grant on every table,
// and a grant on every table of the table's schema, that current holds for the
// role or for PUBLIC, and then a grant on the table's database that desired
// declares for either and current does not hold yet, which the plan adds
// before the revoke would run. A grant on the database that the plan removes
// is left out, because it keeps nothing once the plan runs: one desired
// revokes there, and one held by a role desired declares and does not grant
// there.
//
// all is what ALL grants on each object kind of the target. ALL in privileges
// names what it grants on a table, so a grant on the database of a privilege
// no table carries, such as EXECUTE, keeps nothing a REVOKE ALL on a table
// names. A declared grant on the database counts for a privilege only where
// ALL on a schema of the target grants it: on MySQL and MariaDB a grant on a
// database reaches its tables, and on Oracle, which lists nothing for a
// schema, a declared grant on one is no grant on its tables. The zero value
// lists nothing: ALL then names every privilege, and a declared grant counts
// for every privilege it names.
//
// A table named without its database is in the database the read describes,
// [catalog.Database.CurrentDatabase] where the read names it, and a grant on
// another database is no cover for it. That database is compared without
// regard to case, as a server that folds names compares it, and so is a table
// that names it in any case. A read that does
// not name its database reports grants on it alone, so a grant on any
// database counts for the table there: a declared grant whose database cannot
// be compared only refuses more.
//
// known is false when current does not say which grants on every table
// exist, or says only some of them, as a read made without the privilege to
// look does not. The covers are then the grants the read established, and a
// caller cannot tell that nothing else covers the table.
func Find(desired *schemamodel.Database, current *catalog.Database, role string, privileges []string, table string, all descriptor.AllPrivileges) (covers []Cover, known bool) {
	if current == nil {
		return nil, false
	}
	privileges = onTable(privileges, all.All("TABLE"))
	held := func(grantee string) bool {
		return sameGrantee(grantee, role) || strings.EqualFold(grantee, publicGrantee)
	}
	scope := scopeOf(current, table)
	for _, grant := range current.Grants {
		if !isSchema(grant.ObjectType) || !scope.reaches(grant.ObjectName) || !held(grant.Role) || !names(privileges, grant.Privilege) || removed(desired, grant) {
			continue
		}
		covers = append(covers, Cover{
			Grantee: grant.Role, Grant: grant.Privilege + " ON " + grant.ObjectName + ".*", AlsoDeclared: declares(desired, grant),
		})
	}
	for _, grant := range current.WideGrants {
		elsewhere := grant.Schema != "" && scope.qualified && !strings.EqualFold(grant.Schema, scope.database)
		if !elsewhere && held(grant.Role) && names(privileges, grant.Privilege) {
			covers = append(covers, Cover{Grantee: grant.Role, Grant: grant.Grant})
		}
	}
	covers = append(covers, declaredCovers(desired, current, privileges, all, scope, held)...)
	return covers, current.WideGrants != nil && !current.WideGrantsPartial
}

// tableScope is the database a revoke's table is in, as far as the read says.
type tableScope struct {
	database string
	// qualified is true where the database is known: the table names it, or
	// the read names the database it describes.
	qualified bool
	// session is true where the database is the one the read describes: a
	// table named without its database, or one that names that database in
	// any case.
	session bool
}

// scopeOf is the database of table, named as database.table or, without its
// database, in the database current describes.
func scopeOf(current *catalog.Database, table string) tableScope {
	database, _, qualified := strings.Cut(table, ".")
	if !qualified {
		database = current.CurrentDatabase
	}
	session := current.CurrentDatabase != "" && strings.EqualFold(database, current.CurrentDatabase)
	return tableScope{database: database, qualified: database != "", session: session}
}

// reaches reports whether a grant on database name gives a privilege on the
// table. Where the database is unknown, a grant on any database does. The
// database the read describes is compared without regard to case: a server
// that folds names, with lower_case_table_names 1 or 2, holds one database
// under both spellings, and on one that does not, a grant on a database that
// differs only in case only refuses more.
func (s tableScope) reaches(name string) bool {
	return !s.qualified || name == s.database || (s.session && strings.EqualFold(name, s.database))
}

// declaredCovers is the grants on the table's database that desired declares
// for a grantee held names, spelled with the privileges the plan adds.
func declaredCovers(
	desired *schemamodel.Database, current *catalog.Database, privileges []string, all descriptor.AllPrivileges,
	scope tableScope, held func(grantee string) bool,
) []Cover {
	if desired == nil {
		return nil
	}
	var covers []Cover
	for _, declared := range desired.Grants {
		grantee := declared.Role
		if declared.OnSchema == "" || !scope.reaches(declared.OnSchema) || !held(grantee) {
			continue
		}
		if granted := planned(declared, current, privileges, all); granted != "" {
			covers = append(covers, Cover{Grantee: grantee, Grant: granted + " ON " + declared.OnSchema + ".*", Declared: true})
		}
	}
	return covers
}

// isSchema reports whether a grant's object type is a whole database.
func isSchema(objectType string) bool {
	return strings.EqualFold(strings.TrimSpace(objectType), "SCHEMA")
}

// planned returns what the declared grant on a database adds of privileges:
// ALL when it names ALL, or else each privilege it names that privileges
// names too, in its order. A privilege ALL on a schema of the target does not
// grant adds nothing to the database's tables, nor does one current holds
// there already.
func planned(declared schemamodel.Grant, current *catalog.Database, privileges []string, all descriptor.AllPrivileges) string {
	grantee := declared.Role
	onSchema := all.All("SCHEMA")
	adds := func(privilege string) bool {
		reaches := len(all.Kinds) == 0 || slices.ContainsFunc(onSchema, func(name string) bool { return strings.EqualFold(name, privilege) })
		return reaches && names(privileges, privilege) && !slices.ContainsFunc(current.Grants, func(grant catalog.Grant) bool {
			return strings.EqualFold(strings.TrimSpace(grant.ObjectType), "SCHEMA") && sameGrantee(grant.Role, grantee) &&
				grant.ObjectName == declared.OnSchema && strings.EqualFold(strings.TrimSpace(grant.Privilege), privilege)
		})
	}
	var added []string
	for _, privilege := range declared.Privileges {
		privilege = strings.ToUpper(strings.TrimSpace(privilege))
		if isAll(privilege) {
			if slices.ContainsFunc(privileges, adds) {
				return "ALL"
			}
			continue
		}
		if adds(privilege) {
			added = append(added, privilege)
		}
	}
	return strings.Join(added, ", ")
}

// Validate refuses each declared revoke on a table that a grant wider than
// the table keeps, on a target where a REVOKE on a table takes only a grant on
// that table, and returns nil on every other target. The revoke is refused
// whether the plan creates the table or it exists: on an existing table the
// plan finds nothing under the table's name to revoke and reads as synced,
// and a member of the role keeps the privilege.
func Validate(dialect string, desired *schemamodel.Database, current *catalog.Database) error {
	if !platform.TableRevokeTakesOnlyTableGrants(dialect) || desired == nil || current == nil {
		return nil
	}
	target, _ := platform.Lookup(dialect)
	var problems []error
	for _, revoke := range desired.RevokedGrants {
		if revoke.OnTable == "" {
			continue
		}
		covers, _ := Find(desired, current, revoke.Role, revoke.Privileges, revoke.OnTable, target.AllPrivileges)
		for _, cover := range covers {
			problems = append(problems, refusal(revoke, cover))
		}
	}
	return errors.Join(problems...)
}

// refusal says which grant keeps what revoke names, and where to revoke it,
// or for a grant the schema declares, that the schema gives the privilege on
// the whole database.
func refusal(revoke schemamodel.Grant, cover Cover) error {
	role := revoke.Role
	if cover.Declared && cover.Grantee == role {
		return fmt.Errorf(
			"the schema grants %s to role %q, which covers table %s, so a revoke on the table cannot take it: "+
				"grant the privilege on each table that needs it instead of on the database",
			cover.Grant, role, revoke.OnTable)
	}
	if cover.Declared {
		return fmt.Errorf(
			"the schema grants %s to %s, which gives role %q the privilege on table %s, so a revoke on the table cannot take it: "+
				"grant the privilege on each table that needs it instead of on the database",
			cover.Grant, cover.Grantee, role, revoke.OnTable)
	}
	if cover.AlsoDeclared && cover.Grantee == role {
		return fmt.Errorf(
			"role %q holds %s, which the schema grants too and which covers table %s, so a revoke on the table cannot take it: "+
				"grant the privilege on each table that needs it instead of on the database",
			role, cover.Grant, revoke.OnTable)
	}
	if cover.AlsoDeclared {
		return fmt.Errorf(
			"%s holds %s, which the schema grants too and which gives role %q the privilege on table %s, "+
				"so a revoke on the table cannot take it: grant the privilege on each table that needs it instead of on the database",
			cover.Grantee, cover.Grant, role, revoke.OnTable)
	}
	if cover.Grantee == role {
		return fmt.Errorf(
			"role %q holds %s, which covers table %s, so a revoke on the table cannot take it: "+
				"revoke %s from %q instead, or grant the privilege on each table that needs it",
			role, cover.Grant, revoke.OnTable, cover.Grant, role)
	}
	return fmt.Errorf(
		"%s holds %s, which gives role %q the privilege on table %s, so a revoke on the table cannot take it: "+
			"revoke %s from %s instead, or grant the privilege on each table that needs it",
		cover.Grantee, cover.Grant, role, revoke.OnTable, cover.Grant, cover.Grantee)
}

// names reports whether a revoke of privileges takes privilege.
func names(privileges []string, privilege string) bool {
	return slices.ContainsFunc(privileges, func(name string) bool {
		return isAll(name) || strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(privilege))
	})
}

// sameGrantee reports whether two grantees are one: a role by its exact name,
// and PUBLIC in any case, as MariaDB reports it in upper case whatever the
// GRANT wrote.
func sameGrantee(a, b string) bool {
	return a == b || (strings.EqualFold(a, publicGrantee) && strings.EqualFold(b, publicGrantee))
}

// isAll reports whether privilege is the keyword ALL, in either spelling.
func isAll(privilege string) bool {
	privilege = strings.ToUpper(strings.TrimSpace(privilege))
	return privilege == "ALL" || privilege == "ALL PRIVILEGES"
}

// onTable spells ALL in a revoke on a table as what ALL grants on a table of
// the target, tableAll, and keeps every other privilege as it is. An empty
// tableAll leaves ALL naming every privilege.
func onTable(privileges, tableAll []string) []string {
	if len(tableAll) == 0 || !slices.ContainsFunc(privileges, isAll) {
		return privileges
	}
	spelled := make([]string, 0, len(privileges)+len(tableAll))
	for _, privilege := range privileges {
		if isAll(privilege) {
			spelled = append(spelled, tableAll...)
			continue
		}
		spelled = append(spelled, privilege)
	}
	return spelled
}

// declares reports whether desired grants the privilege of a grant on a
// database, or ALL, to its grantee on the same database.
func declares(desired *schemamodel.Database, grant catalog.Grant) bool {
	return desired != nil && slices.ContainsFunc(desired.Grants, func(declared schemamodel.Grant) bool {
		return sameGrantee(declared.Role, grant.Role) && declared.OnSchema == grant.ObjectName &&
			names(declared.Privileges, grant.Privilege)
	})
}

// removed reports whether the plan removes a grant on a database: desired
// revokes the privilege there from the grant's grantee, or the grantee is a
// role desired declares and desired does not grant the privilege, or ALL,
// there. The grant comparison removes both, so neither keeps anything once the
// plan runs.
func removed(desired *schemamodel.Database, grant catalog.Grant) bool {
	if desired == nil {
		return false
	}
	there := func(declared schemamodel.Grant) bool {
		return declared.Role == grant.Role && declared.OnSchema == grant.ObjectName && names(declared.Privileges, grant.Privilege)
	}
	if slices.ContainsFunc(desired.RevokedGrants, there) {
		return true
	}
	declared := slices.ContainsFunc(desired.Roles, func(role schemamodel.Role) bool { return role.Name == grant.Role })
	return declared && !slices.ContainsFunc(desired.Grants, there)
}
