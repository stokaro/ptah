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
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/membershipplan"
)

// publicGrantee is the grantee whose privileges every role holds.
const publicGrantee = "PUBLIC"

// Target is what [Find] reads of the target beyond the two schemas.
type Target struct {
	// AllPrivileges is what ALL grants on each object kind of the target.
	// ALL in a revoke names what it grants on a table, so a grant on the
	// database of a privilege no table carries, such as EXECUTE, keeps
	// nothing a REVOKE ALL on a table names. A declared grant on the database
	// counts for a privilege only where ALL on a schema of the target grants
	// it: on MySQL and MariaDB a grant on a database reaches its tables, and
	// on Oracle, which lists nothing for a schema, a declared grant on one is
	// no grant on its tables. The zero value lists nothing: ALL then names
	// every privilege, and a declared grant counts for every privilege it
	// names.
	AllPrivileges descriptor.AllPrivileges
	// Semantics gives the name the target stores for a table, a database or
	// a role a declaration names, which is the name a read reports: on Oracle
	// the revoke's table t48 is the table T48 a read lists. The zero value
	// takes each name as written.
	Semantics identifier.Semantics
	// Memberships is what the plan does with role memberships, which decides
	// the holders ([ptah.run/internal/membershipplan.Read]). The zero value
	// plans none, and every membership current holds counts.
	Memberships membershipplan.Declared
}

// Cover is a grant wider than one table that gives a role a privilege a
// REVOKE on the table names.
type Cover struct {
	// Grantee holds the grant: the revoke's role, PUBLIC, or a role either
	// holds through memberships.
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
	// OnTable is true for a grant on the table itself, held by PUBLIC or by a
	// role the revoke's role holds, rather than a grant wider than the table.
	OnTable bool
}

// Find returns the grants wider than table that give role a privilege
// privileges names: a grant on the table's database, a grant on every table,
// and a grant on every table of the table's schema, that current holds for a
// holder of role's privileges, and then a grant on the table's database that
// desired declares for a holder and current does not hold yet, which the plan
// adds before the revoke would run. The holders are role, PUBLIC and every
// role either holds through memberships, as [holders] says. A grant on the
// table itself keeps the privilege too where a holder other than role holds
// it, or desired declares it for one, since the revoke takes only role's. A
// grant on the database that the plan removes is left out, because it keeps
// nothing once the plan runs: one desired revokes there, and one held by a
// role desired declares and does not grant there. target says what ALL
// grants, how the target stores a declared name, and what the plan does with
// memberships.
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
func Find(
	desired *schemamodel.Database, current *catalog.Database, role string, privileges []string, table string, target Target,
) (covers []Cover, known bool) {
	if current == nil {
		return nil, false
	}
	all := target.AllPrivileges
	spelled := table
	desired, role, table = storedNames(desired, target.Semantics), target.Semantics.StoredName(role), storedTable(table, target.Semantics)
	privileges = onTable(privileges, all.All("TABLE"))
	holding := holders(current, role, target.Memberships)
	held := func(grantee string) bool {
		_, holds := holding[grantee]
		return holds || everyAccount(current, grantee)
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
	others := func(grantee string) bool { return !sameGrantee(grantee, role) && held(grantee) }
	covers = append(covers, tableCovers(desired, current, privileges, table, spelled, scope, others)...)
	return covers, current.WideGrants != nil && !current.WideGrantsPartial
}

// storedNames is desired with the roles, tables and databases its roles,
// grants and revokes name spelled as semantics says the target stores them,
// so each compares with a read name as it is. desired is not changed.
func storedNames(desired *schemamodel.Database, semantics identifier.Semantics) *schemamodel.Database {
	if desired == nil {
		return nil
	}
	stored := &schemamodel.Database{
		Roles:         slices.Clone(desired.Roles),
		Grants:        slices.Clone(desired.Grants),
		RevokedGrants: slices.Clone(desired.RevokedGrants),
	}
	for i := range stored.Roles {
		stored.Roles[i].Name = semantics.StoredName(stored.Roles[i].Name)
	}
	for _, grants := range [][]schemamodel.Grant{stored.Grants, stored.RevokedGrants} {
		for i := range grants {
			grants[i].Role = semantics.StoredName(grants[i].Role)
			grants[i].OnSchema = semantics.StoredName(grants[i].OnSchema)
			grants[i].OnTable = storedTable(grants[i].OnTable, semantics)
		}
	}
	return stored
}

// storedTable is table, named as table or database.table, with each part
// spelled as semantics says the target stores it.
func storedTable(table string, semantics identifier.Semantics) string {
	database, name, qualified := strings.Cut(table, ".")
	if !qualified {
		return semantics.StoredName(table)
	}
	return semantics.StoredName(database) + "." + semantics.StoredName(name)
}

// tableCovers is the grants on table that current holds or desired declares
// for a grantee others names, each privilege privileges names a cover of its
// own, spelled with the table as the revoke spells it. A grant on some
// columns of the table keeps only those columns, and a grant the plan removes
// keeps nothing, as [removedFromTable] says.
func tableCovers(
	desired *schemamodel.Database, current *catalog.Database, privileges []string, table, spelled string,
	scope tableScope, others func(grantee string) bool,
) []Cover {
	_, name := tableName(table)
	var covers []Cover
	for _, grant := range current.Grants {
		if !isTable(grant.ObjectType) || grant.Column != "" || grant.ObjectName != name || !scope.reaches(grant.Schema) ||
			!others(grant.Role) || !names(privileges, grant.Privilege) || removedFromTable(desired, grant, table) {
			continue
		}
		covers = append(covers, Cover{
			Grantee: grant.Role, Grant: grant.Privilege + " ON " + spelled, OnTable: true,
			AlsoDeclared: declaresOnTable(desired, grant.Role, table, grant.Privilege),
		})
	}
	if desired == nil {
		return covers
	}
	for _, declared := range desired.Grants {
		if declared.OnTable != table || len(declared.Columns) > 0 || !others(declared.Role) {
			continue
		}
		for _, privilege := range declared.Privileges {
			privilege = strings.ToUpper(strings.TrimSpace(privilege))
			if isAll(privilege) || !names(privileges, privilege) || holdsOnTable(current, declared.Role, name, privilege, scope) {
				continue
			}
			covers = append(covers, Cover{Grantee: declared.Role, Grant: privilege + " ON " + spelled, OnTable: true, Declared: true})
		}
	}
	return covers
}

// tableName splits a revoke's table into its database, empty where it names
// none, and its name.
func tableName(table string) (database, name string) {
	database, name, qualified := strings.Cut(table, ".")
	if !qualified {
		return "", table
	}
	return database, name
}

// isTable reports whether a grant's object type is a table or a view.
func isTable(objectType string) bool {
	return strings.EqualFold(strings.TrimSpace(objectType), "TABLE")
}

// holdsOnTable reports whether current holds privilege on the table name for
// grantee, on the whole table.
func holdsOnTable(current *catalog.Database, grantee, name, privilege string, scope tableScope) bool {
	return slices.ContainsFunc(current.Grants, func(grant catalog.Grant) bool {
		return isTable(grant.ObjectType) && grant.Column == "" && grant.ObjectName == name && scope.reaches(grant.Schema) &&
			sameGrantee(grant.Role, grantee) && strings.EqualFold(strings.TrimSpace(grant.Privilege), privilege)
	})
}

// declaresOnTable reports whether desired grants privilege, or ALL, on table
// to grantee.
func declaresOnTable(desired *schemamodel.Database, grantee, table, privilege string) bool {
	return desired != nil && slices.ContainsFunc(desired.Grants, func(declared schemamodel.Grant) bool {
		return sameGrantee(declared.Role, grantee) && declared.OnTable == table && len(declared.Columns) == 0 &&
			names(declared.Privileges, privilege)
	})
}

// removedFromTable reports whether the plan removes a held grant on table, as
// [removed] reports it for a grant on a database: desired revokes the
// privilege on the table from the grant's grantee, or the grantee is a role
// desired declares and desired does not grant the privilege there.
func removedFromTable(desired *schemamodel.Database, grant catalog.Grant, table string) bool {
	if desired == nil {
		return false
	}
	there := func(declared schemamodel.Grant) bool {
		return declared.Role == grant.Role && declared.OnTable == table && len(declared.Columns) == 0 &&
			names(declared.Privileges, grant.Privilege)
	}
	if slices.ContainsFunc(desired.RevokedGrants, there) {
		return true
	}
	declared := slices.ContainsFunc(desired.Roles, func(role schemamodel.Role) bool { return role.Name == grant.Role })
	return declared && !slices.ContainsFunc(desired.Grants, there)
}

// holders is role, PUBLIC, and every role either of them holds through a
// chain of memberships: those current holds and the plan keeps, and those
// desired declares, which the plan grants, as memberships says. A member
// holds what its roles hold, so a REVOKE on a table takes nothing a role's
// wider grant gives it: measured on MySQL 8.4.13, after GRANT parent TO member
// with parent holding SELECT ON `gc%`.*, REVOKE SELECT ON gcdb.t1 FROM member
// succeeds and a session of member with member active reads t1.
//
// A membership the plan removes passes nothing on once the plan runs, and is
// left out: measured on MySQL 26.10.0, a plan that revokes SELECT on t1 from
// member and removes member's membership in parent, which holds SELECT on the
// database, leaves a session of member refused t1 with Error 1142. The plan
// revokes the privilege before it removes the membership, and nothing reads
// the table between the two statements. Where PUBLIC is a role of that name
// ([catalog.Database.PublicIsRole]), it is no holder of its own.
//
// Each holder maps to the member it is held through: empty for role and
// PUBLIC, and for any other holder a holder that holds it directly.
func holders(current *catalog.Database, role string, memberships membershipplan.Declared) map[string]string {
	held := make(map[string][]string)
	for _, membership := range current.RoleMemberships {
		if memberships.Fate(current, membership) != membershipplan.Removed {
			held[membership.Member] = append(held[membership.Member], membership.Role)
		}
	}
	for key := range memberships.Stated() {
		held[key.Member] = append(held[key.Member], key.Role)
	}
	seen := map[string]string{role: ""}
	queue := []string{role}
	if !current.PublicIsRole {
		seen[publicGrantee] = ""
		queue = append(queue, publicGrantee)
	}
	for len(queue) > 0 {
		member := queue[0]
		queue = queue[1:]
		for _, granted := range held[member] {
			if _, reached := seen[granted]; !reached {
				seen[granted] = member
				queue = append(queue, granted)
			}
		}
	}
	return seen
}

// everyAccount reports whether grantee is PUBLIC, every account's grantee,
// in any case, as MariaDB reports it in upper case whatever the GRANT wrote.
// Where PUBLIC is a role of that name ([catalog.Database.PublicIsRole]), no
// grantee is.
func everyAccount(current *catalog.Database, grantee string) bool {
	return !current.PublicIsRole && strings.EqualFold(grantee, publicGrantee)
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
//
// A revoke is refused too where a holder of the revoke's role, through the
// memberships the plan keeps, holds privileges the read does not state
// ([catalog.Database.UnreadRoles]), or where every account does
// ([catalog.Database.EveryAccountHoldsUnread]): no cover can be ruled out
// then, and a revoke planned in silence can leave a member reading the table.
// Measured on MySQL 26.10.0: with the role 'nm@localhost'@'%' holding SELECT
// on t1 beside the login 'nm'@'localhost', the role nmh holding it and
// SELECT on t1, and the login nmu holding nmh, REVOKE SELECT ON t1 FROM nmh
// succeeds and nmu still reads t1.
//
// target is what [Find] reads, for dialect.
func Validate(dialect string, desired *schemamodel.Database, current *catalog.Database, target Target) error {
	if !platform.TableRevokeTakesOnlyTableGrants(dialect) || desired == nil || current == nil {
		return nil
	}
	var problems []error
	for _, revoke := range desired.RevokedGrants {
		if revoke.OnTable == "" {
			continue
		}
		problems = append(problems, unreadHolderErrs(current, revoke, target)...)
		covers, _ := Find(desired, current, revoke.Role, revoke.Privileges, revoke.OnTable, target)
		for _, cover := range covers {
			problems = append(problems, refused{
				revoke:       revoke,
				cover:        cover,
				own:          cover.Grantee == target.Semantics.StoredName(revoke.Role),
				everyAccount: everyAccount(current, cover.Grantee),
			}.err())
		}
	}
	return errors.Join(problems...)
}

// unreadHolderErrs refuses revoke where every account holds roles the read
// does not state, and once for each principal the read did not read in full
// that a holder of revoke's role holds, as [Validate] says.
func unreadHolderErrs(current *catalog.Database, revoke schemamodel.Grant, target Target) []error {
	var problems []error
	what := fmt.Sprintf("a revoke of %s on table %s from %q", strings.Join(revoke.Privileges, ", "), revoke.OnTable, revoke.Role)
	if unread := current.EveryAccountHoldsUnread; unread != nil {
		held := "roles"
		if len(unread.Roles) > 0 {
			held = "the roles " + strings.Join(unread.Roles, ", ")
		}
		problems = append(problems, fmt.Errorf(
			"every account holds %s whose memberships the read does not state: %s; so %s cannot be shown to take the privilege",
			held, unread.Cause, what))
	}
	if len(current.UnreadRoles) == 0 {
		return problems
	}
	role := target.Semantics.StoredName(revoke.Role)
	holding := holders(current, role, target.Memberships)
	for _, unread := range current.UnreadRoles {
		member, held := holding[unread.Name]
		if !held || unread.Name == role {
			continue
		}
		holder := fmt.Sprintf("role %q holds %s", revoke.Role, unread.Name)
		remedy := "end the membership that passes " + unread.Name + " on, or leave the revoke out"
		switch {
		case member == publicGrantee && !current.PublicIsRole:
			holder = "every account holds " + unread.Name
			remedy = "leave the revoke out"
		case member != role:
			holder = fmt.Sprintf("role %q holds %s through %s", revoke.Role, unread.Name, member)
		}
		problems = append(problems, fmt.Errorf(
			"%s, whose privileges the read did not read in full: %s; so %s cannot be shown to take the privilege: %s",
			holder, unread.Cause, what, remedy))
	}
	return problems
}

// refused is a declared revoke on a table and one grant that keeps what it
// names.
type refused struct {
	revoke schemamodel.Grant
	cover  Cover
	// own reports a cover the revoke's role holds itself.
	own bool
	// everyAccount reports a cover PUBLIC holds where it is every account's
	// grantee.
	everyAccount bool
}

// err says which grant keeps what the revoke names, and where to revoke it,
// or for a grant the schema declares, that the schema gives the privilege on
// the whole database.
func (r refused) err() error {
	revoke, cover, own := r.revoke, r.cover, r.own
	role := revoke.Role
	if cover.OnTable {
		return r.tableErr()
	}
	if cover.Declared && own {
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
	if cover.AlsoDeclared && own {
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
	if own {
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

// tableErr is [refused.err] for a grant on the table itself, which another
// grantee holds or the schema grants to one.
func (r refused) tableErr() error {
	revoke, cover := r.revoke, r.cover
	role := revoke.Role
	// PUBLIC is every role's, through no membership a statement can end.
	membership := ", or end the membership that passes it on"
	if r.everyAccount {
		membership = ""
	}
	switch {
	case cover.Declared:
		return fmt.Errorf(
			"the schema grants %s to %s, which gives role %q the privilege on table %s, so a revoke from %q cannot take it: "+
				"leave the grant out%s",
			cover.Grant, cover.Grantee, role, revoke.OnTable, role, membership)
	case cover.AlsoDeclared:
		return fmt.Errorf(
			"%s holds %s, which the schema grants too and which gives role %q the privilege on table %s, "+
				"so a revoke from %q cannot take it: leave the grant out%s",
			cover.Grantee, cover.Grant, role, revoke.OnTable, role, membership)
	default:
		return fmt.Errorf(
			"%s holds %s, which gives role %q the privilege on table %s, so a revoke from %q cannot take it: "+
				"revoke %s from %s instead%s",
			cover.Grantee, cover.Grant, role, revoke.OnTable, role, cover.Grant, cover.Grantee, membership)
	}
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
