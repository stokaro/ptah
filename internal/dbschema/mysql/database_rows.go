package mysql

import (
	"context"
	"maps"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/mysqlgrants"
	"ptah.run/internal/mysqlname"
	"ptah.run/internal/rowaccess"
)

// databaseRows are the database-level grant rows the roles hold: by role, the
// database name each row names and the privileges it holds, in upper case. A
// grant on a database name pattern, such as GRANT SELECT ON `cs%`.*, names no
// database the read describes, so the grants on the database leave it out,
// and a role whose member reads every table there would read as holding
// nothing. A nil value is a read that did not establish the rows.
//
// The rows feed two readers that answer different questions on purpose.
// [databaseRows.patternGrants] is what the revoke refusal reads, and it
// reports every pattern that reaches a database, since a revoke under any of
// them may be undone. [databaseRows.databaseAccess] is what the security
// analysis reads, and it reports only the row the server applies.
type databaseRows struct {
	options mysqlgrants.Options
	held    map[string]map[string][]string
}

// readDatabaseRows reads every database-level row of every role, the rows on
// a database's own name and the rows on a pattern alike. mariaDB is
// [mysqlgrants.Options.MariaDB].
//
// MySQL with partial_revokes ON reads "%" and "_" in a database name as the
// characters themselves, so a pattern grant reaches no other database there:
// measured on MySQL 8.4.13, after GRANT SELECT ON `wc%`.*, a member is refused
// wc1.t. MariaDB has no such setting. The read then returns no rows, since no
// row but the database's own applies to it.
func (r *Reader) readDatabaseRows(ctx context.Context, mariaDB bool) (*databaseRows, error) {
	options, err := mysqlgrants.ReadOptions(ctx, r.db)
	if err != nil {
		return nil, err
	}
	options.MariaDB = mariaDB
	rows := &databaseRows{options: options, held: make(map[string]map[string][]string)}
	if options.LiteralDatabaseNames {
		return rows, nil
	}
	predicate, err := r.rolePredicate(ctx)
	if err != nil {
		return nil, err
	}
	result, err := r.db.QueryContext(ctx, databaseRowQuery(qualifyPredicate(predicate, "u.")))
	if err != nil {
		return nil, err
	}
	defer result.Close()
	for result.Next() {
		var role, name, privilege string
		if err := result.Scan(&role, &name, &privilege); err != nil {
			return nil, err
		}
		if rows.held[role] == nil {
			rows.held[role] = make(map[string][]string)
		}
		rows.held[role][name] = append(rows.held[role][name], strings.ToUpper(strings.TrimSpace(privilege)))
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

// databaseRowQuery projects one row per privilege a role holds through a
// database-level grant row.
func databaseRowQuery(rolePredicate string) string {
	const granteeUser = `REPLACE(SUBSTRING_INDEX(sp.GRANTEE, '@', 1), '''', '')`
	return `
		SELECT ` + granteeUser + ` AS role_name, sp.TABLE_SCHEMA, sp.PRIVILEGE_TYPE
		FROM information_schema.SCHEMA_PRIVILEGES AS sp
		JOIN mysql.user AS u ON u.user = ` + granteeUser + `
		WHERE ` + rolePredicate + `
			  AND u.user NOT LIKE 'mysql.%'
		ORDER BY role_name, sp.TABLE_SCHEMA, sp.PRIVILEGE_TYPE`
}

// patternGrants are the table privileges each role holds on each of databases
// through a row on another name that reaches it, one grant per privilege,
// reported as wide on the database it reaches: a REVOKE on one table takes
// none of it. Measured on MySQL 8.4.13 and MariaDB 11.8.9, after GRANT SELECT
// ON `wc%`.*, a member reads wc1.t and REVOKE SELECT ON wc1.t fails with Error
// 1147. A privilege no grant on a table names, such as EVENT, is left out, as
// [Reader.readWideGrants] leaves it out.
//
// Every row that reaches the database is reported, including one the server
// does not apply because another row shadows it: the refusal must not plan a
// revoke that a pattern undoes once the row shadowing it is gone, so it fails
// closed. [mysqlgrants.Options.MatchesDatabase] decides which rows reach it,
// as the server's privilege check does; SQL LIKE disagrees with that check on
// a multi-byte name and, on MySQL under NO_BACKSLASH_ESCAPES, on an escaped
// wildcard.
func (d *databaseRows) patternGrants(databases []string) []catalog.WideGrant {
	if d == nil {
		return nil
	}
	same := mysqlname.NamesOf(d.options.FoldCase)
	grants := make([]catalog.WideGrant, 0)
	for _, database := range databases {
		for _, role := range slices.Sorted(maps.Keys(d.held)) {
			for _, name := range slices.Sorted(maps.Keys(d.held[role])) {
				if same.Same(name, database) || !d.options.MatchesDatabase(name, database) {
					continue
				}
				for _, privilege := range d.held[role][name] {
					if namesATablePrivilege(privilege) {
						grants = append(grants, catalog.WideGrant{
							Role: role, Privilege: privilege, Grant: privilege + " ON " + quoteMySQLIdentifier(name) + ".*", Schema: database,
						})
					}
				}
			}
		}
	}
	return grants
}

// databaseAccess is the access to the rows of every table of each of
// databases that a database-level row gives each role, the subject, when the
// server applies that row to the subject's session.
//
// The server applies one database-level row of all those a session holds,
// as [mysqlgrants.Options.ApplyingRows] says, so whether a role's row reaches
// its member depends on the member's other rows. That holds for a row on the
// database's own name too: a name holding "_", such as app_db, is a pattern
// to the server, and on MySQL `app%` sorts before it and wins. So the access
// of every row is decided here, the database's own included, and
// [catalog.Database.SchemaGrantsInWideAccess] tells the security analysis to
// leave the grants on the database out.
//
// Each role in roles and each role holding a row is a subject, and its
// session holds its own rows and those of every role it inherits through
// memberships. That is the session with the subject active, which MariaDB
// looks up alone and ORs with PUBLIC's, itself a subject. On MySQL the
// session's account and any other active role add their rows to the same
// lookup, and those may shadow the row or be applied instead; the account's
// rows are no role's, and the read does not measure them.
//
// Where the order of the rows cannot be established, every row that may
// apply is reported with only the privileges all of them hold. Each entry is
// the read or the write access one row gives. A row on another name is named
// by the privileges of that row that give it, in the words a REVOKE of it
// uses; a row on the database's own name is named by no source, as the grants
// on the database are not.
func (d *databaseRows) databaseAccess(databases []string, roles []catalog.Role, memberships []catalog.RoleMembership) []catalog.WideAccess {
	if d == nil {
		return nil
	}
	subjects := slices.Collect(maps.Keys(d.held))
	for _, role := range roles {
		subjects = append(subjects, role.Name)
	}
	slices.Sort(subjects)
	access := make([]catalog.WideAccess, 0)
	for _, database := range databases {
		for _, subject := range slices.Compact(subjects) {
			access = append(access, d.subjectAccess(database, subject, inheritedRoles(subject, memberships))...)
		}
	}
	return access
}

// statesOwnRows reports whether [databaseRows.databaseAccess] decides the
// access of the rows on each database's own name. Under partial_revokes no
// row is read, since every row naming the database applies, and the grants
// on the database count as they are.
func (d *databaseRows) statesOwnRows() bool {
	return d != nil && !d.options.LiteralDatabaseNames
}

// subjectAccess is [databaseRows.databaseAccess] for one subject, whose
// session holds the rows of holders.
func (d *databaseRows) subjectAccess(database, subject string, holders []string) []catalog.WideAccess {
	var names []string
	for _, holder := range holders {
		names = slices.AppendSeq(names, maps.Keys(d.held[holder]))
	}
	applying := d.options.ApplyingRows(mysqlgrants.RoleLookup, database, names)
	credited := d.credited(applying, holders)
	same := mysqlname.NamesOf(d.options.FoldCase)
	var access []catalog.WideAccess
	for _, name := range applying {
		own := same.Same(name, database)
		for _, holder := range holders {
			privileges := slices.DeleteFunc(slices.Clone(d.held[holder][name]), func(privilege string) bool { return !credited[privilege] })
			for _, held := range accessOf(holder, quoteMySQLIdentifier(name)+".*", privileges) {
				held.Schema, held.Subject = database, subject
				if own {
					held.Source = ""
				}
				access = append(access, held)
			}
		}
	}
	return access
}

// credited is what every one of the applying rows holds, a row being every
// row of holders on that name.
func (d *databaseRows) credited(applying, holders []string) map[string]bool {
	var credited map[string]bool
	for _, name := range applying {
		held := make(map[string]bool)
		for _, holder := range holders {
			for _, privilege := range d.held[holder][name] {
				held[privilege] = true
			}
		}
		if credited == nil {
			credited = held
			continue
		}
		maps.DeleteFunc(credited, func(privilege string, _ bool) bool { return !held[privilege] })
	}
	return credited
}

// inheritedRoles is subject and every role it holds through memberships,
// directly or through a chain of them, sorted.
func inheritedRoles(subject string, memberships []catalog.RoleMembership) []string {
	seen := map[string]bool{subject: true}
	queue := []string{subject}
	for len(queue) > 0 {
		member := queue[0]
		queue = queue[1:]
		for _, membership := range memberships {
			if membership.Member == member && !seen[membership.Role] {
				seen[membership.Role] = true
				queue = append(queue, membership.Role)
			}
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// accessOf is the read and the write access that privileges, held by role on
// the objects on names, give: an entry for each that any of them gives,
// whose source names those privileges alone, in the order
// [globalTablePrivileges] lists them.
func accessOf(role, on string, privileges []string) []catalog.WideAccess {
	slices.SortStableFunc(privileges, func(a, b string) int { return tablePrivilegeRank(a) - tablePrivilegeRank(b) })
	var read, write []string
	for _, privilege := range slices.Compact(privileges) {
		reads, writes := rowaccess.Of(privilege)
		if reads {
			read = append(read, privilege)
		}
		if writes {
			write = append(write, privilege)
		}
	}
	var access []catalog.WideAccess
	if len(read) > 0 {
		access = append(access, catalog.WideAccess{Role: role, Read: true, Source: strings.Join(read, ", ") + " ON " + on})
	}
	if len(write) > 0 {
		access = append(access, catalog.WideAccess{Role: role, Write: true, Source: strings.Join(write, ", ") + " ON " + on})
	}
	return access
}

// tablePrivilegeRank is privilege's place in [globalTablePrivileges], after
// every one of them for another privilege.
func tablePrivilegeRank(privilege string) int {
	rank := slices.IndexFunc(globalTablePrivileges, func(held globalTablePrivilege) bool { return held.privilege == privilege })
	if rank < 0 {
		return len(globalTablePrivileges)
	}
	return rank
}
