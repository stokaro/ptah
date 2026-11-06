package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/mysqlgrants"
	"ptah.run/internal/readscope"
)

// The privileges that show each kind of object, at the scope that shows all of
// them. information_schema lists only what an account holds a privilege on and a
// comparison reads a missing row as a missing object, so a read that cannot
// prove it saw a kind refuses. Each list was measured one privilege at a time
// on MySQL 8.4 and 26.10 and on MariaDB 11.8 and 12.3, against a database holding
// one object of each kind; a privilege left out of a list hid that kind.
var (
	// listingPrivileges show every table and view of a database with its
	// columns. CREATE, DROP, ALTER, INDEX, DELETE, TRIGGER, SHOW VIEW and CREATE
	// VIEW list the tables and hide their columns.
	listingPrivileges = []string{"SELECT", "INSERT", "UPDATE", "REFERENCES"}
	// mariaDBConstraintPrivileges show a table's rows in TABLE_CONSTRAINTS on
	// MariaDB: a privilege beyond SELECT, on the table or above it. MySQL shows
	// the constraints of every table it lists.
	mariaDBConstraintPrivileges = []string{
		"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REFERENCES", "INDEX", "ALTER",
		"CREATE VIEW", "SHOW VIEW", "TRIGGER", "DELETE HISTORY", "GRANT OPTION",
	}
	// routineListingPrivileges show every routine of a database, at the database
	// level or globally. A row whose body stays hidden is refused on its own.
	routineListingPrivileges = []string{"EXECUTE", "ALTER ROUTINE", "CREATE ROUTINE", "SHOW CREATE ROUTINE"}
)

// sessionAccount is who a read runs as, and what that account may see.
type sessionAccount struct {
	name    string
	mariaDB bool
	grants  mysqlgrants.Grants
}

// readSessionAccount asks the server who the session is and what it holds.
//
// A failure here fails the read: without the answer nothing says the read is
// complete, and SHOW GRANTS about oneself needs no privilege.
func (r *Reader) readSessionAccount(ctx context.Context) (sessionAccount, error) {
	var account sessionAccount
	var version string
	if err := r.db.QueryRowContext(ctx, "SELECT CURRENT_USER(), VERSION()").Scan(&account.name, &version); err != nil {
		return sessionAccount{}, fmt.Errorf("failed to read the connecting account: %w", err)
	}
	account.mariaDB = capability.BannerPlatform(version) == platform.MariaDB

	lines, err := dbreset.QueryNames(ctx, r.db, "SHOW GRANTS")
	if err != nil {
		return sessionAccount{}, fmt.Errorf("failed to read the connecting account's grants: %w", err)
	}
	options, err := r.readGrantOptions(ctx)
	if err != nil {
		return sessionAccount{}, fmt.Errorf("failed to read the settings grants depend on: %w", err)
	}
	// MariaDB prints a role's rows under the role's name.
	options.MergedRoles = !account.mariaDB
	account.grants = mysqlgrants.Parse(lines, options)
	return account, nil
}

func (r *Reader) readGrantOptions(ctx context.Context) (mysqlgrants.Options, error) {
	rows, err := r.db.QueryContext(ctx, `SHOW VARIABLES WHERE Variable_name IN ('partial_revokes', 'lower_case_table_names')`)
	if err != nil {
		return mysqlgrants.Options{}, err
	}
	defer rows.Close()
	var options mysqlgrants.Options
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return mysqlgrants.Options{}, err
		}
		switch strings.ToLower(name) {
		case "partial_revokes":
			options.LiteralDatabaseNames = strings.EqualFold(value, "ON")
		case "lower_case_table_names":
			options.FoldCase = value != "0"
		}
	}
	return options, rows.Err()
}

// missingPrivilege is one line of a refusal.
type missingPrivilege struct {
	objects   string
	privilege string
}

// refuseUnseenObjects refuses a read of database that left out what the run
// depends on. schema is what the read found and hiddenRoutines the routines it
// listed without a body. The privileges come first because a hidden object has
// no row to find, and the rows come after because an object the account may list
// and not read has a row with nothing in it.
func (r *Reader) refuseUnseenObjects(
	ctx context.Context,
	a sessionAccount,
	database string,
	schema *catalog.Database,
	hiddenRoutines []string,
) error {
	missing := a.missingPrivileges(ctx, database, schema, hiddenRoutines)
	if len(missing) == 0 {
		return nil
	}
	routinePrivilege, err := r.routinePrivilegeName(ctx, a, database)
	if err != nil {
		return err
	}
	var message strings.Builder
	fmt.Fprintf(&message,
		"the account %s cannot see everything in database %q: information_schema lists only what an account holds "+
			"a privilege on, and a comparison reads a hidden object as absent. The account needs:",
		a.name, database,
	)
	for _, line := range missing {
		privilege := line.privilege
		if privilege == "" {
			privilege = routinePrivilege
		}
		fmt.Fprintf(&message, "\n  - %s: %s", line.objects, privilege)
	}
	return errors.New(message.String())
}

// missingPrivileges is what the account lacks to see what the run depends on in
// database, one line per kind. An empty privilege names the routine privilege,
// which only the server can spell.
func (a sessionAccount) missingPrivileges(
	ctx context.Context,
	database string,
	schema *catalog.Database,
	hiddenRoutines []string,
) []missingPrivilege {
	onDatabase := a.grants.OnDatabase(database)
	wholeDatabase := "`" + strings.ReplaceAll(database, "`", "``") + "`.*"
	listed := a.listsEveryTable(database)
	authored := readscope.IsAuthored(ctx)

	var missing []missingPrivilege
	if !listed && needsListing(ctx) {
		missing = append(missing, missingPrivilege{"tables and views", "SELECT on " + wholeDatabase})
	}
	if readscope.Needs(ctx, readscope.Views) {
		emptyViews := viewsWithoutDefinition(schema.Views)
		switch {
		case len(emptyViews) > 0:
			missing = append(missing, missingPrivilege{
				"the definitions of views " + strings.Join(emptyViews, ", "),
				"SELECT and SHOW VIEW on " + wholeDatabase,
			})
		case !authored && !listed && !onDatabase.Holds("SHOW VIEW"):
			missing = append(missing, missingPrivilege{
				"the definitions of views",
				"SELECT and SHOW VIEW on " + wholeDatabase,
			})
		}
	}
	if a.mariaDB && !authored && readscope.Needs(ctx, readscope.Constraints) &&
		!a.holdsOnEveryTable(database, schema.Tables, mariaDBConstraintPrivileges) {
		missing = append(missing, missingPrivilege{
			"primary keys, unique keys, foreign keys and CHECK constraints",
			"a privilege beyond SELECT, such as SHOW VIEW, on " + wholeDatabase,
		})
	}
	if readscope.Needs(ctx, readscope.Triggers) &&
		(slices.ContainsFunc(schema.Triggers, triggerHasNoBody) ||
			(!authored && !a.holdsOnEveryTable(database, schema.Tables, []string{"TRIGGER"}))) {
		missing = append(missing, missingPrivilege{"triggers", "TRIGGER on " + wholeDatabase})
	}
	if readscope.Needs(ctx, readscope.Routines) {
		switch {
		case len(hiddenRoutines) > 0:
			missing = append(missing, missingPrivilege{
				"the bodies of routines " + strings.Join(hiddenRoutines, ", ") + ", unless the account is their definer",
				"",
			})
		case !authored && !a.holdsRoutineListing(database):
			missing = append(missing, missingPrivilege{"procedures and functions", ""})
		}
	}
	return missing
}

// listsEveryTable reports whether information_schema lists every table and view
// of database to the account with its columns. A grant on one table never does:
// it says nothing about the tables it does not name.
func (a sessionAccount) listsEveryTable(database string) bool {
	return a.grants.OnDatabase(database).Holds(listingPrivileges...)
}

// needsListing reports whether the run depends on the list of tables and views.
func needsListing(ctx context.Context) bool {
	return readscope.Needs(ctx, readscope.Tables) || readscope.Needs(ctx, readscope.Views)
}

// holdsOnEveryTable reports whether the account holds one of privileges on every
// table of database, whichever level each grant is at. tables is what the read
// found, so where it could not list them all only a grant above them can say.
func (a sessionAccount) holdsOnEveryTable(database string, tables []catalog.Table, privileges []string) bool {
	if !a.listsEveryTable(database) {
		return a.grants.OnDatabase(database).Holds(privileges...)
	}
	return !slices.ContainsFunc(tables, func(table catalog.Table) bool {
		return !a.grants.OnTable(database, table.Name).Holds(privileges...)
	})
}

// holdsRoutineListing reports whether information_schema.ROUTINES lists every
// routine of database to the account. On top of [routineListingPrivileges], SELECT
// held globally shows them with their bodies, as SHOW_ROUTINE does on MySQL and
// SELECT on mysql.proc does on MariaDB, where that is the table routines live in.
func (a sessionAccount) holdsRoutineListing(database string) bool {
	global := a.grants.Global(database)
	return a.grants.OnDatabase(database).Holds(routineListingPrivileges...) ||
		global.Holds("SELECT", "SHOW_ROUTINE") ||
		(a.mariaDB && a.grants.OnTable("mysql", "proc").Holds("SELECT"))
}

// routinePrivilegeName is what a refusal asks for to read every routine: the
// privilege this server has for it, which SHOW PRIVILEGES lists under a different
// name on each engine and not at all on an older line of either.
func (r *Reader) routinePrivilegeName(ctx context.Context, a sessionAccount, database string) (string, error) {
	rows, err := r.db.QueryContext(ctx, "SHOW PRIVILEGES")
	if err != nil {
		return "", fmt.Errorf("failed to read the privileges this server has: %w", err)
	}
	defer rows.Close()
	var known []string
	for rows.Next() {
		var privilege, scope, comment sql.NullString
		if err := rows.Scan(&privilege, &scope, &comment); err != nil {
			return "", fmt.Errorf("failed to read the privileges this server has: %w", err)
		}
		known = append(known, strings.ToUpper(privilege.String))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("failed to read the privileges this server has: %w", err)
	}
	switch {
	case a.mariaDB && slices.Contains(known, "SHOW CREATE ROUTINE"):
		return "SHOW CREATE ROUTINE on `" + strings.ReplaceAll(database, "`", "``") + "`.*", nil
	case a.mariaDB:
		return "SELECT on mysql.proc", nil
	case slices.Contains(known, "SHOW_ROUTINE"):
		return "SHOW_ROUTINE on *.*", nil
	default:
		return "SELECT on *.*", nil
	}
}

// viewsWithoutDefinition names the views the read listed with an empty
// definition, which is how both engines report a view to an account without SHOW
// VIEW and SELECT on it. A view always has a definition, so the row means the
// account may not read it.
func viewsWithoutDefinition(views []catalog.View) []string {
	var names []string
	for _, view := range views {
		if view.Body == "" {
			names = append(names, view.Name)
		}
	}
	return names
}

// triggerHasNoBody reports a trigger listed with an empty body, which is how
// MariaDB reports a trigger to an account holding INSERT, UPDATE or DELETE on its
// table and not TRIGGER.
func triggerHasNoBody(trigger catalog.Trigger) bool {
	return trigger.Body == ""
}
