package mysql

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/mysqlname"
)

// readRoutinePrivilegesInto records the privileges a routine replacement or
// the rollback of a removal must grant again, or that the server refused to
// say ([catalog.RoutinePrivileges]).
//
// DROP PROCEDURE and DROP FUNCTION delete the routine's rows of
// mysql.procs_priv under the default automatic_sp_privileges, measured on
// MySQL 8.4.13 and MariaDB 11.8.9, and a plan replaces a routine for a new
// body with DROP and CREATE. These rows are what it grants again after the
// CREATE. No information_schema view lists them, so the read takes them from
// mysql.procs_priv, which an account reads only with SELECT on it. A refusal
// is recorded rather than failing the read: the description does not depend
// on these rows. A routine replacement and the rollback of a removal do.
//
// With automatic_sp_privileges off the DROP keeps the rows and the CREATE
// grants none, so there is nothing to give back and nothing to refuse, and
// mysql.procs_priv is not read.
func (r *Reader) readRoutinePrivilegesInto(ctx context.Context, account sessionAccount, schema *catalog.Database, databases []string) error {
	var automatic int64
	if err := r.db.QueryRowContext(ctx, "SELECT @@GLOBAL.automatic_sp_privileges").Scan(&automatic); err != nil {
		return fmt.Errorf("failed to read automatic_sp_privileges: %w", err)
	}
	if automatic == 0 {
		schema.RoutinePrivileges = catalog.RoutinePrivileges{}
		return nil
	}
	grants, err := r.readRoutinePrivileges(ctx, account, databases)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read privileges on routines: %w", err)
		}
		schema.RoutinePrivileges = catalog.RoutinePrivileges{Refused: true}
		return nil
	}
	schema.RoutinePrivileges = catalog.RoutinePrivileges{Grants: existingRoutinePrivileges(grants, schema)}
	return nil
}

// existingRoutinePrivileges drops rows whose routine is absent from the
// catalog. A DROP with automatic_sp_privileges disabled can leave them in
// mysql.procs_priv; restoring them on a later CREATE would grant stale access.
// Names of routines compare without case, but kind and database still select
// separate objects. Both database spellings come from this same read.
func existingRoutinePrivileges(grants []catalog.RoutineGrant, schema *catalog.Database) []catalog.RoutineGrant {
	return slices.DeleteFunc(grants, func(grant catalog.RoutineGrant) bool {
		return !slices.ContainsFunc(schema.Functions, func(function catalog.Function) bool {
			return cmp.Or(function.Schema, schema.CurrentDatabase) == grant.Schema &&
				strings.EqualFold(function.Name, grant.Name) && function.Kind == routineKind(grant.Type)
		})
	})
}

func (r *Reader) readRoutinePrivileges(ctx context.Context, account sessionAccount, databases []string) ([]catalog.RoutineGrant, error) {
	var grants []catalog.RoutineGrant
	for _, database := range databases {
		inDatabase, err := r.readRoutinePrivilegesIn(ctx, account, database)
		if err != nil {
			return nil, err
		}
		grants = append(grants, inDatabase...)
	}
	return grants, nil
}

// readRoutinePrivilegesIn reads the rows of mysql.procs_priv for the routines
// of one database. The database is reported as the read spells it, as
// [Reader.readGrants] reports it.
//
// The CREATE that replaces a routine grants the account running it EXECUTE
// and ALTER ROUTINE on the routine, unless it holds both already, so a row of
// the reading account that holds exactly those, without the grant option, is
// the one the CREATE writes back and is left out ([sessionAccount.isCreator]).
// Each other row records whether the reading account may grant it again
// ([sessionAccount.mayGrant]).
//
// MariaDB records a role with an empty host, and only a role: it records a
// user created with an empty host under the host %, and answers ERROR 1133
// to a GRANT that names a role with an empty host, measured on 11.8.9. MySQL
// 8.4.13 records a user created with an empty host under it, and a role
// there is an account with a host.
func (r *Reader) readRoutinePrivilegesIn(ctx context.Context, account sessionAccount, database string) ([]catalog.RoutineGrant, error) {
	rows, err := r.db.QueryContext(ctx, routinePrivilegeQuery(), nameBinds(database, 1)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var grants []catalog.RoutineGrant
	for rows.Next() {
		var user, host, name, routineType, privileges string
		if err := rows.Scan(&user, &host, &name, &routineType, &privileges); err != nil {
			return nil, err
		}
		grant := catalog.RoutineGrant{
			User:   user,
			Host:   host,
			Role:   account.mariaDB && host == "",
			Schema: database,
			Name:   name,
			Type:   strings.ToUpper(strings.TrimSpace(routineType)),
		}
		for privilege := range strings.SplitSeq(privileges, ",") {
			privilege = strings.ToUpper(strings.TrimSpace(privilege))
			if privilege == "GRANT" {
				grant.WithOption = true
			} else if privilege != "" {
				grant.Privileges = append(grant.Privileges, privilege)
			}
		}
		if account.isCreator(grant) {
			continue
		}
		grant.ReaderMayGrant = account.mayGrant(grant)
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return grants, nil
}

// isCreator reports whether grant is the row the CREATE of the routine grants
// the account: its own, holding EXECUTE and ALTER ROUTINE and not the grant
// option. Measured on MySQL 8.4.13 and MariaDB 11.8.9 with
// automatic_sp_privileges on, an account holding CREATE ROUTINE on the
// database alone gets that row from each CREATE PROCEDURE it runs.
//
// On MySQL a session with a role active gets it only from the first CREATE of
// the routine: after a DROP of the routine, the CREATE writes no row, from that
// session or a later one, with or without a role that holds anything on the
// database. Measured on 8.4.13; MariaDB 11.8.9 writes the row back. Such a
// session keeps the row, so a plan grants it back or refuses.
func (a sessionAccount) isCreator(grant catalog.RoutineGrant) bool {
	at := strings.LastIndex(a.name, "@")
	return at >= 0 && !a.activeRole && !grant.Role && grant.User == a.name[:at] && grant.Host == a.name[at+1:] && !grant.WithOption &&
		len(grant.Privileges) == 2 &&
		slices.Contains(grant.Privileges, "EXECUTE") && slices.Contains(grant.Privileges, "ALTER ROUTINE")
}

// mayGrant reports whether the account may grant grant again after the DROP
// of its routine: SHOW GRANTS gives it the grant option and each privilege of
// the row on the routine's database, or on every database. A row SHOW GRANTS
// prints on the routine itself is not read, which is right here, because the
// DROP takes it.
func (a sessionAccount) mayGrant(grant catalog.RoutineGrant) bool {
	return a.grants.OnDatabase(grant.Schema).Grantable(grant.Privileges...)
}

// routinePrivilegeQuery projects the rows of mysql.procs_priv in one
// database. Db compares as bytes there, so the column is compared through a
// collation that ignores case, and [mysqlname.SameDatabase] keeps the bytes
// where the server keeps names apart by case.
func routinePrivilegeQuery() string {
	const database = "CONVERT(pp.Db USING utf8mb4) COLLATE utf8mb4_general_ci"
	return `
		SELECT pp.User, pp.Host, pp.Routine_name, pp.Routine_type, pp.Proc_priv
		FROM mysql.procs_priv AS pp
		WHERE ` + mysqlname.SameDatabase(database, "?") + `
		ORDER BY pp.Routine_type, pp.Routine_name, pp.User, pp.Host`
}
