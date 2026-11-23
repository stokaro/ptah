package mysql

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/mysqlname"
)

// readRoutinePrivilegesInto records what is granted on the routines of the
// databases a read describes, or that the server refused to say
// ([catalog.RoutinePrivileges]).
//
// DROP PROCEDURE and DROP FUNCTION delete the routine's rows of
// mysql.procs_priv under the default automatic_sp_privileges, measured on
// MySQL 8.4.13 and MariaDB 11.8.9, and a plan replaces a routine for a new
// body with DROP and CREATE. These rows are what it grants again after the
// CREATE. No information_schema view lists them, so the read takes them from
// mysql.procs_priv, which an account reads only with SELECT on it. A refusal
// is recorded rather than failing the read: the description does not depend
// on these rows, and only a plan that replaces a routine does.
func (r *Reader) readRoutinePrivilegesInto(ctx context.Context, account sessionAccount, schema *catalog.Database, databases []string) error {
	grants, err := r.readRoutinePrivileges(ctx, account.mariaDB, databases)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read privileges on routines: %w", err)
		}
		schema.RoutinePrivileges = catalog.RoutinePrivileges{Refused: true}
		return nil
	}
	schema.RoutinePrivileges = catalog.RoutinePrivileges{Grants: grants}
	return nil
}

func (r *Reader) readRoutinePrivileges(ctx context.Context, mariaDB bool, databases []string) ([]catalog.RoutineGrant, error) {
	var grants []catalog.RoutineGrant
	for _, database := range databases {
		inDatabase, err := r.readRoutinePrivilegesIn(ctx, database, mariaDB)
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
// MariaDB records a role with an empty host, and only a role: it records a
// user created with an empty host under the host %, and answers ERROR 1133
// to a GRANT that names a role with an empty host, measured on 11.8.9. MySQL
// 8.4.13 records a user created with an empty host under it, and a role
// there is an account with a host.
func (r *Reader) readRoutinePrivilegesIn(ctx context.Context, database string, mariaDB bool) ([]catalog.RoutineGrant, error) {
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
			Role:   mariaDB && host == "",
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
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return grants, nil
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
