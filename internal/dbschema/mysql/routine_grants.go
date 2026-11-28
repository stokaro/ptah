package mysql

import (
	"cmp"
	"context"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/mysqlname"
)

// readRoutineGrantsIn reads the grants on the functions and procedures of
// databases that a schema declares: those of the grantees [readGrantee]
// names, as [Reader.readGrants] reads the grants on tables. No
// information_schema view lists a privilege on a routine, on MySQL or on
// MariaDB, so the rows come from mysql.procs_priv.
//
// A grant names its routine as the GRANT that wrote the row spelled it, and a
// routine name compares without case: measured on MySQL 8.4.13 and 26.10.0 and
// MariaDB 11.8.9 and 12.3.3, `GRANT EXECUTE ON FUNCTION F1 TO r` on the
// function f1 writes F1. The grant is reported on the routine as schema spells
// it, and a row whose routine schema does not hold is left out: with
// automatic_sp_privileges off, a DROP keeps the rows of the routine it drops.
//
// A row holding only the grant option reads as USAGE with the option, as
// SHOW GRANTS prints it.
func (r *Reader) readRoutineGrantsIn(
	ctx context.Context,
	schema *catalog.Database,
	databases []string,
	source grantSource,
) ([]catalog.Grant, error) {
	var grants []catalog.Grant
	for _, database := range databases {
		inDatabase, err := r.readRoutineGrants(ctx, schema, database, source)
		if err != nil {
			return nil, err
		}
		grants = append(grants, inDatabase...)
	}
	return grants, nil
}

func (r *Reader) readRoutineGrants(ctx context.Context, schema *catalog.Database, database string, source grantSource) ([]catalog.Grant, error) {
	rows, err := r.db.QueryContext(ctx, routineGrantQuery(readGrantee(source)), nameBinds(database, 1)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var grants []catalog.Grant
	for rows.Next() {
		var user, host, name, routineType, privileges string
		if err := rows.Scan(&user, &host, &name, &routineType, &privileges); err != nil {
			return nil, err
		}
		kind := strings.ToUpper(strings.TrimSpace(routineType))
		spelled, found := routineSpelling(schema, database, name, kind)
		if !found {
			continue
		}
		grants = append(grants, routineGrantRows(user, database, spelled, kind, privileges)...)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return grants, nil
}

// routineSpelling is the name schema holds the routine of kind under in
// database, and whether it holds one. Both database spellings come from the
// same read.
func routineSpelling(schema *catalog.Database, database, name, kind string) (string, bool) {
	for _, function := range schema.Functions {
		if cmp.Or(function.Schema, schema.CurrentDatabase) == database &&
			strings.EqualFold(function.Name, name) && function.Kind == routineKind(kind) {
			return function.Name, true
		}
	}
	return "", false
}

// routineGrantRows is one grant for each privilege of a row of
// mysql.procs_priv, whose Proc_priv is a set such as `Execute,Grant`.
func routineGrantRows(grantee, database, routine, kind, privileges string) []catalog.Grant {
	var held []string
	withOption := false
	for privilege := range strings.SplitSeq(privileges, ",") {
		switch privilege = strings.ToUpper(strings.TrimSpace(privilege)); privilege {
		case "":
		case "GRANT":
			withOption = true
		default:
			held = append(held, privilege)
		}
	}
	if len(held) == 0 && withOption {
		held = []string{"USAGE"}
	}
	grants := make([]catalog.Grant, 0, len(held))
	for _, privilege := range held {
		grants = append(grants, catalog.Grant{
			Role:       grantee,
			Privilege:  privilege,
			ObjectType: kind,
			Schema:     database,
			ObjectName: routine,
			WithOption: withOption,
		})
	}
	return grants
}

// routineGrantQuery projects the rows of mysql.procs_priv in one database for
// the grantees rolePredicate names, in the columns
// [Reader.readRoutinePrivilegesIn] reads, with the grantee's account from
// mysql.user. Db compares as bytes there, so the column is compared through a
// collation that ignores case, and [mysqlname.SameDatabase] keeps the bytes
// where the server keeps names apart by case.
func routineGrantQuery(rolePredicate string) string {
	const database = "CONVERT(pp.Db USING utf8mb4) COLLATE utf8mb4_general_ci"
	return `
		SELECT u.user, u.host, pp.Routine_name, pp.Routine_type, pp.Proc_priv
		FROM mysql.procs_priv AS pp
		JOIN mysql.user AS u ON u.user = pp.User AND u.host = pp.Host
		WHERE ` + mysqlname.SameDatabase(database, "?") + `
			  AND ` + rolePredicate + `
			  AND u.user NOT LIKE 'mysql.%'
		ORDER BY u.user, pp.Routine_type, pp.Routine_name`
}
