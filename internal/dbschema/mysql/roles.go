package mysql

import (
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
)

// readRoles reports the roles this server holds, and nothing else that lives in
// the same table.
//
// The refusal this replaces said Ptah "cannot read or compare" a MySQL-family
// role. The catalog says otherwise, measured on MySQL 8.4: a role is a row in
// mysql.user, and two columns separate it from an account that can log in.
//
//	user        | host | account_locked | password_expired
//	app_reader  | %    | Y              | Y
//	root        | %    | N              | N
//
// A role is created locked with its password expired and no authentication
// string; a user is not. Reading without that discriminator would report every
// account on the server as a role, and the first plan would then offer to drop
// them (stokaro/ptah#1762).
//
// mysql.user is server-wide rather than per-database, so what comes back is not
// scoped by the connected schema the way a table read is. That is a property of
// the object: a role is a server principal, and there is no per-database one to
// read instead.
func (r *Reader) readRoles() ([]types.DBRole, error) {
	predicate, err := r.rolePredicate()
	if err != nil {
		return nil, err
	}
	rows, err := r.db.Query(`
		SELECT user
		FROM mysql.user
		WHERE ` + predicate + `
		  AND user NOT LIKE 'mysql.%'
		ORDER BY user`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var roles []types.DBRole
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		// Every attribute a DBRole can carry is a PostgreSQL one, and a
		// MySQL-family role carries none of them: CREATE ROLE takes a name and
		// nothing else. Reporting them false is not a default standing in for
		// an unread value -- it is what the object is.
		roles = append(roles, types.DBRole{Name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return roles, nil
}

// rolePredicate picks the discriminator this server actually has.
//
// The two engines record the same fact differently, and asking the catalog
// which columns exist is what this reader already does elsewhere rather than
// branching on a dialect name.
//
// MariaDB 11.8 has mysql.user.is_role and NO account_locked column at all --
// `SELECT account_locked FROM mysql.user` is
// `ERROR 1054: Unknown column 'account_locked'`, so a query written for MySQL
// does not degrade there, it fails. MySQL 8.4 has no is_role and marks a role
// locked with its password expired and an empty authentication string.
func (r *Reader) rolePredicate() (string, error) {
	var hasIsRole int
	err := r.db.QueryRow(`
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'mysql' AND table_name = 'user' AND column_name = 'is_role'`).Scan(&hasIsRole)
	if err != nil {
		return "", err
	}
	if hasIsRole > 0 {
		return "is_role = 'Y'", nil
	}
	return "account_locked = 'Y' AND password_expired = 'Y' AND authentication_string = ''", nil
}

// readGrants reports the privileges the roles hold on the connected database.
//
// The two scopes MySQL accepts are read from the two tables that record them:
// mysql.tables_priv for `db`.`table` and mysql.db for `db`.*. A grant on
// another database is not reported, because the description this reader
// produces is of one database and a grant elsewhere is not part of it.
func (r *Reader) readGrants(dbName string) ([]types.DBGrant, error) {
	predicate, err := r.rolePredicate()
	if err != nil {
		return nil, err
	}
	rolePredicateOn := func(prefix string) string { return qualifyPredicate(predicate, prefix) }
	query := `
		SELECT grantee, object_name, privilege, object_type
		FROM (
			SELECT tp.user AS grantee, CONCAT(tp.db, '.', tp.table_name) AS object_name,
				   UPPER(tp.table_priv) AS privilege, 'TABLE' AS object_type
			FROM mysql.tables_priv AS tp
			WHERE tp.db = ? AND tp.table_priv <> ''
			UNION ALL
			SELECT d.user AS grantee, d.db AS object_name,
				   'SELECT' AS privilege, 'SCHEMA' AS object_type
			FROM mysql.db AS d
			WHERE d.db = ? AND d.Select_priv = 'Y'
		) AS granted
		JOIN mysql.user AS u ON u.user = granted.grantee
		WHERE ` + rolePredicateOn("u.") + `
			  AND u.user NOT LIKE 'mysql.%'
		ORDER BY grantee, object_name, privilege`
	rows, err := r.db.Query(query, dbName, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var grants []types.DBGrant
	for rows.Next() {
		var role, object, privilege, objectType string
		if err := rows.Scan(&role, &object, &privilege, &objectType); err != nil {
			return nil, err
		}
		// mysql.tables_priv stores several privileges in one SET column, so one
		// row can carry `Select,Insert`. The comparator holds one privilege per
		// grant, which is what the declaration writes.
		for one := range strings.SplitSeq(privilege, ",") {
			one = strings.TrimSpace(one)
			if one == "" {
				continue
			}
			grants = append(grants, types.DBGrant{
				Role:       role,
				Privilege:  one,
				ObjectType: objectType,
				ObjectName: object,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return grants, nil
}

// qualifyPredicate prefixes every column name in a role predicate, so the same
// discriminator can be used in a join where mysql.user carries an alias.
func qualifyPredicate(predicate, prefix string) string {
	for _, column := range []string{"is_role", "account_locked", "password_expired", "authentication_string"} {
		predicate = strings.ReplaceAll(predicate, column, prefix+column)
	}
	return predicate
}
