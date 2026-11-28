package mysqllike

import (
	"fmt"
	"strings"

	"ptah.run/core/ptaherr"
)

// routinePrivileges are the privileges a grant on a function or a procedure
// may name here. Measured on MySQL 8.4.13 and 26.10.0 and MariaDB 11.8.9 and
// 12.3.3, `GRANT SELECT ON FUNCTION f TO r` is ERROR 1144, and
// `GRANT ALL ON PROCEDURE p TO r` writes Execute and Alter Routine to
// mysql.procs_priv. MariaDB also accepts SHOW CREATE ROUTINE there and writes
// an empty set to mysql.procs_priv, so a read cannot see it, and it is refused
// with the rest. USAGE is the row a grantee keeps when it holds only the grant
// option on the routine, as SHOW GRANTS prints it.
var routinePrivileges = map[string]bool{
	"EXECUTE": true, "ALTER ROUTINE": true, "ALL": true, "ALL PRIVILEGES": true, "USAGE": true,
}

// routineOnlyPrivileges are the privileges this family grants on a routine
// and on a database, and never on a table. Measured on MySQL 8.4.13 and
// 26.10.0 and MariaDB 11.8.9 and 12.3.3, `GRANT EXECUTE ON f1 TO r`, where f1
// is a function, is ERROR 1144: a target named without FUNCTION or PROCEDURE
// is a table.
var routineOnlyPrivileges = map[string]bool{"EXECUTE": true, "ALTER ROUTINE": true}

// grantTarget spells the object a GRANT or a REVOKE names, or refuses one this
// family cannot name, before the server sees it.
//
// A function or a procedure is named by its name alone, after FUNCTION or
// PROCEDURE: the server holds one routine of each kind under a name, and an
// argument list there is ERROR 1064. ROUTINE, PostgreSQL's word for either, is
// ERROR 1064 too and does not say which of the two to write, so it is refused,
// and so is an argument list, which would name one overload of a routine that
// has none.
//
// A privilege only a routine or a database holds is refused on a table, since
// the server answers it with ERROR 1144 after the statements before it ran.
func (r *Renderer) grantTarget(statement, objectType, objectName, arguments string, privileges []string) (string, error) {
	kind := strings.ToUpper(strings.TrimSpace(objectType))
	switch kind {
	case "ROUTINE":
		return "", fmt.Errorf("%w: %s ON ROUTINE %s: %s names a routine as FUNCTION or PROCEDURE, and ROUTINE does not say which",
			ptaherr.ErrUnsupportedFeature, statement, objectName, r.dialect)
	case "FUNCTION", "PROCEDURE":
		if strings.TrimSpace(arguments) != "" {
			return "", fmt.Errorf("%w: %s ON %s %s(%s): %s names a routine by its name alone: leave out the argument types",
				ptaherr.ErrUnsupportedFeature, statement, kind, objectName, arguments, r.dialect)
		}
		for _, privilege := range privileges {
			if normalized := strings.ToUpper(strings.TrimSpace(privilege)); !routinePrivileges[normalized] {
				return "", fmt.Errorf("%w: %s %s ON %s %s: on %s only EXECUTE and ALTER ROUTINE are modeled on a routine",
					ptaherr.ErrUnsupportedFeature, statement, normalized, kind, objectName, r.dialect)
			}
		}
		return kind + " " + grantScope(kind, objectName), nil
	case "", "TABLE", "VIEW":
		for _, privilege := range privileges {
			if normalized := strings.ToUpper(strings.TrimSpace(privilege)); routineOnlyPrivileges[normalized] {
				return "", fmt.Errorf("%w: %s %s ON %s: %s grants %s on a routine or a database, and a target named "+
					"without FUNCTION or PROCEDURE is a table: write ON FUNCTION %s or ON PROCEDURE %s",
					ptaherr.ErrUnsupportedFeature, statement, normalized, objectName, r.dialect, normalized,
					objectName, objectName)
			}
		}
	}
	return grantScope(kind, objectName), nil
}
