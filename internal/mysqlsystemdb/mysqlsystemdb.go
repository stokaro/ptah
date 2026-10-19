// Package mysqlsystemdb answers, from a database NAME alone, which databases a
// MySQL or MariaDB server owns rather than the user, and renders the predicate
// a whole-server read and cleanup keep the user's databases with.
//
// It is a leaf on purpose. The question is about names, so it needs neither a
// connection nor a database URL. [ptah.run/internal/pgsystemschema] answers the
// same question for the PostgreSQL family.
package mysqlsystemdb

import (
	"slices"
	"strings"
)

// mysqlSystemDatabases are the databases a MySQL or MariaDB server, or a
// clustering layer beside it, owns. A whole-server run neither describes nor
// changes them, and a dev-database reset never touches them.
//
// The pinned community binary v1.3.0 leaves out information_schema, mysql,
// performance_schema and sys, measured on MySQL 8.4.11 and MariaDB 11.8.9.
// The others belong to InnoDB Cluster, NDB and TiDB. A user who created one
// of them by hand would lose it to a server-wide change, so they are left out
// too.
var mysqlSystemDatabases = []string{
	"information_schema",
	"metrics_schema",
	"mysql",
	"mysql_innodb_cluster_metadata",
	"mysql_innodb_cluster_metadata_backup",
	"mysql_innodb_cluster_metadata_bkp",
	"mysql_innodb_cluster_metadata_previous",
	"ndbinfo",
	"performance_schema",
	"sys",
}

// IsMySQLSystemDatabase reports whether name is a database the MySQL or
// MariaDB server owns. The server compares database names without case on the
// platforms it keeps them case-insensitive, and a system database is never
// meant under another case, so the comparison folds case.
func IsMySQLSystemDatabase(name string) bool {
	return slices.Contains(mysqlSystemDatabases, strings.ToLower(name))
}

// MySQLUserDatabasesPredicate returns the WHERE predicate that keeps the user
// databases over column, a schema-name column of information_schema, and
// drops the ones [IsMySQLSystemDatabase] names.
func MySQLUserDatabasesPredicate(column string) string {
	quoted := make([]string, len(mysqlSystemDatabases))
	for i, name := range mysqlSystemDatabases {
		quoted[i] = "'" + name + "'"
	}
	return "LOWER(" + column + ") NOT IN (" + strings.Join(quoted, ", ") + ")"
}
