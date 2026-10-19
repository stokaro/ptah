package mysql

import (
	"context"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/mysqlsystemdb"
)

// ScopeCatalog returns what the MySQL family's information_schema answers
// about the databases and base tables a connection reaches, where a schema is
// a database. It is offered on every dialect the writer serves.
func (w *Writer) ScopeCatalog() dbreset.ScopeCatalog {
	return mysqlScopeCatalog{}
}

// mysqlScopeCatalog reads the MySQL family's information_schema for the
// databases and base tables of a connection's scope.
type mysqlScopeCatalog struct{}

// Tables lists the base tables of schema, or of the session's DATABASE() when
// schema is empty, in name order. A view is not a table.
func (mysqlScopeCatalog) Tables(
	ctx context.Context, q dbreset.Querier, _ capability.Capabilities, schema string,
) ([]dbreset.ScopeTable, error) {
	query := `
			SELECT table_schema, table_name
			FROM information_schema.tables
			WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE())
			  AND table_type = 'BASE TABLE'
			ORDER BY table_name`
	return dbreset.QueryScopeTables(ctx, q, query, schema)
}

// Realm lists every user database of the server with the base tables it
// holds, in database and then table order. The realm of a URL that names no
// database is a whole server, and its schemas are its user databases; see
// [mysqlsystemdb.MySQLUserDatabasesPredicate]. The LEFT JOIN keeps a database
// that holds no table, which refuses a whole server as any other does.
func (mysqlScopeCatalog) Realm(
	ctx context.Context, q dbreset.Querier, _ capability.Capabilities,
) ([]dbreset.ScopeTable, error) {
	query := `
		SELECT s.SCHEMA_NAME, COALESCE(t.TABLE_NAME, '')
		FROM information_schema.SCHEMATA s
		LEFT JOIN information_schema.TABLES t
		  ON t.TABLE_SCHEMA = s.SCHEMA_NAME
		 AND t.TABLE_TYPE = 'BASE TABLE'
		WHERE ` + mysqlsystemdb.MySQLUserDatabasesPredicate("s.SCHEMA_NAME") + `
		ORDER BY s.SCHEMA_NAME, t.TABLE_NAME`
	return dbreset.QueryScopeTables(ctx, q, query)
}

// RealmSchemas lists every user database of the server, as
// [mysqlScopeCatalog.Realm] selects them. The pinned community binary v1.3.0
// describes the same set: measured on MySQL 8.4.11 and MariaDB 11.8.9,
// `schema inspect` against a URL naming no database lists every database but
// information_schema, mysql, performance_schema and sys.
func (mysqlScopeCatalog) RealmSchemas(
	ctx context.Context, q dbreset.Querier, _ capability.Capabilities,
) ([]string, error) {
	query := `
		SELECT SCHEMA_NAME
		FROM information_schema.SCHEMATA
		WHERE ` + mysqlsystemdb.MySQLUserDatabasesPredicate("SCHEMA_NAME")
	return dbreset.QueryNames(ctx, q, query)
}
