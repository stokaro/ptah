package mysql

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/mysqlsystemdb"
)

// CleanupCatalog returns what the MySQL family's catalog answers a cleanup
// plan: the stored programs and MariaDB sequences the writer's cleanup drops
// and the schema reader does not describe. It is nil on a dialect whose
// descriptor does not state that the cleanup lists them
// (descriptor.Cleanup.StoredObjects).
func (w *Writer) CleanupCatalog() dbreset.CleanupCatalog {
	if described, _ := platform.Lookup(w.dialect); !described.Cleanup.StoredObjects {
		return nil
	}
	return mysqlCleanupCatalog{}
}

// mysqlCleanupCatalog reads the MySQL family's information_schema for a
// cleanup plan.
type mysqlCleanupCatalog struct{}

// userDatabasesQuery lists the user databases of a whole server, one
// SCHEMA_NAME per row in name order. It is the list a cleanup of the server
// drops, and both cleanups, the writer's of a dev server and the plan of a
// whole server, read it here so that they cannot disagree about which
// databases are the user's.
func userDatabasesQuery() string {
	return `
		SELECT SCHEMA_NAME
		FROM information_schema.SCHEMATA
		WHERE ` + mysqlsystemdb.MySQLUserDatabasesPredicate("SCHEMA_NAME") + `
		ORDER BY SCHEMA_NAME`
}

// crossDatabaseForeignKeysQuery lists the foreign keys of a user database
// that reference a table of another database, one TABLE_SCHEMA, TABLE_NAME
// and CONSTRAINT_NAME per row. A cleanup of a whole server drops them before
// the databases: dropping a database a key of another one references fails
// with error 3730. Both cleanups read it here, as they read
// [userDatabasesQuery].
func crossDatabaseForeignKeysQuery() string {
	return `
		SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL
		  AND REFERENCED_TABLE_SCHEMA <> TABLE_SCHEMA
		  AND ` + mysqlsystemdb.MySQLUserDatabasesPredicate("TABLE_SCHEMA") + `
		ORDER BY TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME`
}

// ServerObjects lists, through q, what a cleanup of a whole server drops:
// every foreign key a user database keeps into another, first, and then each
// user database, as a schema dropped whole. A database the server owns is
// never listed; see [mysqlsystemdb.IsMySQLSystemDatabase].
//
// The pinned community binary v1.3.0 drops the databases alone, in name order,
// and stops at the first one another database's foreign key references:
// measured on MySQL 8.4.11, `schema clean` against a server holding r1 and a
// table of r4 referencing r1.t answers `drop schema named "r1": Error 3730
// (HY000): Cannot drop table 't' referenced by a foreign key constraint 'zfk'
// on table 'z'`. Dropping those keys first empties the server whatever order
// the databases come in.
func (mysqlCleanupCatalog) ServerObjects(ctx context.Context, q dbreset.Querier) ([]dbreset.CleanupObject, error) {
	databases, err := cleanupServerDatabases(ctx, q)
	if err != nil {
		return nil, err
	}
	keys, err := cleanupCrossDatabaseForeignKeys(ctx, q)
	if err != nil {
		return nil, err
	}
	objects := make([]dbreset.CleanupObject, 0, len(databases)+len(keys))
	objects = append(objects, keys...)
	for _, database := range databases {
		objects = append(objects, dbreset.CleanupObject{Kind: "schema", Schema: database, Name: database})
	}
	return objects, nil
}

// cleanupServerDatabases lists the user databases of a whole server.
func cleanupServerDatabases(ctx context.Context, q dbreset.Querier) ([]string, error) {
	rows, err := q.QueryContext(ctx, userDatabasesQuery())
	if err != nil {
		return nil, fmt.Errorf("inspect cleanup databases: %w", err)
	}
	defer rows.Close()
	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan cleanup database: %w", err)
		}
		databases = append(databases, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cleanup databases: %w", err)
	}
	return databases, nil
}

// cleanupCrossDatabaseForeignKeys lists the foreign keys of a user database
// that reference a table of another database.
func cleanupCrossDatabaseForeignKeys(ctx context.Context, q dbreset.Querier) ([]dbreset.CleanupObject, error) {
	rows, err := q.QueryContext(ctx, crossDatabaseForeignKeysQuery())
	if err != nil {
		return nil, fmt.Errorf("inspect cleanup foreign keys between databases: %w", err)
	}
	defer rows.Close()
	var keys []dbreset.CleanupObject
	for rows.Next() {
		key := dbreset.CleanupObject{Kind: "foreign_key"}
		if err := rows.Scan(&key.Schema, &key.Table, &key.Name); err != nil {
			return nil, fmt.Errorf("scan cleanup foreign key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cleanup foreign keys: %w", err)
	}
	return keys, nil
}

// Objects reports the stored programs and MariaDB sequences that the
// writer's listCleanupObjects returns but the reader never records:
// information_schema.routines, information_schema.events, and the SEQUENCE
// rows of information_schema.tables.
//
// The scope mirrors the writer's cleanupSchema: schema when the connection
// pins one, otherwise the session's current database. An object names no
// schema, as the writer's cleanup drops it from that scope.
func (mysqlCleanupCatalog) Objects(ctx context.Context, q dbreset.Querier, schema string) ([]dbreset.CleanupObject, error) {
	schema = strings.TrimSpace(schema)
	rows, err := q.QueryContext(ctx, `
		SELECT object_name, object_kind
		FROM (
			SELECT routine_name AS object_name, routine_type AS object_kind
			FROM information_schema.routines
			WHERE routine_schema = COALESCE(NULLIF(?, ''), DATABASE())

			UNION ALL

			SELECT event_name, 'EVENT'
			FROM information_schema.events
			WHERE event_schema = COALESCE(NULLIF(?, ''), DATABASE())

			UNION ALL

			SELECT table_name, 'SEQUENCE'
			FROM information_schema.tables
			WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE())
			  AND table_type = 'SEQUENCE'
		) AS cleanup_objects
		ORDER BY object_kind, object_name`,
		schema, schema, schema,
	)
	if err != nil {
		return nil, fmt.Errorf("inspect stored objects: %w", err)
	}
	defer rows.Close()

	objects := make([]dbreset.CleanupObject, 0)
	for rows.Next() {
		var name string
		var kind string
		if err := rows.Scan(&name, &kind); err != nil {
			return nil, fmt.Errorf("scan stored object: %w", err)
		}
		objectKind, ok := storedObjectKind(kind)
		if !ok {
			return nil, fmt.Errorf("inspect stored objects: unsupported object kind %q", kind)
		}
		objects = append(objects, dbreset.CleanupObject{Kind: objectKind, Name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stored objects: %w", err)
	}
	return objects, nil
}

// storedObjectKind names a kind information_schema reports by the plan's name
// for it, and reports false for one the plan has no name for.
func storedObjectKind(kind string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(kind)) {
	case "EVENT":
		return "event", true
	case "FUNCTION":
		return "function", true
	case "PROCEDURE":
		return "procedure", true
	case "SEQUENCE":
		return "sequence", true
	default:
		return "", false
	}
}

// ViewDepths answers nothing: the writer's own order drops views first.
func (mysqlCleanupCatalog) ViewDepths(context.Context, dbreset.Querier, string) ([]dbreset.ViewDepth, error) {
	return nil, nil
}

// LockRelations takes no lock: the MySQL family runs no scoped plan in one
// transaction, since its DDL commits implicitly.
func (mysqlCleanupCatalog) LockRelations(context.Context, dbreset.Executor, []dbreset.Relation) error {
	return nil
}
