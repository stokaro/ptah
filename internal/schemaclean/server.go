package schemaclean

import (
	"context"
	"fmt"

	"ptah.run/dbschema"
	"ptah.run/internal/systemschema"
)

// ObjectTypeSchema is a schema a cleanup drops whole: on MySQL and MariaDB, a
// user database of a whole server, which `DROP DATABASE` drops with every
// object in it.
const ObjectTypeSchema = "schema"

// IsServer reports whether conn is a whole MySQL or MariaDB server, whose
// cleanup drops every user database rather than the objects of one; see
// [catalog.ServerInfo.WholeServer].
func IsServer(conn *dbschema.DatabaseConnection) bool {
	return conn.Info().WholeServer
}

// inspectServer plans the cleanup of a whole MySQL or MariaDB server: every
// user database, dropped whole, after every foreign key one database keeps
// into another.
//
// The pinned community binary v1.3.0 drops the databases alone, in name order,
// and stops at the first one another database's foreign key references:
// measured on MySQL 8.4.11, `schema clean` against a server holding r1 and a
// table of r4 referencing r1.t answers `drop schema named "r1": Error 3730
// (HY000): Cannot drop table 't' referenced by a foreign key constraint 'zfk'
// on table 'z'`. Dropping those keys first empties the server whatever order
// the databases come in. A database the server owns is never listed; see
// [systemschema.IsMySQLSystemDatabase].
func inspectServer(ctx context.Context, conn *dbschema.DatabaseConnection) ([]Object, error) {
	databases, err := serverDatabases(ctx, conn)
	if err != nil {
		return nil, err
	}
	keys, err := crossDatabaseForeignKeys(ctx, conn)
	if err != nil {
		return nil, err
	}
	objects := make([]Object, 0, len(databases)+len(keys))
	objects = append(objects, keys...)
	for _, database := range databases {
		objects = append(objects, Object{Type: ObjectTypeSchema, Schema: database, Name: database})
	}
	return objects, nil
}

// serverDatabases lists the user databases of a whole server.
func serverDatabases(ctx context.Context, conn *dbschema.DatabaseConnection) ([]string, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT SCHEMA_NAME
		FROM information_schema.SCHEMATA
		WHERE `+systemschema.MySQLUserDatabasesPredicate("SCHEMA_NAME")+`
		ORDER BY SCHEMA_NAME`)
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

// crossDatabaseForeignKeys lists the foreign keys of a user database that
// reference a table of another database.
func crossDatabaseForeignKeys(ctx context.Context, conn *dbschema.DatabaseConnection) ([]Object, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL
		  AND REFERENCED_TABLE_SCHEMA <> TABLE_SCHEMA
		  AND `+systemschema.MySQLUserDatabasesPredicate("TABLE_SCHEMA")+`
		ORDER BY TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME`)
	if err != nil {
		return nil, fmt.Errorf("inspect cleanup foreign keys between databases: %w", err)
	}
	defer rows.Close()
	var keys []Object
	for rows.Next() {
		var key Object
		if err := rows.Scan(&key.Schema, &key.Table, &key.Name); err != nil {
			return nil, fmt.Errorf("scan cleanup foreign key: %w", err)
		}
		key.Type = ObjectTypeForeignKey
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cleanup foreign keys: %w", err)
	}
	return keys, nil
}
