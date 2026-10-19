package mysql

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dbreset"
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
