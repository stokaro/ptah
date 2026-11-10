package postgres

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/platform"
)

// readUnmodeledObjects names PostgreSQL objects the schema comparison cannot
// describe. This reader also serves other PostgreSQL-wire engines; their
// catalogs do not promise these tables or columns, so they take no such query.
func (r *Reader) readUnmodeledObjects(ctx context.Context, database *catalog.Database) error {
	if r.dialect != platform.Postgres {
		return nil
	}
	for _, schemaName := range r.schemasToRead() {
		if err := r.readUnmodeledInSchema(ctx, database, schemaName); err != nil {
			return err
		}
	}
	return r.readUnmodeledInDatabase(ctx, database)
}

func (r *Reader) readUnmodeledInSchema(ctx context.Context, database *catalog.Database, schemaName string) error {
	const query = `
		SELECT kind, name FROM (
			SELECT 'aggregate' AS kind,
			       p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' AS name,
			       p.pronamespace AS namespace
			FROM pg_catalog.pg_proc p WHERE p.prokind = 'a'
			UNION ALL
			SELECT 'collation', c.collname, c.collnamespace
			FROM pg_catalog.pg_collation c
			UNION ALL
			SELECT 'foreign table', c.relname, c.relnamespace
			FROM pg_catalog.pg_class c WHERE c.relkind = 'f'
		) objects
		JOIN pg_catalog.pg_namespace n ON n.oid = objects.namespace
		WHERE n.nspname = $1
		ORDER BY kind, name`
	rows, err := r.db.QueryContext(ctx, query, schemaName)
	if err != nil {
		return fmt.Errorf("read unmodeled PostgreSQL objects in schema %q: %w", schemaName, err)
	}
	defer rows.Close()
	for rows.Next() {
		object := catalog.UnmodeledObject{Schema: r.outputSchema(schemaName), Reason: "Ptah does not compare this object kind"}
		if err := rows.Scan(&object.Kind, &object.Name); err != nil {
			return fmt.Errorf("read unmodeled PostgreSQL objects in schema %q: %w", schemaName, err)
		}
		database.UnmodeledObjects = append(database.UnmodeledObjects, object)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read unmodeled PostgreSQL objects in schema %q: %w", schemaName, err)
	}
	return nil
}

func (r *Reader) readUnmodeledInDatabase(ctx context.Context, database *catalog.Database) error {
	const query = `
		SELECT 'event trigger' AS kind, evtname AS name FROM pg_catalog.pg_event_trigger
		UNION ALL
		SELECT 'publication', pubname FROM pg_catalog.pg_publication
		ORDER BY kind, name`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("read unmodeled PostgreSQL database objects: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		object := catalog.UnmodeledObject{Reason: "Ptah does not compare this object kind"}
		if err := rows.Scan(&object.Kind, &object.Name); err != nil {
			return fmt.Errorf("read unmodeled PostgreSQL database objects: %w", err)
		}
		database.UnmodeledObjects = append(database.UnmodeledObjects, object)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read unmodeled PostgreSQL database objects: %w", err)
	}
	return nil
}
