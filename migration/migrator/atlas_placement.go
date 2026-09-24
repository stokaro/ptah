package migrator

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
)

// refuseStrandedAtlasHistory refuses an Atlas-layout run that would read an
// empty revision table in Atlas's schema while a table of that name stands in
// the connection's schema.
//
// placeAtlasRevisionTable sends a run with no named schema to
// atlas_schema_revisions, where Atlas reads. A table written by a run that kept
// it in the connection's schema, or by Atlas through a URL that pinned
// search_path, is then out of sight, and the run would read the database as
// never migrated: `up` would replay the whole directory against a schema that
// already has it. Naming the schema, or moving the table, is the operator's
// choice, so this refuses rather than picking one.
//
// Only a placed schema is checked. A schema the caller named is the answer to
// this question already.
func (m *Migrator) refuseStrandedAtlasHistory(ctx context.Context) error {
	if !m.atlasPlacedSchema || !platform.IsPostgresFamily(m.connectionDialect()) {
		return nil
	}
	connected := m.connectionSchemaName()
	if connected == "" || connected == m.migrationsSchema {
		return nil
	}
	rows, err := m.conn.QueryContext(ctx, sqlutil.Rebind(m.connectionDialect(), `
		SELECT n.nspname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = ?
		  AND c.relkind IN ('r', 'p')
		  AND n.nspname IN (?, ?)`),
		m.migrationsTable, m.migrationsSchema, connected,
	)
	if err != nil {
		return fmt.Errorf("failed to look for the revision table in %q and %q: %w", m.migrationsSchema, connected, err)
	}
	defer func() { _ = rows.Close() }()
	found := make(map[string]bool, 2)
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return fmt.Errorf("failed to look for the revision table: %w", err)
		}
		found[schema] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to look for the revision table: %w", err)
	}
	if found[m.migrationsSchema] || !found[connected] {
		return nil
	}
	return fmt.Errorf(
		"found revision table %q in schema %q, and none in schema %q, where Atlas keeps it for a URL "+
			"that pins no search_path and where this run reads it: pass --migrations-schema %s to keep "+
			"using the table where it is, or move it into schema %q",
		m.migrationsTable, connected, m.migrationsSchema, connected, m.migrationsSchema,
	)
}
