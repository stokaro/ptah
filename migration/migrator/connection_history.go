package migrator

import (
	"context"
	"fmt"
)

// refuseStrandedConnectionHistory refuses a run that would read an empty
// revision table in the connection's metadata schema while a table of that
// name stands in the schema the connection works in.
//
// A SQL Server connection keeps Ptah's tables in the schema the URL's schema
// parameter names, or else in dbo ([catalog.ServerInfo.MetadataSchema]), and
// works in the default schema of its user. A history kept in that default
// schema, through a URL or a --migrations-schema that named it, is then out of
// sight, and the run would read the database as never migrated: up would
// apply the whole directory again. Naming the schema, or
// moving the tables, is the operator's choice, so this refuses rather than
// picking one.
//
// Only a schema the caller did not name is checked. A schema the caller named
// is the answer to this question already. Initialize runs on the caller's
// migrator, before a session pins its copy to the metadata schema, so a
// schema set here is the caller's.
//
// [catalog.ServerInfo.MetadataSchema]: ptah.run/catalog.ServerInfo
func (m *Migrator) refuseStrandedConnectionHistory(ctx context.Context) error {
	kept, connected, apart := m.metadataSchemaApart()
	if !apart {
		return nil
	}
	table := m.migrationsTableName()
	inKept, err := m.tableExistsIn(ctx, kept, table)
	if err != nil {
		return fmt.Errorf("failed to look for the revision table in %q: %w", kept, err)
	}
	if inKept {
		return nil
	}
	inConnected, err := m.tableExistsIn(ctx, connected, table)
	if err != nil {
		return fmt.Errorf("failed to look for the revision table in %q: %w", connected, err)
	}
	if !inConnected {
		return nil
	}
	return fmt.Errorf(
		"found revision table %q in schema %q, the default schema of the connection's user, and none in schema %q, "+
			"where this run keeps and reads it: pass --migrations-schema %s (--revisions-schema %s in ptah-compat), "+
			"or add schema=%s to the URL, to keep using the table where it is, or move the migration tables into schema %q",
		table, connected, kept, connected, connected, connected, kept,
	)
}

// metadataSchemaApart reports the schema a run keeps its history in and the
// schema the connection works in, where the caller named no schema and the
// connection keeps its history apart from where it works
// ([catalog.ServerInfo.MetadataSchema]).
//
// [catalog.ServerInfo.MetadataSchema]: ptah.run/catalog.ServerInfo
func (m *Migrator) metadataSchemaApart() (kept, connected string, apart bool) {
	if m.conn == nil || m.atlasPlacedSchema || m.migrationsSchema != "" {
		return "", "", false
	}
	info := m.conn.Info()
	kept, connected = info.MetadataSchema, info.Schema
	return kept, connected, kept != "" && connected != "" && kept != connected
}

// MigrationsSchema is the schema the migrator reads and writes its history
// in: the schema the caller named ([Migrator.WithMigrationsTable]), or else
// the one the connection keeps its history in. It is empty on an engine that
// leaves the revision table unqualified when no schema is named, where the
// server resolves the name.
func (m *Migrator) MigrationsSchema() string {
	return m.metadataTableSchemaName()
}
