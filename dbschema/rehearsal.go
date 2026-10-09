package dbschema

import (
	"context"

	"ptah.run/catalog"
)

// ReadRehearsalSchemaContext reads a private reader's schema and, where the
// reader implements catalog.RehearsalSchemaReader, its observed environment.
// Readers without that capability use their ordinary schema read. This adds
// no inferred observations and changes no execution scope. Before applying a
// baseline derived from this read, the caller must validate every statement
// against the dev database's replay guard. Errors return no partial schema.
func ReadRehearsalSchemaContext(ctx context.Context, conn *DatabaseConnection) (*catalog.Database, error) {
	reader, restore := conn.readerScopedTo(nil)
	defer restore()
	read := reader.ReadSchemaContext
	if environment, ok := reader.(catalog.RehearsalSchemaReader); ok {
		read = environment.ReadRehearsalSchemaContext
	}
	schema, err := read(ctx)
	if err != nil {
		return nil, err
	}
	return recordUnmodeledObjectKinds(schema, conn.Info().Dialect), nil
}
