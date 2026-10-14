// Package spanner gives migration/migrator what Spanner's PostgreSQL
// interface does differently from what its descriptor states: the catalog
// queries about the migrator's own tables, which name the schema the
// connection carries.
//
// Spanner's PostgreSQL interface does not have current_schema() anywhere a
// query can call it, so it does not take the PostgreSQL family's catalog
// queries. Measured against the Cloud Spanner emulator behind PGAdapter
// 0.55.2:
//
//	SELECT current_schema()                          public
//	SELECT ... WHERE table_schema = current_schema()  ERROR: Postgres function
//	                                                 current_schema() is not supported
//
// It asks for a schema by name instead, and answers correctly because a
// Spanner connection carries its schema (stokaro/ptah#2233).
package spanner

import (
	"context"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/migratorext"
)

// Services returns Spanner's services.
func Services() migratorext.Services {
	return migratorext.Services{
		Dialects:       []string{platform.Spanner},
		Catalog:        catalog{},
		ReadOnlyChecks: true,
	}
}

type catalog struct{}

// PresenceQuery asks information_schema in the configured schema or the
// connection's.
func (catalog) PresenceQuery(table migratorext.Table) (string, []any, error) {
	return migratorext.InformationSchemaPresence(table.ConfiguredOrConnectionSchema(), table.Name)
}

// ColumnExists asks information_schema in the configured schema or the
// connection's.
func (catalog) ColumnExists(ctx context.Context, q migratorext.Queryer, table migratorext.Table, column string) (bool, error) {
	schema := table.ConfiguredSchema
	if schema == "" {
		schema = strings.TrimSpace(table.ConnectionSchema)
	}
	return migratorext.InformationSchemaColumnExists(ctx, q,
		sqlutil.Rebind(platform.Spanner, migratorext.InformationSchemaColumnsQuery), schema, table, column)
}
