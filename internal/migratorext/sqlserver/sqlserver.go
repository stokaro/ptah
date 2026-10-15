// Package sqlserver gives migration/migrator what SQL Server does differently
// from what its descriptor states: the catalog queries about the migrator's
// own tables, the statement grammar a no-transaction run reads, and what it
// adds to proving a check assertion read-only.
package sqlserver

import (
	"context"
	"errors"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/migratorext"
)

// Services returns SQL Server's services.
func Services() migratorext.Services {
	return migratorext.Services{
		Dialects: []string{platform.SQLServer},
		Catalog:  catalog{},
		Checks:   checks{},
		// T-SQL's BEGIN and END delimit a block; BEGIN TRAN opens a
		// transaction.
		BlockBegin: true,
		// A name that begins with # is a temporary table.
		HashTemporaryNames: true,
	}
}

type catalog struct{}

// PresenceQuery asks sys.tables whether one table exists.
func (catalog) PresenceQuery(table migratorext.Table) (string, []any, error) {
	return `SELECT COUNT(*)
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name = ? AND t.name = ?`, []any{table.ConfiguredOrConnectionSchema(), table.Name}, nil
}

// ColumnExists asks INFORMATION_SCHEMA in the catalog's own casing. Under
// Turkish collations, lowercase information_schema does not resolve to
// INFORMATION_SCHEMA even when the catalog is case-insensitive.
func (catalog) ColumnExists(ctx context.Context, q migratorext.Queryer, table migratorext.Table, column string) (bool, error) {
	query := sqlutil.Rebind(platform.SQLServer, `
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`)
	return migratorext.InformationSchemaColumnExists(ctx, q, query, table.ConfiguredSchema, table, column)
}

type checks struct{}

// Validate refuses an assertion that advances a sequence, which no
// transaction rolls back.
func (checks) Validate(assertion string) error {
	if migratorext.ContainsIdentifierSequence(assertion, dialectlexer.Options(platform.SQLServer), "NEXT", "VALUE", "FOR") {
		return errors.New("check assertion must not advance a SQL Server sequence with NEXT VALUE FOR")
	}
	return nil
}
