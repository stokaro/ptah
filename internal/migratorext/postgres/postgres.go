// Package postgres gives migration/migrator what PostgreSQL, CockroachDB and
// YugabyteDB do differently from what their descriptors state: the catalog
// queries about the migrator's own tables, written around current_schema(),
// the widening of an older revision table's version column, the owner check
// and the spelling of a migration's timeouts.
package postgres

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/migratorext"
	"ptah.run/migration/migrationfile"
)

// Services returns the PostgreSQL family's services. Spanner is in the
// family and is not served here: its PostgreSQL interface has no
// current_schema() a query can call.
func Services() migratorext.Services {
	return migratorext.Services{
		Dialects:       []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB},
		Catalog:        catalog{},
		VersionColumn:  catalog{},
		Owners:         catalog{},
		Timeouts:       timeouts{},
		ReadOnlyChecks: true,
	}
}

type catalog struct{}

// PresenceQuery inspects the schema an unqualified CREATE TABLE targets,
// current_schema(), when no schema is configured, even when an older table
// with the same name exists later in search_path, so a dry run and a real
// initialization resolve the same table.
func (catalog) PresenceQuery(table migratorext.Table) (string, []any, error) {
	if table.ConfiguredSchema == "" {
		return `SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = current_schema() AND table_name = ? AND table_type = 'BASE TABLE'`, []any{table.Name}, nil
	}
	return migratorext.InformationSchemaPresence(table.ConfiguredSchema, table.Name)
}

// ColumnExists asks information_schema in the configured schema, or in
// current_schema() when none is configured.
func (catalog) ColumnExists(ctx context.Context, q migratorext.Queryer, table migratorext.Table, column string) (bool, error) {
	query := sqlutil.Rebind(platform.Postgres, `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema = COALESCE(NULLIF(?, ''), current_schema())
  AND table_name = ? AND column_name = ?`)
	return migratorext.InformationSchemaColumnExists(ctx, q, query, table.ConfiguredSchema, table, column)
}

// WidenVersion widens a version column an older layout created narrower than
// BIGINT.
func (catalog) WidenVersion(ctx context.Context, q migratorext.Queryer, table migratorext.Table) error {
	var dataType string
	if err := q.QueryRowContext(ctx, sqlutil.Rebind(platform.Postgres, `
SELECT data_type
FROM information_schema.columns
WHERE table_schema = COALESCE(NULLIF(?, ''), current_schema())
  AND table_name = ? AND column_name = 'version'`),
		table.ConfiguredSchema,
		table.Name,
	).Scan(&dataType); err != nil {
		return fmt.Errorf("failed to inspect migrations version column: %w", err)
	}
	return migratorext.WidenVersion(ctx, q, dataType,
		fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE BIGINT", table.Qualified, table.Quote("version")))
}

// OwnerQuery reads the owner from pg_class and asks the server itself whether
// the current session can exercise the owner's privileges, because a name
// comparison would not follow a grant at all.
//
// The connected role itself, not a role it can act as. Membership of any kind
// says this session reaches the owner; it says nothing about who else does,
// and a table owned by a shared group role is modifiable by every other member
// -- which is the arrangement the refusal exists to catch, wearing a grant.
// A deployment that really wants a group-owned metadata table says so with the
// override.
//
// Every name is pg_catalog-qualified. search_path is attacker-influenced input
// wherever the role that squats a table can also create a schema: a function
// named pg_get_userbyid in an earlier schema would answer this guard, and the
// guard would report the squatter's table as this connection's own.
//
// No relkind filter: CREATE TABLE IF NOT EXISTS collides with any relation
// holding the name, so a partitioned table, a view or a foreign table under it
// is adopted the same way an ordinary one is. Narrowing to `r` would report a
// partitioned squat as an absent table and hand it the adoption this refusal
// exists to stop.
//
// It binds the configured schema alone, so an unconfigured run falls through
// to current_schema() inside the query. The schema recorded when the
// connection opened is not the one an unqualified CREATE TABLE resolves
// later: a `$user` schema created after that point takes precedence, and a
// lookup bound to the older name would inspect one schema while the DDL and
// the writes used another.
func (catalog) OwnerQuery(table migratorext.Table) (string, []any) {
	return `SELECT
  pg_catalog.pg_get_userbyid(c.relowner),
  current_user,
  pg_catalog.pg_get_userbyid(c.relowner) = current_user
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = COALESCE(NULLIF(?, ''), pg_catalog.current_schema())
  AND c.relname = ?`, []any{table.ConfiguredSchema, table.Name}
}

type timeouts struct{}

// Statements sets two GUCs. Inside a transaction they are transaction-local;
// outside one `SET LOCAL` is a warning and changes nothing -- measured on
// PostgreSQL 18.6 and CockroachDB v26.3.1 -- so a migration that runs outside
// a transaction takes the session spelling, and RESET returns the session to
// the value it started with.
func (timeouts) Statements(timeouts migrationfile.Timeouts, session bool) (setup, restore []string, err error) {
	if session {
		setup = make([]string, 0, 2)
		restore = make([]string, 0, 2)
		if timeouts.HasLockTimeout {
			setup = append(setup, "SET lock_timeout = '"+migratorext.MillisLiteral(timeouts.LockTimeout)+"'")
			restore = append(restore, "RESET lock_timeout")
		}
		if timeouts.HasStatementTimeout {
			setup = append(setup, "SET statement_timeout = '"+migratorext.MillisLiteral(timeouts.StatementTimeout)+"'")
			restore = append(restore, "RESET statement_timeout")
		}
		return setup, migratorext.Reversed(restore), nil
	}
	setup = make([]string, 0, 2)
	if timeouts.HasLockTimeout {
		setup = append(setup, "SET LOCAL lock_timeout = '"+migratorext.MillisLiteral(timeouts.LockTimeout)+"'")
	}
	if timeouts.HasStatementTimeout {
		setup = append(setup, "SET LOCAL statement_timeout = '"+migratorext.MillisLiteral(timeouts.StatementTimeout)+"'")
	}
	return setup, nil, nil
}
