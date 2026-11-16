package migrator

import (
	"context"
	"fmt"
	"path/filepath"

	"ptah.run/internal/mysqldefiner"
)

// refuseDefinersTheAccountCannotName refuses a run in direction whose files
// name a definer the connected account may not name, before the first file
// of the run starts.
//
// A MySQL or MariaDB statement that writes DEFINER= for another account fails
// with ERROR 1227 without the privilege the server checks for it, and the
// family runs DDL outside a transaction: the files before the failing one
// stay applied and the failing one is left failed with part of it applied.
// The definers a file names do not depend on what the files before it did,
// and neither does what the account holds, so the question is asked of every
// file at once. [mysqldefiner.Check] refuses nothing it cannot decide, and the
// server then answers as it would without this check.
func (m *Migrator) refuseDefinersTheAccountCannotName(
	ctx context.Context,
	migrations []*Migration,
	direction MigrationDirection,
) error {
	for _, migration := range migrations {
		body := migrationSQLForDirection(migration, direction)
		if body == "" {
			continue
		}
		statements := splitSQLStatementsForConnection(m.conn, body)
		if err := mysqldefiner.Check(ctx, m.conn, migrationSourceName(migration, direction), statements); err != nil {
			return err
		}
	}
	return nil
}

// migrationSourceName names the file of direction a refusal points at: the
// file's base name where the migration was read from one, and its version
// otherwise.
func migrationSourceName(migration *Migration, direction MigrationDirection) string {
	path := migration.upSourcePath
	if direction == MigrationDirectionDown {
		path = migration.downSourcePath
	}
	if path == "" {
		return fmt.Sprintf("migration %d", migration.Version)
	}
	return filepath.Base(path)
}
