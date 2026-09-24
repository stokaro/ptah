// Package devclean resets disposable development databases between migration
// replay operations.
package devclean

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/dbschema"
)

type databaseRealmCleaner interface {
	DropDatabaseRealm(context.Context) error
}

// DatabaseRealm removes every user object in the mutation realm represented by
// conn. Dialect writers with a broader realm than their configured schema
// implement DropDatabaseRealm; single-realm writers use DropAllTables.
func DatabaseRealm(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	if conn == nil {
		return fmt.Errorf("clean dev database realm: nil database connection")
	}
	writer := conn.SchemaWriter()
	if cleaner, ok := writer.(databaseRealmCleaner); ok {
		return cleaner.DropDatabaseRealm(ctx)
	}
	return writer.DropAllTables(ctx)
}

// Baseline is what a dev database held before a run, and what the run's
// cleanups leave in place: the extensions installed there.
//
// A dev database's extensions are its environment, as they are to the Atlas
// community binary, which applies a migration that uses a preinstalled
// extension's type without creating it. Removing them took that type away
// before the first migration ran, and a TimescaleDB extension created again in
// the same session answered `schema "_timescaledb_functions" does not exist`
// (stokaro/ptah#3542). An extension the run creates is not in the baseline,
// so the cleanup after the run still removes it.
//
// The zero Baseline keeps nothing, which is [DatabaseRealm].
type Baseline struct {
	extensions []string
}

// Extensions returns the extension names the baseline keeps, sorted.
func (b Baseline) Extensions() []string {
	return slices.Clone(b.extensions)
}

type extensionLister interface {
	InstalledExtensions(context.Context) ([]string, error)
}

type databaseRealmKeeper interface {
	DropDatabaseRealmKeeping(context.Context, []string) error
}

// CaptureBaseline records what [DatabaseRealmKeeping] leaves in place. It
// reads the database as it is, so it belongs before the run's first cleanup.
// A dialect without extensions captures an empty baseline.
func CaptureBaseline(ctx context.Context, conn *dbschema.DatabaseConnection) (Baseline, error) {
	if conn == nil {
		return Baseline{}, fmt.Errorf("capture dev database baseline: nil database connection")
	}
	lister, ok := conn.SchemaWriter().(extensionLister)
	if !ok {
		return Baseline{}, nil
	}
	extensions, err := lister.InstalledExtensions(ctx)
	if err != nil {
		return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
	}
	return Baseline{extensions: extensions}, nil
}

// DatabaseRealmKeeping is [DatabaseRealm] that leaves the baseline in place.
//
// A baseline that names extensions was captured through a writer that keeps
// them, so a writer that cannot is refused rather than allowed to remove
// them.
func DatabaseRealmKeeping(ctx context.Context, conn *dbschema.DatabaseConnection, baseline Baseline) error {
	if conn == nil {
		return fmt.Errorf("clean dev database realm: nil database connection")
	}
	if keeper, ok := conn.SchemaWriter().(databaseRealmKeeper); ok {
		return keeper.DropDatabaseRealmKeeping(ctx, baseline.extensions)
	}
	if len(baseline.extensions) > 0 {
		return fmt.Errorf("clean dev database realm: this writer cannot keep the extensions the database held before the run")
	}
	return DatabaseRealm(ctx, conn)
}
