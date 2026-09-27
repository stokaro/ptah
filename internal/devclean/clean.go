// Package devclean resets disposable development databases between migration
// replay operations.
package devclean

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"ptah.run/dbschema"
	"ptah.run/internal/migrateclean"
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
	// realm records whether the claim judged the dev database's whole realm
	// or only its connected schema; see [Reset].
	realm bool
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

// Claim takes a dev database for one run. It refuses one that is not clean,
// see [EnsureClean], and records what [DatabaseRealmKeeping] leaves in place.
// It reads the database as it is, so it belongs before the run's first
// cleanup, and before the caller registers the cleanup that runs on its way
// out: registered first, that cleanup would run on the refusal too. A dialect
// without extensions captures an empty baseline.
func Claim(ctx context.Context, conn *dbschema.DatabaseConnection) (Baseline, error) {
	if conn == nil {
		return Baseline{}, fmt.Errorf("capture dev database baseline: nil database connection")
	}
	if err := EnsureClean(ctx, conn); err != nil {
		return Baseline{}, err
	}
	realm := migrateclean.RealmScoped(conn)
	lister, ok := conn.SchemaWriter().(extensionLister)
	if !ok {
		return Baseline{realm: realm}, nil
	}
	extensions, err := lister.InstalledExtensions(ctx)
	if err != nil {
		return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
	}
	return Baseline{extensions: extensions, realm: realm}, nil
}

// EnsureClean refuses a dev database that holds objects no run of Ptah put
// there, before anything resets it.
//
// A reset drops every user object in the realm, and a dev database the
// operator pointed at by mistake held their data. The pinned community binary
// refuses such a database, and so does every caller of this function: the
// predicate and the object the refusal names are [migrateclean.DevRefusal]'s.
// A database that answers nil can still hold objects that predicate does not
// count, such as a view or a function; the reset removes them.
//
// A dialect that predicate does not cover is not checked.
func EnsureClean(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	err := migrateclean.DevRefusal(ctx, conn)
	if _, ok := errors.AsType[*migrateclean.NotCleanError](err); ok {
		return fmt.Errorf("%w; Ptah resets a dev database before and after it uses one, so point --dev-url at an empty database", err)
	}
	if err != nil {
		return fmt.Errorf("check that the dev database is clean: %w", err)
	}
	return nil
}

// Reset empties a dev database the caller claimed, over the scope the claim
// judged it clean in: the whole realm when the URL pinned no schema, and the
// connected schema when it pinned one. A rehearsal resets its dev database
// this way before and after it runs, so it removes what it created -- a schema
// or an extension schema included -- and leaves a schema outside the pinned
// one alone, since the claim did not look there.
//
// A migration replay resets the engine's realm instead, whatever the URL
// pinned; see [DatabaseRealmKeeping].
func Reset(ctx context.Context, conn *dbschema.DatabaseConnection, baseline Baseline) error {
	if conn == nil {
		return fmt.Errorf("reset dev database: nil database connection")
	}
	if baseline.realm {
		return DatabaseRealmKeeping(ctx, conn, baseline)
	}
	return conn.SchemaWriter().DropAllTables(ctx)
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
