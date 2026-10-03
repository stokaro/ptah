// Package devclean resets disposable development databases between migration
// replay operations.
package devclean

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"ptah.run/dbschema"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/devdocker"
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
// cleanups leave in place: the extensions installed there, and, when the URL
// pinned one schema, the other schemas of the database.
//
// A dev database's extensions are its environment, as they are to the Atlas
// community binary, which applies a migration that uses a preinstalled
// extension's type without creating it. Removing them took that type away
// before the first migration ran, and a TimescaleDB extension created again in
// the same session answered `schema "_timescaledb_functions" does not exist`
// (stokaro/ptah#3542). An extension the run creates is not in the baseline,
// so the cleanup after the run still removes it.
//
// A URL that pins one schema has the claim judge that schema alone, as the
// pinned community binary judges it, and that binary leaves the database's
// other schemas as they were, tables included. A replay empties the engine's
// whole realm, so without the schemas in the baseline it dropped a table in a
// schema nobody had checked. A schema the run creates is not in the baseline
// either, and is removed; an object the run creates inside a kept schema stays.
//
// The zero Baseline keeps nothing, which is [DatabaseRealm].
type Baseline struct {
	extensions []string
	// schemas are the schemas outside the one the URL pinned; see above.
	schemas []string
	// realm records whether the claim judged the dev database's whole realm
	// or only its connected schema; see [Reset].
	realm bool
	// server records whose server the dev database is on; see [Claim].
	server dbreset.Server
}

// Extensions returns the extension names the baseline keeps, sorted.
func (b Baseline) Extensions() []string {
	return slices.Clone(b.extensions)
}

// Schemas returns the schemas a realm cleanup leaves as they are, sorted: the
// ones outside the schema the URL pinned. It is empty when the URL pinned
// none.
func (b Baseline) Schemas() []string {
	return slices.Clone(b.schemas)
}

type extensionLister interface {
	InstalledExtensions(context.Context) ([]string, error)
}

type schemaLister interface {
	UserSchemas(context.Context) ([]string, error)
}

type databaseRealmKeeper interface {
	DropDatabaseRealmKeepingSchemas(context.Context, []string, []string, dbreset.Server) error
}

type schemaKeeper interface {
	DropAllTablesKeeping(context.Context, []string) error
}

// Claim takes a dev database for one run. It refuses one that is not clean,
// see [EnsureClean], and records what [DatabaseRealmKeeping] leaves in place.
// It reads the database as it is, so it belongs before the run's first
// cleanup, and before the caller registers the cleanup that runs on its way
// out: registered first, that cleanup would run on the refusal too. A dialect
// without extensions captures an empty baseline.
//
// It also records whose server the database is on, from the record
// [devdocker.RunOwned] reads, which a migration replay reads to choose its
// realm too. On a server the run owns, a realm cleanup empties the server's
// default user database, such as PostgreSQL's postgres, which it refuses on
// any other server.
func Claim(ctx context.Context, conn *dbschema.DatabaseConnection) (Baseline, error) {
	if conn == nil {
		return Baseline{}, fmt.Errorf("capture dev database baseline: nil database connection")
	}
	if err := EnsureClean(ctx, conn); err != nil {
		return Baseline{}, err
	}
	baseline := Baseline{realm: migrateclean.RealmScoped(conn), server: dbreset.NamedServer}
	if devdocker.RunOwned(conn.Info().URL) {
		baseline.server = dbreset.OwnedServer
	}
	writer := conn.SchemaWriter()
	if lister, ok := writer.(extensionLister); ok {
		extensions, err := lister.InstalledExtensions(ctx)
		if err != nil {
			return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
		}
		baseline.extensions = extensions
	}
	if lister, ok := writer.(schemaLister); ok && !baseline.realm {
		schemas, err := lister.UserSchemas(ctx)
		if err != nil {
			return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
		}
		pinned := conn.Info().Schema
		for _, schema := range schemas {
			if schema != pinned {
				baseline.schemas = append(baseline.schemas, schema)
			}
		}
	}
	return baseline, nil
}

// EnsureClean refuses a dev database that holds objects no run of Ptah put
// there, before anything resets it.
//
// A reset drops every user object in the realm, and a dev database the
// operator pointed at by mistake held their data. The pinned community binary
// refuses such a database, and so does every caller of this function: the
// predicate and the object the refusal names are [migrateclean.DevRefusal]'s.
// Every dialect's writer lists what its reset drops, a view or a function as
// well as a table, and the predicate refuses those too. An object the reset
// does not list is not dropped: a PostgreSQL text search configuration keeps
// its schema, and the realm cleanup's failure names it.
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
// one alone, since the claim did not look there. The baseline's extensions
// stay in either scope.
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
	writer := conn.SchemaWriter()
	if keeper, ok := writer.(schemaKeeper); ok {
		return keeper.DropAllTablesKeeping(ctx, baseline.extensions)
	}
	if len(baseline.extensions) > 0 {
		return fmt.Errorf("reset dev database: this writer cannot keep the extensions the database held before the run")
	}
	return writer.DropAllTables(ctx)
}

// DatabaseRealmKeeping is [DatabaseRealm] that leaves the baseline in place.
//
// A baseline that names extensions was captured through a writer that keeps
// them, so a writer that cannot is refused rather than allowed to remove
// them.
//
// A refusal of the server's default user database on a server the run does not
// own ends with the two ways to make it the run's.
func DatabaseRealmKeeping(ctx context.Context, conn *dbschema.DatabaseConnection, baseline Baseline) error {
	if conn == nil {
		return fmt.Errorf("clean dev database realm: nil database connection")
	}
	if keeper, ok := conn.SchemaWriter().(databaseRealmKeeper); ok {
		err := keeper.DropDatabaseRealmKeepingSchemas(ctx, baseline.extensions, baseline.schemas, baseline.server)
		if errors.Is(err, dbreset.ErrServerDefaultDatabase) {
			return fmt.Errorf("%w; %s", err, devdocker.OwnedServerRemedy)
		}
		return err
	}
	if len(baseline.extensions) > 0 || len(baseline.schemas) > 0 {
		return fmt.Errorf("clean dev database realm: this writer cannot keep the extensions or schemas the database held before the run")
	}
	return DatabaseRealm(ctx, conn)
}
