// Package devsnapshot reproduces, on the Atlas-compatible surface, the pinned
// community binary's refusal of a dev database that is not clean.
//
// The pinned community binary v1.3.0 takes a snapshot of the dev database
// before it uses one, and refuses at exit 1, touching nothing, when the
// snapshot finds a table. Measured on 2026-09-26 against PostgreSQL 18, MySQL
// 8.4, MariaDB 11.8 and SQLite, every verb that takes --dev-url does so
// whenever it uses the dev database:
//
//	migrate diff       always
//	migrate lint       always, with no migration file to lint too
//	migrate validate   whenever --dev-url is given, an empty directory too
//	schema inspect     when -u is a schema file, a directory or a migration directory
//	schema diff        when either side is not a database
//	schema apply       when --to is not a database, with no change to plan too
//
// A database source alone does not use the dev database, and the binary does
// not look at it: `schema diff` between two databases, `schema apply --to` a
// database and `schema inspect -u` a database exit 0 with a table in it.
//
// Ptah resets a dev database where the binary takes a snapshot, and the reset
// refuses the same database for the same reason: see
// ptah.run/internal/devclean.EnsureClean. That refusal is not enough on this
// surface. Some of these runs never reset the dev database -- two schema files
// are compared as read, a plan with no change is not rehearsed, an empty
// directory is not replayed -- and without this package they exit 0 where the
// binary exits 1. The binary's sentence also carries a prefix that depends on
// the verb and the source, which only the verb can reproduce.
package devsnapshot

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/dbschema"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/migrateclean"
)

// Check is how a verb words the refusal: which prefix the binary puts in
// front of `connected database is not clean: …`.
type Check int

const (
	// Declarative is a source that is already a schema document: an HCL file
	// or a directory of them. The binary applies it to the dev database with
	// no snapshot, and prints the sentence bare.
	Declarative Check = iota
	// Snapshot is SQL the dev database has to run: a SQL file, a directory of
	// them, or a migration directory. `sql/migrate: taking database snapshot:`
	// comes first.
	Snapshot
	// Lint is `migrate lint`, which prints `taking database snapshot:` without
	// the package name.
	Lint
	// Replay is `migrate validate`, which prints `replaying the migration
	// directory:` in front of the snapshot prefix.
	Replay
)

// Wrap puts the verb's prefix in front of the refusal, which the binary
// prints after its package name.
func (c Check) Wrap(err *migrateclean.NotCleanError) error {
	switch c {
	case Snapshot:
		return fmt.Errorf("sql/migrate: taking database snapshot: sql/migrate: %w", err)
	case Lint:
		return fmt.Errorf("taking database snapshot: sql/migrate: %w", err)
	case Replay:
		return fmt.Errorf("replaying the migration directory: sql/migrate: taking database snapshot: sql/migrate: %w", err)
	default:
		return fmt.Errorf("sql/migrate: %w", err)
	}
}

// ForSources reports whether the binary uses the dev database for these
// sources, and how it words the refusal when it does.
//
// A database source does not use it. Any other source does, and the prefix is
// the snapshot one unless every source that uses it is a schema document:
// measured, `schema diff --from file://a.hcl --to file://b.sql` and the
// reverse both print the snapshot prefix, and HCL on both sides prints none.
func ForSources(sets ...atlassource.Set) (Check, bool) {
	check := Declarative
	uses := false
	for _, set := range sets {
		if set.Kind == atlassource.KindDatabase || len(set.Sources) == 0 {
			continue
		}
		uses = true
		if !set.DeclarativeLocalFiles() {
			check = Snapshot
		}
	}
	return check, uses
}

// Refuse returns the binary's refusal when the dev database devURL names holds
// a table, and nil otherwise.
//
// It returns nil without looking in three cases, each of which the verb
// answers itself. A `docker://` value names a container the run starts, which
// is empty. A dev database that cannot be reached or read is reported by the
// verb when it connects, in the words its own tests pin; this check does not
// guess at them. And a dialect [migrateclean.Governs] does not cover has no
// measured refusal to reproduce.
func Refuse(ctx context.Context, devURL string, check Check) error {
	if strings.TrimSpace(devURL) == "" || devdocker.IsURL(devURL) {
		return nil
	}
	connectCtx, cancel := dbcli.ConnectContext(ctx, dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, strings.TrimSpace(devURL))
	if err != nil {
		return nil //nolint:nilerr // the verb reports the connection failure itself
	}
	defer dbschema.CloseAndWarn(conn)
	return RefuseConnection(ctx, conn, check)
}

// RefuseConnection is [Refuse] for a verb that already holds the dev
// connection. A catalog that cannot be read is left to the reset that follows,
// which refuses it in its own words: see ptah.run/internal/devclean.EnsureClean.
func RefuseConnection(ctx context.Context, conn *dbschema.DatabaseConnection, check Check) error {
	if notClean, ok := errors.AsType[*migrateclean.NotCleanError](migrateclean.DevRefusal(ctx, conn)); ok {
		return check.Wrap(notClean)
	}
	return nil
}
