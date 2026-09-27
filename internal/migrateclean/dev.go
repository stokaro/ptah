package migrateclean

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
)

// NotCleanError is the refusal of a dev database that already holds objects,
// in the words the pinned community binary v1.3.0 uses before it takes one.
//
// Its text is the part every verb of that binary prints the same way, less the
// `sql/migrate:` package name the binary puts in front of it. What a verb puts
// there differs -- `sql/migrate: taking database snapshot: sql/migrate:` for a
// source it replays, `sql/migrate:` alone for an HCL one -- and belongs to the
// Atlas-compatible verb; a native one words it itself.
type NotCleanError struct {
	// Reason names the object that made the database unclean:
	// `found table "t" in connected schema`, `found schema "s"`.
	Reason string
}

// Error returns the binary's sentence for the reason.
func (e *NotCleanError) Error() string {
	return "connected database is not clean: " + e.Reason
}

// DevRefusal reads the dev database the connection selects and returns the
// refusal the pinned binary gives before it uses one that is not clean, or nil
// when it is clean or its dialect is not one [GovernsDev] covers.
//
// A dev database is reset before and after a run uses it, so a caller that
// resets one asks this first and refuses on a non-nil answer. The binary takes
// a snapshot of the dev database instead of resetting it, and refuses when the
// snapshot finds tables: measured per verb on 2026-09-26 with `migrate diff`,
// `migrate lint`, `migrate validate`, `schema inspect`, `schema diff` and
// `schema apply`, each refusing at exit 1 with the dev database untouched.
//
// An error that is not a *NotCleanError means the catalog could not be read,
// and the caller must not treat the database as clean.
func DevRefusal(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	scope, err := inspect(ctx, conn, GovernsDev)
	if err != nil {
		return err
	}
	return scope.DevRefusal()
}

// GovernsDev reports whether a dev database of the dialect is checked before
// it is reset.
//
// It is wider than [Governs], which is the `migrate apply` gate and covers only
// the dialects that gate was measured on. A reset drops the tables of a dev
// database on every dialect Ptah replays or rehearses on, so every one of them
// is checked. Without the check, a CockroachDB, YugabyteDB, Spanner, SQL
// Server or ClickHouse dev database holding a table is emptied by the reset.
//
//   - PostgreSQL, MySQL, MariaDB and SQLite, measured against the pinned
//     community binary, which refuses the same databases.
//   - CockroachDB, which that binary reaches through a `postgres://` URL and
//     judges by the PostgreSQL rules: measured on CockroachDB 26.2 on
//     2026-09-27, it refuses a table in the connected schema and, with no
//     search_path, any schema but an empty public, and it accepts a view, a
//     sequence or an empty database. YugabyteDB takes the same rules.
//   - Spanner, SQL Server, ClickHouse and Oracle, whose URLs that binary does
//     not open (`unknown driver`), and neither does it open `cockroachdb://`
//     or `yugabytedb://`. There is no sentence of its to match, and the
//     refusal uses the shape of its others.
//
// Oracle is checked although no run resets an Oracle dev database: the
// replay's lock and the rehearsal's identity check both refuse one first. The
// check keeps a path that reaches the reset without them from emptying one.
func GovernsDev(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
		platform.MySQL, platform.MariaDB, platform.SQLite,
		platform.SQLServer, platform.ClickHouse, platform.Oracle:
		return true
	default:
		return false
	}
}

// RealmScoped reports whether conn's URL left the run at realm scope, where
// [Scope.DevRefusal] judges every schema of the database rather than the
// connected one. A caller that resets the dev database after the check resets
// the same scope the check judged, so the reset cannot reach an object the
// check did not look at.
func RealmScoped(conn *dbschema.DatabaseConnection) bool {
	if conn == nil {
		return false
	}
	info := conn.Info()
	return realmScoped(info.Dialect, info.URL, strings.TrimSpace(info.Schema))
}

// DevRefusal returns the pinned binary's refusal of a dev database with this
// scope, or nil when the database is clean.
//
// The operand is tables, as it is for `migrate apply`, and the object the
// refusal names is chosen the same way; the sentences differ, and a dev
// database has no revision table to exempt. Measured against PostgreSQL 18,
// MySQL 8.4, MariaDB 11.8 and SQLite on 2026-09-26:
//
//   - Schema scope names the first table in byte order: `found table "t" in
//     connected schema` on PostgreSQL, `found table "t" in schema "dev"` on
//     MySQL and MariaDB. A view, a sequence, a function, an enum or an
//     extension alone is clean, and so is a table in another schema.
//   - SQLite names the first table in catalog order, which is the order the
//     tables were created in: `found table "t"`. `sqlite_sequence` alone is
//     clean.
//   - Realm scope, a PostgreSQL URL that pins no search_path, is clean with no
//     schema at all, or with `public` alone and no table in it: `found table
//     "t" in schema "public"` names its first table otherwise. Any other
//     schema, even an empty one, refuses with `found schema "s"`, naming the
//     first schema in byte order -- which is `public` itself when it sorts
//     first, whether or not it holds anything.
func (s Scope) DevRefusal() error {
	if !GovernsDev(s.Dialect) {
		return nil
	}
	if s.Realm {
		return s.devRealmRefusal()
	}
	if len(s.Tables) == 0 {
		return nil
	}
	switch platform.NormalizeDialect(s.Dialect) {
	case platform.SQLite:
		return &NotCleanError{Reason: fmt.Sprintf("found table %q", s.Tables[0])}
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return &NotCleanError{Reason: fmt.Sprintf("found table %q in connected schema", s.Tables[0])}
	default:
		return &NotCleanError{Reason: fmt.Sprintf("found table %q in schema %q", s.Tables[0], s.Schema)}
	}
}

// devRealmRefusal is [Scope.DevRefusal] for a connection that pinned no
// schema. Schemas is in byte order, which is the order the binary names them
// in.
func (s Scope) devRealmRefusal() error {
	switch {
	case len(s.Schemas) == 0:
		return nil
	case len(s.Schemas) == 1 && s.Schemas[0].Name == postgresDefaultSchema:
		if len(s.Schemas[0].Tables) == 0 {
			return nil
		}
		return &NotCleanError{Reason: fmt.Sprintf("found table %q in schema %q",
			s.Schemas[0].Tables[0], postgresDefaultSchema)}
	default:
		return &NotCleanError{Reason: fmt.Sprintf("found schema %q", s.Schemas[0].Name)}
	}
}
