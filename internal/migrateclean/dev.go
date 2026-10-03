package migrateclean

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/ydbgap"
)

// NotCleanError is the refusal of a dev database that already holds objects,
// in the words the pinned community binary v1.3.0 uses before it takes one.
//
// Its text is the part every verb of that binary prints the same way, less the
// `sql/migrate:` package name the binary puts in front of it. What a verb puts
// there differs -- `sql/migrate: taking database snapshot: sql/migrate:` for a
// source it replays, `sql/migrate:` alone for an HCL one -- and belongs to the
// Atlas-compatible verb; a native one words it itself.
//
// The binary refuses tables and schemas only. A refusal naming another object
// is Ptah's, in the same shape; see [Scope.DevRefusal].
type NotCleanError struct {
	// Reason names the object that made the database unclean:
	// `found table "t" in connected schema`, `found schema "s"`,
	// `found view "v" in connected schema`.
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
// It also reads what else the reset would drop, as the dialect's writer lists
// it through the query the reset runs. See [Scope.DevRefusal] for why those
// refuse.
//
// An error that is not a *NotCleanError means the catalog could not be read,
// and the caller must not treat the database as clean.
//
// A YDB database is refused outright: the reset, the clean check and the
// realm identity a dev run needs are not implemented for it yet.
func DevRefusal(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	if conn != nil && platform.NormalizeDialect(conn.Info().Dialect) == platform.YDB {
		return errors.New(ydbgap.DevDatabases.Message())
	}
	scope, err := inspect(ctx, conn, GovernsDev)
	if err != nil {
		return err
	}
	if !GovernsDev(scope.Dialect) {
		return nil
	}
	// A whole MySQL-family server is reset database by database, so any user
	// database refuses it and there is nothing smaller to list.
	if scope.Realm && isMySQLFamily(scope.Dialect) {
		return scope.DevRefusal()
	}
	scope.Dropped, err = resetObjects(ctx, conn, scope)
	if err != nil {
		return fmt.Errorf("clean check: %w", err)
	}
	return scope.DevRefusal()
}

// resetObjectLister is the writer that says what a reset of its database
// drops. The list is the reset's own, so the check and the reset cannot
// disagree about which objects a reset removes.
type resetObjectLister interface {
	ResetObjects(ctx context.Context, scope dbreset.Scope) ([]dbreset.Object, error)
}

// installedExtensionLister is the writer that names the extensions a claim
// keeps, and with them everything they own.
type installedExtensionLister interface {
	InstalledExtensions(ctx context.Context) ([]string, error)
}

// resetObjects reads what a reset of the scope drops, keeping the extensions
// the database holds, which is what a claim keeps. Every dialect's writer
// lists it. A writer that cannot is refused rather than read as listing
// nothing, since the check would then count tables alone and the reset would
// drop the rest in silence.
func resetObjects(ctx context.Context, conn *dbschema.DatabaseConnection, scope Scope) ([]dbreset.Object, error) {
	writer := conn.SchemaWriter()
	lister, ok := writer.(resetObjectLister)
	if !ok {
		return nil, fmt.Errorf("the %s writer cannot list what its reset drops", scope.Dialect)
	}
	var kept []string
	if extensions, ok := writer.(installedExtensionLister); ok {
		installed, err := extensions.InstalledExtensions(ctx)
		if err != nil {
			return nil, err
		}
		kept = installed
	}
	schemas := []string{scope.Schema}
	if scope.Realm {
		schemas = make([]string, 0, len(scope.Schemas))
		for _, schema := range scope.Schemas {
			schemas = append(schemas, schema.Name)
		}
	}
	return lister.ResetObjects(ctx, dbreset.Scope{
		Schemas:               schemas,
		KeptExtensions:        kept,
		KeptDefaultPrivileges: keptDefaultPrivileges(scope.pinnedSchema()),
	})
}

// pinnedSchema is the schema the URL pinned, or the empty string at realm
// scope.
func (s Scope) pinnedSchema() string {
	if s.Realm {
		return ""
	}
	return s.Schema
}

// KeptDefaultPrivileges names the PostgreSQL default privileges a dev database
// keeps across its resets, as it keeps its extensions: they are the dev
// database's environment, not something a run put there.
//
// With a URL that pins a schema, that is the defaults set in the schema and
// the global ones, which apply in every schema. With none, it is the global
// ones alone. Measured against the pinned community binary v1.3.0 on
// PostgreSQL 18 on 2026-10-03, with `ALTER DEFAULT PRIVILEGES FOR ROLE postgres
// IN SCHEMA public GRANT SELECT ON TABLES TO app` and `ALTER DEFAULT PRIVILEGES
// FOR ROLE postgres GRANT USAGE ON SEQUENCES TO app` in the dev database,
// `migrate diff` exits 0 in both scopes and keeps the global default in both.
// It keeps the one set in public with `?search_path=public`, and drops it with
// no search_path, since its realm cleanup drops public with everything set in
// it. A default the run added survives the binary's cleanup; Ptah's reset
// removes it.
//
// So the claim keeps what the binary keeps, and the clean check still refuses
// a default set in a schema of a URL that pins none, where the binary drops it
// in silence; see [Scope.DevRefusal]. The check and the claim both ask this
// function, so a default the check lets through is one the reset keeps.
func KeptDefaultPrivileges(conn *dbschema.DatabaseConnection) dbreset.DefaultPrivilegeScope {
	if conn == nil {
		return dbreset.DefaultPrivilegeScope{}
	}
	if RealmScoped(conn) {
		return keptDefaultPrivileges("")
	}
	return keptDefaultPrivileges(strings.TrimSpace(conn.Info().Schema))
}

// keptDefaultPrivileges is [KeptDefaultPrivileges] for a URL that pinned
// schema, or none when schema is empty.
func keptDefaultPrivileges(schema string) dbreset.DefaultPrivilegeScope {
	if schema == "" {
		return dbreset.DefaultPrivilegeScope{Global: true}
	}
	return dbreset.DefaultPrivilegeScope{Schemas: []string{schema}, Global: true}
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

// DevRefusal returns the refusal of a dev database with this scope, or nil when
// the database is clean.
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
//
// Past those, the refusal is Ptah's and deliberately stricter than the
// binary's: a dev database holding anything else the reset would drop, listed
// in Dropped, is refused with the first of them, `found view "v" in connected
// schema` in schema scope and `found view "v" in schema "public"` in realm
// scope, `found large object "16385"`, or a default privilege the reset
// revokes, named by owner, object type and grantee. On the other engines the
// object is named with its schema, `found view "v" in schema "dev"`, as a
// table is there, and on SQLite without one. Measured against the binary
// v1.3.0 on PostgreSQL 18.6 on 2026-09-27, with a view, a materialized view, a
// function, a procedure, a sequence, an enum, a domain, a composite type, a
// collation or a default privilege alone in the dev database: with a
// search_path it accepts the database and leaves the object in place, because
// it does not model those kinds; with none it accepts the database and drops
// every one of them but the enum. Ptah's reset drops all of them in both
// scopes, and a replay drops a large object too, where the binary leaves it.
// The default privileges [KeptDefaultPrivileges] names are the exception: the
// reset returns them to what they were, so they are not in Dropped and do not
// refuse.
// On MySQL 26.7 and MariaDB 12.3 the binary keeps a view, a routine, an event,
// a MariaDB sequence and a system-versioned table, and Ptah's reset dropped
// each; on SQLite both drop a view. SQL Server, ClickHouse and Oracle have no
// binary to compare with, and their resets drop views, routines, sequences,
// synonyms, types, dictionaries and schemas (stokaro/ptah#3851).
//
// A refusal keeps the object where the binary would keep it, and says so where
// the binary would drop it in silence; keeping it instead would have every dev
// read filter it out, and a migration's `CREATE ... IF NOT EXISTS` pass
// against it (stokaro/ptah#3808).
func (s Scope) DevRefusal() error {
	if !GovernsDev(s.Dialect) {
		return nil
	}
	if s.Realm {
		if refusal := s.devRealmRefusal(); refusal != nil {
			return refusal
		}
		return s.droppedRefusal()
	}
	if len(s.Tables) == 0 {
		return s.droppedRefusal()
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

// droppedRefusal names the first object in Dropped, or returns nil when the
// reset drops nothing but the tables already judged.
func (s Scope) droppedRefusal() error {
	if len(s.Dropped) == 0 {
		return nil
	}
	object := s.Dropped[0]
	switch {
	case object.Schema == "":
		return &NotCleanError{Reason: fmt.Sprintf("found %s %q", object.Kind, object.Name)}
	case s.Realm || !platform.IsPostgresFamily(s.Dialect):
		return &NotCleanError{Reason: fmt.Sprintf("found %s %q in schema %q", object.Kind, object.Name, object.Schema)}
	default:
		return &NotCleanError{Reason: fmt.Sprintf("found %s %q in connected schema", object.Kind, object.Name)}
	}
}

// devRealmRefusal is [Scope.DevRefusal] for a connection that pinned no
// schema. Schemas is in byte order, which is the order the binary names them
// in.
//
// On a whole MySQL or MariaDB server every user database refuses, even an
// empty one and even one named atlas_schema_revisions: measured on MySQL
// 8.4.11 and MariaDB 11.8.9, the binary refuses `found schema "A2"` for a dev
// server holding b1, A2 and c3 (stokaro/ptah#3789).
func (s Scope) devRealmRefusal() error {
	switch {
	case len(s.Schemas) == 0:
		return nil
	case isMySQLFamily(s.Dialect):
		return &NotCleanError{Reason: fmt.Sprintf("found schema %q", s.Schemas[0].Name)}
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
