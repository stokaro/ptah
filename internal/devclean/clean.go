// Package devclean resets disposable development databases between migration
// replay operations.
package devclean

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/migrateclean"
	"ptah.run/internal/pgsnapshot"
	"ptah.run/internal/readscope"
)

type sessionObjectDropper interface {
	DropSessionObjects(context.Context) error
}

// SessionObjects removes the objects the pinned session of conn holds for
// itself, such as SQLite's TEMP tables, and does nothing for an engine whose
// session holds none a replay could create. A replay that admits session
// objects (see [ReplayGuard.WithSessionScope]) calls it before it cleans the
// realm and on every outcome, because a database that keeps its only
// connection would hand them to the next caller.
func SessionObjects(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	if conn == nil {
		return fmt.Errorf("drop session objects: nil database connection")
	}
	if dropper, ok := conn.SchemaWriter().(sessionObjectDropper); ok {
		return dropper.DropSessionObjects(ctx)
	}
	return nil
}

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
// cleanups leave in place: the extensions installed there, its PostgreSQL
// default privileges, and, when the URL pinned one schema, the other schemas
// of the database.
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
// The default privileges are environment too. An image such as Supabase's
// grants its API roles on every table created in public through `ALTER
// DEFAULT PRIVILEGES ... IN SCHEMA public`, and the pinned community binary
// takes a dev database holding them. A reset that revoked them would leave the
// claim to refuse such a database rather than lose them, and the image could
// not be a dev database at all (stokaro/ptah#4034). The baseline records the ones
// [migrateclean.KeptDefaultPrivileges] names, and every reset returns them to
// what they were: a default the run added is revoked, one it changed is set
// back, and one it did not touch is left alone.
//
// So are the database-scoped objects a realm cleanup cannot remove, such as an
// event trigger or a publication. The pinned community binary uses a dev
// database holding them and leaves them, and the Supabase image ships six
// event triggers and a publication. A realm cleanup refused the database
// rather than leave them (stokaro/ptah#4055). The baseline records the ones
// the database held, which the cleanup leaves and checks are still there; it
// still refuses one the run created.
//
// A dev database an atlas.hcl docker block provisioned keeps more: the whole
// state the image and the block's baseline left, which is its starting point
// (stokaro/ptah#4056). The Supabase image holds the auth, storage and realtime
// schemas, their tables and functions and the grants on them, and none of it
// is the run's. The claim does not judge such a database clean; it records
// the starting point, and every reset returns the database to it, removing
// what the run added in any schema -- a trigger a migration puts on
// auth.users included -- and putting back each privilege the run changed. A
// URL that names no block keeps the rule the community binary applies.
//
// The schemas a realm cleanup keeps -- the one the URL resolves to, and
// public -- are environment in what they hold beside their objects: their
// owner, their access list and their comment. A run changes those when it
// creates a document that revokes USAGE on public from PUBLIC or comments the
// schema, and a cleanup that kept what it found handed the change to the next
// run. Measured on PostgreSQL 18, the next diff through the same dev database
// planned `GRANT USAGE ON SCHEMA "public" TO PUBLIC` for a pair it had
// reported synced before. The baseline records each as the claim found it, and
// every realm cleanup returns the schema to it.
//
// On a server the run owns, the baseline holds the server's roles as well. A
// replay there runs CREATE ROLE, and a role is the server's rather than the
// dev database's, so a reset of the database does not reach it. Every reset
// drops the roles the server did not hold at the claim, which on a server the
// run owns are the run's. Without that, a server the operator declared
// disposable, which outlives the run, holds the role for the next replay of
// the same directory, and the server refuses its CREATE ROLE: `role
// "app_reader" already exists` (SQLSTATE 42710). A role the server held at the
// claim is never dropped, whoever created it. On any other server the replay
// refuses CREATE ROLE, and the baseline holds no roles.
//
// The zero Baseline keeps nothing, which is [DatabaseRealm].
type Baseline struct {
	extensions []string
	// schemas are the schemas outside the one the URL pinned; see above.
	schemas []string
	// schemasRecorded reports that the claim listed the schemas outside the
	// pinned one, so a reset can tell a schema the run created from one the
	// database held; see [Reset].
	schemasRecorded bool
	// defaultPrivileges are what the default privileges were at the claim;
	// see above.
	defaultPrivileges dbreset.DefaultPrivileges
	// artifacts are the database-scoped objects the database held; see above.
	artifacts []dbreset.Object
	// artifactsRecorded reports that the claim listed the database-scoped
	// objects, so a reset of the one schema a URL pinned can tell a
	// publication the run created from one the database held; see [Reset].
	artifactsRecorded bool
	// schemaStates are the owner, access list and comment of the schemas a
	// realm cleanup keeps, as the claim found them; see above.
	schemaStates []dbreset.SchemaState
	// startingPoint is the whole state of a dev database an atlas.hcl docker
	// block provisioned; see above. Nil for every other dev database.
	startingPoint *pgsnapshot.Snapshot
	// environment is the same state as a read describes it, for
	// [Baseline.WithoutStartingPoint]. Nil for every other dev database.
	environment *catalog.Database
	// realm records whether the claim judged the dev database's whole realm
	// or only its connected schema; see [Reset].
	realm bool
	// server records whose server the dev database is on; see [Claim].
	server dbreset.Server
	// roles are the server's roles at the claim, read on a server the run
	// owns through a writer that can drop the ones added since; see above.
	// Nil everywhere else, and a reset then drops no role.
	roles []string
	// survivors are the names a realm reset leaves in place whoever created
	// them, as the claim read them through a writer that can say, and
	// survivorsRecorded reports that it did; see [Baseline.Survivors].
	survivors         dbreset.Survivors
	survivorsRecorded bool
	// databaseRoles are the SQL Server database roles the database held at
	// the claim, and databaseRolesRecorded reports that the claim listed
	// them through a writer that drops the ones added since; see
	// [Baseline.DatabaseRoles].
	databaseRoles         []string
	databaseRolesRecorded bool
}

// RolesAtClaim returns the roles the dev server held at the claim, sorted, on
// a server the run owns, where every reset drops the roles the run created. It
// is nil on any other server: a reset there drops no role, so a role a run
// creates outlives it.
func (b Baseline) RolesAtClaim() []string {
	return slices.Clone(b.roles)
}

// StartingPoint returns the starting point of a dev database an atlas.hcl
// docker block provisioned, as a read describes it, and nil for any other dev
// database. The caller must not change it. See [Baseline.WithoutStartingPoint].
func (b Baseline) StartingPoint() *catalog.Database {
	return b.environment
}

// Schemas returns the schemas a realm cleanup leaves as they are, sorted: the
// ones outside the schema the URL pinned. It is empty when the URL pinned
// none.
func (b Baseline) Schemas() []string {
	return slices.Clone(b.schemas)
}

// SchemasLeftAsFound returns the schemas every reset leaves as the claim found
// them, with everything they hold, sorted: the ones beside the schema a URL
// pinned. An object a run writes into one of them outlives the run, and the
// reset cannot remove it, since it does not know the object from what the
// schema held at the claim; so a run must not write there. It is empty for a
// realm cleanup, which drops what a run added in every schema, and for a
// database that returns to a starting point, which every reset restores whole.
func (b Baseline) SchemasLeftAsFound() []string {
	if b.realm || b.startingPoint != nil {
		return nil
	}
	return slices.Clone(b.schemas)
}

type extensionLister interface {
	InstalledExtensions(context.Context) ([]string, error)
}

type startingPointRecorder interface {
	CaptureStartingPoint(context.Context) (*pgsnapshot.Snapshot, error)
}

// artifactLister is a writer that lists the database-scoped objects of its
// database, and says whether its server answers the list at all: one that does
// not lists none, and its cleanups cannot tell a publication the run created.
type artifactLister interface {
	DatabaseScopedArtifacts(context.Context) ([]dbreset.Object, error)
	ReadsDatabaseScopedArtifacts(context.Context) (bool, error)
}

// addedPublicationDropper is a writer that drops the publications its database
// holds beside the ones a claim found.
type addedPublicationDropper interface {
	DropPublicationsAddedSince(context.Context, dbreset.Kept) error
}

type defaultPrivilegeReader interface {
	DefaultPrivilegeBaseline(context.Context, dbreset.DefaultPrivilegeScope) (dbreset.DefaultPrivileges, error)
}

type schemaLister interface {
	UserSchemas(context.Context) ([]string, error)
}

// survivorReader is a writer that reads the names its realm reset leaves in
// place whoever created them, and says whether it could; see
// [dbreset.Survivors].
type survivorReader interface {
	RealmResetSurvivors(context.Context) (dbreset.Survivors, bool, error)
}

type schemaStateReader interface {
	SchemaStates(context.Context) ([]dbreset.SchemaState, error)
}

// databaseRoleReader is a writer that lists the database roles of its
// database, and whose realm reset drops the ones a claim did not find; see
// [Baseline.DatabaseRoles].
type databaseRoleReader interface {
	DatabaseRoles(context.Context) ([]string, error)
}

type databaseRealmKeeper interface {
	DropDatabaseRealmKeeping(context.Context, dbreset.Kept) error
}

type schemaKeeper interface {
	DropAllTablesKeeping(context.Context, dbreset.Kept) error
}

// addedSchemaDropper is a writer that drops the schemas beside the pinned one
// that a claim did not find; see [Reset].
type addedSchemaDropper interface {
	DropSchemasAddedSince(context.Context, dbreset.Kept) error
}

// roleKeeper is a writer that lists the server's roles and drops the ones a
// later read finds that an earlier one did not; see [Baseline].
type roleKeeper interface {
	ServerRoles(context.Context) ([]string, error)
	DropRolesAddedSince(context.Context, []string) error
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
// any other server, and every reset drops the roles the claim did not find
// there; see [Baseline].
func Claim(ctx context.Context, conn *dbschema.DatabaseConnection) (Baseline, error) {
	if conn == nil {
		return Baseline{}, fmt.Errorf("capture dev database baseline: nil database connection")
	}
	writer := conn.SchemaWriter()
	recorder, records := writer.(startingPointRecorder)
	fromBlock := records && migrateclean.StartsFromDeclaredState(conn)
	if !fromBlock {
		if err := EnsureClean(ctx, conn); err != nil {
			return Baseline{}, err
		}
	}
	server, roles, err := claimedServer(ctx, conn, writer)
	if err != nil {
		return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
	}
	realm := migrateclean.RealmScoped(conn) || migrateclean.ResetsEverySchema(conn)
	baseline := Baseline{realm: realm, server: server, roles: roles}
	if fromBlock {
		start, err := recorder.CaptureStartingPoint(ctx)
		if err != nil {
			return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
		}
		baseline.startingPoint = start
		baseline.environment, err = dbschema.ReadSchemaWithSchemasContext(readscope.Authored(ctx), conn, start.Schemas)
		if err != nil {
			return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
		}
	}
	if lister, ok := writer.(extensionLister); ok {
		extensions, err := lister.InstalledExtensions(ctx)
		if err != nil {
			return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
		}
		baseline.extensions = extensions
	}
	if reader, ok := writer.(defaultPrivilegeReader); ok {
		defaults, err := reader.DefaultPrivilegeBaseline(ctx, migrateclean.KeptDefaultPrivileges(conn))
		if err != nil {
			return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
		}
		baseline.defaultPrivileges = defaults
	}
	if err := baseline.recordArtifacts(ctx, writer); err != nil {
		return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
	}
	if err := baseline.recordSchemas(ctx, conn, writer); err != nil {
		return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
	}
	if reader, ok := writer.(survivorReader); ok {
		survivors, recorded, err := reader.RealmResetSurvivors(ctx)
		if err != nil {
			return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
		}
		baseline.survivors, baseline.survivorsRecorded = survivors, recorded
	}
	if err := baseline.recordDatabaseRoles(ctx, writer); err != nil {
		return Baseline{}, fmt.Errorf("capture dev database baseline: %w", err)
	}
	return baseline, nil
}

// recordDatabaseRoles records the database roles the database holds, through
// a writer that lists them, on a claim whose reset empties the realm: only a
// realm reset drops the roles a claim did not find, so a claim of anything
// less records none; see [Baseline.DatabaseRoles].
func (b *Baseline) recordDatabaseRoles(ctx context.Context, writer catalog.SchemaWriter) error {
	reader, ok := writer.(databaseRoleReader)
	if !ok || !b.realm {
		return nil
	}
	roles, err := reader.DatabaseRoles(ctx)
	if err != nil {
		return err
	}
	b.databaseRoles, b.databaseRolesRecorded = roles, true
	return nil
}

// recordArtifacts records the database-scoped objects the database holds,
// through a writer that lists them, and whether the list is a record at all:
// a server that cannot answer it lists none however many it holds, and its
// resets cannot tell a publication the run created; see [Baseline.Publications].
func (b *Baseline) recordArtifacts(ctx context.Context, writer catalog.SchemaWriter) error {
	lister, ok := writer.(artifactLister)
	if !ok {
		return nil
	}
	artifacts, err := lister.DatabaseScopedArtifacts(ctx)
	if err != nil {
		return err
	}
	recorded, err := lister.ReadsDatabaseScopedArtifacts(ctx)
	if err != nil {
		return err
	}
	b.artifacts, b.artifactsRecorded = artifacts, recorded
	return nil
}

// Publications is what every reset of the run does with the dev database's
// publications, for [ReplayGuard.WithPublications]. A reset drops each
// publication the claim did not find once the claim listed them, whether it
// empties the realm, returns to a starting point or empties the one schema a
// URL pinned, and keeps the ones the claim found as it finds them. A claim
// that could not list them drops none.
func (b Baseline) Publications() replay.PublicationScope {
	if !b.artifactsRecorded {
		return replay.PublicationScope{}
	}
	scope := replay.PublicationScope{Dropped: true}
	for _, artifact := range b.artifacts {
		if artifact.Kind == "publication" {
			scope.Kept = append(scope.Kept, artifact.Name)
		}
	}
	return scope
}

// DatabaseRoles is what every reset of the run does with the dev database's
// SQL Server database roles, for [ReplayGuard.WithDatabaseRoles]. A realm
// reset drops each database role the claim did not find once the claim listed
// them, and keeps the ones it found. A claim that did not list them, and a
// reset of the one schema a URL pinned, drop none.
func (b Baseline) DatabaseRoles() replay.DatabaseRoleScope {
	if !b.databaseRolesRecorded {
		return replay.DatabaseRoleScope{}
	}
	return replay.DatabaseRoleScope{Dropped: true, Kept: slices.Clone(b.databaseRoles)}
}

// Survivors names the extensions and schemas that outlive every reset of the
// run, for [ReplayGuard.WithSurvivors]; see [replay.CleanupSurvivors].
//
// It records them only for a claim that judged the whole realm, through a
// writer that read the names, and whose database has no starting point. Every
// reset of such a claim, [Reset] and [DatabaseRealmKeeping] alike, is a realm
// reset, which drops each extension and each schema the claim did not find,
// wherever it is installed. [Reset] of a claim scoped to the one schema a URL
// pinned leaves the schemas beside it, and refuses an extension it was not
// asked to keep rather than drop it; a database with a starting point is
// returned to it rather than emptied. Neither records any, and a comment on an
// extension or a schema stays refused there.
func (b Baseline) Survivors() replay.CleanupSurvivors {
	if !b.realm || b.startingPoint != nil || !b.survivorsRecorded {
		return replay.CleanupSurvivors{}
	}
	return replay.CleanupSurvivors{
		Recorded:   true,
		Extensions: slices.Clone(b.survivors.Extensions),
		Schemas:    slices.Clone(b.survivors.Schemas),
	}
}

// ReturnsSchemaState reports whether every reset of the run returns the schemas
// it keeps to the owner, access list and comment the claim recorded, and drops
// the others. A realm cleanup does, on an engine whose claim read them. A
// cleanup of the one schema a URL pinned does not read the states, and leaves
// the other schemas as it finds them.
func (b Baseline) ReturnsSchemaState() bool {
	return b.realm && len(b.schemaStates) > 0
}

// recordSchemas records what b keeps of the database's schemas, through a
// writer that reads them: the state of the ones a realm cleanup keeps, unless
// the database returns to a starting point instead, and the other schemas when
// the URL pinned one; see [Baseline].
func (b *Baseline) recordSchemas(ctx context.Context, conn *dbschema.DatabaseConnection, writer catalog.SchemaWriter) error {
	if reader, ok := writer.(schemaStateReader); ok && b.startingPoint == nil {
		states, err := reader.SchemaStates(ctx)
		if err != nil {
			return err
		}
		b.schemaStates = states
	}
	if lister, ok := writer.(schemaLister); ok && !b.realm {
		schemas, err := lister.UserSchemas(ctx)
		if err != nil {
			return err
		}
		b.schemasRecorded = true
		pinned := conn.Info().Schema
		for _, schema := range schemas {
			if schema != pinned {
				b.schemas = append(b.schemas, schema)
			}
		}
	}
	return nil
}

// NotCleanRemedy is the remedy [EnsureClean] names for a dev database that is
// not clean, and the one a run that had to leave its dev database as it was
// names for the refusal the next claim will give.
const NotCleanRemedy = "point --dev-url at an empty database"

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
		return fmt.Errorf("%w; Ptah resets a dev database before and after it uses one, so %s", err, NotCleanRemedy)
	}
	if err != nil {
		return fmt.Errorf("check that the dev database is clean: %w", err)
	}
	return nil
}

// Reset empties a dev database the caller claimed, over the scope the claim
// judged it clean in: the whole realm when the URL pinned no schema or the engine
// judges every schema, and the connected schema when it pinned one. A rehearsal
// resets its dev database this way before and after it runs, so it removes what
// it created -- a schema or an extension schema included -- and leaves a schema
// outside the pinned one alone, since the claim did not look there. A schema
// beside the pinned one that the claim did not find is the run's, and a writer
// that can drop it does, with everything in it: a rehearsal creates one when
// the state it recreates names an object in another schema, and left behind
// it fails the next run's CREATE SCHEMA. The baseline's extensions
// and default privileges stay in either scope, and on a server the run owns
// either scope drops the roles the run created; see [Baseline].
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
	return baseline.resetSchema(ctx, conn.SchemaWriter())
}

// resetSchema is [Reset] of the schema a URL pinned, through writer.
func (b Baseline) resetSchema(ctx context.Context, writer catalog.SchemaWriter) error {
	if keeper, ok := writer.(schemaKeeper); ok {
		if err := keeper.DropAllTablesKeeping(ctx, b.kept()); err != nil {
			return err
		}
		if err := b.dropAddedSchemas(ctx, writer); err != nil {
			return err
		}
		if err := b.dropAddedPublications(ctx, writer); err != nil {
			return err
		}
		return b.dropAddedRoles(ctx, writer)
	}
	if len(b.extensions) > 0 || len(b.defaultPrivileges.Rows) > 0 {
		return fmt.Errorf("reset dev database: this writer cannot keep the extensions or default privileges the database held before the run")
	}
	if err := writer.DropAllTables(ctx); err != nil {
		return err
	}
	return b.dropAddedRoles(ctx, writer)
}

// DatabaseRealmKeeping is [DatabaseRealm] that leaves the baseline in place.
//
// A baseline that names extensions or default privileges was captured through
// a writer that keeps them, so a writer that cannot is refused rather than
// allowed to remove them.
//
// A refusal of the server's default user database on a server the run does not
// own ends with the two ways to make it the run's.
func DatabaseRealmKeeping(ctx context.Context, conn *dbschema.DatabaseConnection, baseline Baseline) error {
	if conn == nil {
		return fmt.Errorf("clean dev database realm: nil database connection")
	}
	if keeper, ok := conn.SchemaWriter().(databaseRealmKeeper); ok {
		err := keeper.DropDatabaseRealmKeeping(ctx, baseline.kept())
		if errors.Is(err, dbreset.ErrServerDefaultDatabase) {
			return fmt.Errorf("%w; %s", err, devdocker.OwnedServerRemedy)
		}
		if err != nil {
			return err
		}
		return baseline.dropAddedRoles(ctx, conn.SchemaWriter())
	}
	if len(baseline.extensions) > 0 || len(baseline.schemas) > 0 || len(baseline.defaultPrivileges.Rows) > 0 ||
		len(baseline.artifacts) > 0 {
		return fmt.Errorf("clean dev database realm: this writer cannot keep the extensions, schemas, default privileges or database-scoped objects the database held before the run")
	}
	if err := DatabaseRealm(ctx, conn); err != nil {
		return err
	}
	return baseline.dropAddedRoles(ctx, conn.SchemaWriter())
}

// claimedServer is whose server conn is on, from the record
// [devdocker.RunOwned] reads, and on a server the run owns the roles it
// holds, read through writer; see [Baseline]. A writer that cannot drop the
// roles added since reads none, and its resets drop none.
func claimedServer(
	ctx context.Context, conn *dbschema.DatabaseConnection, writer catalog.SchemaWriter,
) (dbreset.Server, []string, error) {
	if !devdocker.RunOwned(conn.Info().URL) {
		return dbreset.NamedServer, nil, nil
	}
	keeper, ok := writer.(roleKeeper)
	if !ok {
		return dbreset.OwnedServer, nil, nil
	}
	roles, err := keeper.ServerRoles(ctx)
	return dbreset.OwnedServer, roles, err
}

// dropAddedSchemas drops the schemas beside the pinned one that the claim did
// not find, on a writer that can; see [Reset]. A database that returns to a
// starting point has been returned whole already, and a claim that did not
// list the schemas cannot tell which are the run's, so neither drops any.
func (b Baseline) dropAddedSchemas(ctx context.Context, writer catalog.SchemaWriter) error {
	dropper, ok := writer.(addedSchemaDropper)
	if !ok || !b.schemasRecorded || b.startingPoint != nil {
		return nil
	}
	return dropper.DropSchemasAddedSince(ctx, b.kept())
}

// dropAddedPublications drops the publications the database holds that the
// claim did not find, on a writer that can; see [Reset]. A publication belongs
// to the database rather than to the schema the URL pinned, so emptying the
// schema leaves it, and the next replay of the same declaration fails on its
// CREATE PUBLICATION. A database that returns to a starting point has been
// returned whole already, and a claim that did not list the database's
// objects cannot tell which publications are the run's, so neither drops any.
func (b Baseline) dropAddedPublications(ctx context.Context, writer catalog.SchemaWriter) error {
	dropper, ok := writer.(addedPublicationDropper)
	if !ok || !b.artifactsRecorded || b.startingPoint != nil {
		return nil
	}
	return dropper.DropPublicationsAddedSince(ctx, b.kept())
}

// dropAddedRoles drops the roles the server holds that it did not hold at the
// claim, once the reset has dropped the objects whose privileges name them;
// see [Baseline]. A baseline that holds no roles drops none.
func (b Baseline) dropAddedRoles(ctx context.Context, writer catalog.SchemaWriter) error {
	if b.roles == nil {
		return nil
	}
	keeper, ok := writer.(roleKeeper)
	if !ok {
		return fmt.Errorf("clean dev database: this writer cannot drop the roles the run created")
	}
	return keeper.DropRolesAddedSince(ctx, b.roles)
}

// kept is the baseline as a writer's reset reads it.
func (b Baseline) kept() dbreset.Kept {
	return dbreset.Kept{
		Extensions:        b.extensions,
		Schemas:           b.schemas,
		DefaultPrivileges: b.defaultPrivileges,
		Artifacts:         b.artifacts,
		ArtifactsRecorded: b.artifactsRecorded,
		Server:            b.server,
		StartingPoint:     b.startingPoint,
		SchemaStates:      b.schemaStates,

		DatabaseRoles:         b.databaseRoles,
		DatabaseRolesRecorded: b.databaseRolesRecorded,
	}
}
