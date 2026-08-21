package generator

import (
	"context"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"time"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/devlock"
	"go.5x5.cz/ptah/migration/migrator"
)

// DynamicRollbackOptions configures PlanDynamicRollback.
type DynamicRollbackOptions struct {
	// TargetConnection is the live database the plan would be applied to. It is
	// read, never written, by planning.
	TargetConnection *dbschema.DatabaseConnection
	// DevDatabaseURL is an ephemeral database the planner drops clean and
	// replays the migration directory into, to materialize the target state.
	// Its contents are discarded.
	DevDatabaseURL string
	// FS is the migration filesystem the target state is replayed from.
	FS fs.FS
	// TargetVersion is the version the database is being rolled back to. The
	// dev database is migrated up to exactly this version, and the schema it
	// then holds is what the plan drives the live database towards.
	TargetVersion int64
	// ProviderOptions are passed to the migration provider (e.g. dir format).
	ProviderOptions []migrator.FSProviderOption
	// ConnectTimeout bounds the dev database connection attempt.
	ConnectTimeout time.Duration
	// RevisionsTable names the revision table this run was configured with, so
	// a non-default one is excluded from the comparison alongside the defaults.
	RevisionsTable string
}

// PlanDynamicRollback computes the statements that take the live database back
// to the schema the migration directory describes at TargetVersion, without
// reading a single down file.
//
// # What "dynamic" means here
//
// The ordinary rollback path runs the down bodies the author wrote. This one
// derives the plan instead: the dev database is dropped clean and migrated up
// to TargetVersion, so it holds exactly the schema that version defines; the
// live database is read as it currently stands; and the difference between them
// is the plan. A migration with no down body, or one whose down body was never
// written, is therefore revertible -- which is the reason to want this -- and a
// down body that disagrees with its up body is bypassed rather than trusted.
//
// # What it costs
//
// The plan is derived from schema structure, so it carries no knowledge of what
// the author meant. A down body can preserve data that a derived DROP will not,
// and it can order operations in a way a structural diff has no reason to
// choose. This is why it is opt-in and why the ordinary path stays the default:
// a reviewed down file is a statement of intent, and a derived plan is an
// inference about structure (stokaro/ptah#1621).
//
// # Why a dev database is required
//
// The target state exists nowhere else. It is not the live database, which is
// what is being changed, and it is not any file in the directory, since a
// version's schema is the accumulation of every migration up to it. The only
// way to see it is to build it, and the only safe place to build it is a
// database whose contents can be destroyed.
func PlanDynamicRollback(ctx context.Context, opts DynamicRollbackOptions) ([]string, error) {
	if opts.TargetConnection == nil {
		return nil, fmt.Errorf("dynamic rollback planning failed: a target database connection is required")
	}
	if opts.DevDatabaseURL == "" {
		return nil, fmt.Errorf(
			"dynamic rollback planning failed: a dev database URL is required to materialize the target schema")
	}
	if opts.FS == nil {
		return nil, fmt.Errorf("dynamic rollback planning failed: a migration filesystem is required")
	}
	devConn, err := openDistinctDevDatabase(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer dbschema.CloseAndWarn(devConn)

	// Build the target state: a clean database carrying exactly what the
	// directory defines at TargetVersion.
	if err := devConn.SchemaWriter().DropAllTables(ctx); err != nil {
		return nil, fmt.Errorf("dynamic rollback planning failed: drop all objects: %w", err)
	}
	mig, err := migrator.NewFSMigrator(devConn, opts.FS, opts.ProviderOptions...)
	if err != nil {
		return nil, fmt.Errorf("dynamic rollback planning failed: register migrations: %w", err)
	}
	if err := mig.MigrateTo(ctx, opts.TargetVersion); err != nil {
		if description := describeReplayError(err); description != "" {
			return nil, fmt.Errorf("dynamic rollback planning failed: replay migrations: %s", description)
		}
		return nil, fmt.Errorf("dynamic rollback planning failed: replay migrations: %w", err)
	}

	// The comparison goes through the same path `schema diff` uses, with both
	// sides as live databases: the target one as it stands, and the dev one now
	// holding exactly the target version's schema.
	//
	// Comparing them by hand was tried and was wrong. Converting the dev
	// database to a declaration and diffing that against the live catalog
	// reported every table as changed -- the round trip does not reproduce what
	// the comparator expects of an authored side, so a rollback of one table
	// planned a rebuild of all of them. Two database sources avoid the
	// conversion entirely, and this path already knows how to compare them.
	diff, err := atlasschema.Diff(ctx, atlasschema.DiffOptions{
		FromURLs: []string{opts.TargetConnection.Info().URL},
		ToURLs:   []string{opts.DevDatabaseURL},
		// Both sides are databases, so no third database is needed to
		// materialize either one.
		Exclude:        migrationMetadataExclusions(opts.RevisionsTable),
		ConnectTimeout: opts.ConnectTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("dynamic rollback planning failed: compare the schemas: %w", err)
	}
	statements := make([]string, 0, len(diff.Changes))
	for _, change := range diff.Changes {
		statements = append(statements, change.Cmd)
	}
	return statements, nil
}

// openDistinctDevDatabase connects to the dev database and refuses one that
// could be the target.
//
// The checks are the same ones VerifyRollbackFromShadow makes, and for a
// stronger reason: that path replays into the dev database, while this one then
// applies a DROP-heavy plan to whatever the target turns out to be. A dev URL
// that resolves to the live database would compute a plan against itself and
// then destroy it.
func openDistinctDevDatabase(
	ctx context.Context,
	opts DynamicRollbackOptions,
) (*dbschema.DatabaseConnection, error) {
	sameDatabase, err := atlasurl.MayAddressSameDatabase(
		opts.TargetConnection.Info().URL, opts.DevDatabaseURL)
	if err != nil {
		return nil, fmt.Errorf(
			"dynamic rollback planning failed: compare target and dev databases: %w", err)
	}
	if sameDatabase {
		return nil, fmt.Errorf(
			"dynamic rollback planning failed: dev database must be distinct from target database")
	}
	connectCtx, cancelConnect := baselineShadowConnectContext(ctx, opts.ConnectTimeout)
	devConn, err := dbschema.ConnectToDatabase(connectCtx, opts.DevDatabaseURL)
	cancelConnect()
	if err != nil {
		return nil, fmt.Errorf("dynamic rollback planning failed: connect to dev database: %w", err)
	}
	if !sameDialect(opts.TargetConnection.Info().Dialect, devConn.Info().Dialect) {
		dialect := devConn.Info().Dialect
		dbschema.CloseAndWarn(devConn)
		return nil, fmt.Errorf(
			"dynamic rollback planning failed: dev database dialect %q does not match target dialect %q",
			dialect, opts.TargetConnection.Info().Dialect)
	}
	sameDatabase, err = devlock.SameRealm(ctx, opts.TargetConnection, devConn)
	if err != nil {
		dbschema.CloseAndWarn(devConn)
		return nil, fmt.Errorf(
			"dynamic rollback planning failed: compare live target and dev database realms: %w", err)
	}
	if sameDatabase {
		dbschema.CloseAndWarn(devConn)
		return nil, fmt.Errorf(
			"dynamic rollback planning failed: dev database must be distinct from target database")
	}
	return devConn, nil
}

// migrationMetadataExclusions keeps Ptah's own bookkeeping out of the
// comparison.
//
// This is not tidiness, it is the difference between a rollback and a
// catastrophe. The two sides record migrations under different table names --
// the live database uses whichever revision layout it was written with, while
// the dev replay writes the layout this run configured -- so a table one side
// has and the other does not looks exactly like a table the rollback should
// drop. Measured before this existed: rolling back through the Atlas-compatible
// surface derived `DROP TABLE IF EXISTS "atlas_schema_revisions"`, applied it,
// and reported success, having destroyed the record of everything that had ever
// been applied.
//
// Both default layouts and the tag namespace are named, plus whatever this run
// configured, because the plan is computed against what the live database
// happens to carry rather than against what this invocation expected to find.
func migrationMetadataExclusions(configuredTable string) []string {
	exclusions := []string{
		"schema_migrations",
		"atlas_schema_revisions",
		"ptah_migration_tags",
	}
	if trimmed := strings.TrimSpace(configuredTable); trimmed != "" &&
		!slices.Contains(exclusions, trimmed) {
		exclusions = append(exclusions, trimmed)
	}
	return exclusions
}
