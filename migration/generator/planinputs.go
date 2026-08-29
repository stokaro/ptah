package generator

// What a plan is built FROM: the connection, the desired schema, and the
// options normalized before either is touched.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/diffpolicy"
	"go.5x5.cz/ptah/migration/safety"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/shadow"
)

func verifyPlannedShadowMigration(
	ctx context.Context,
	opts GenerateMigrationOptions,
	conn *dbschema.DatabaseConnection,
	info catalog.ServerInfo,
	diff *difftypes.SchemaDiff,
	specs []generatedMigrationSpec,
	desired *schemamodel.Database,
) error {
	if opts.ShadowDatabaseURL == "" {
		return nil
	}
	return shadow.VerifyMigration(ctx, shadow.MigrationVerifyOptions{
		ShadowDatabaseURL: opts.ShadowDatabaseURL,
		TargetConnection:  conn,
		MigrationsDir:     opts.OutputDir,
		MigrationsFS:      opts.PriorMigrationsFS,
		Dialect:           info.Dialect,
		Capabilities:      info.Capabilities,
		IdentifierSemantics: cloneIdentifierSemanticsValue(
			diff.IdentifierSemantics,
		),
		Candidates:  shadowCandidatesFromSpecs(specs),
		Generated:   desired,
		CompareOpts: opts.CompareOptions,
		Schemas:     opts.Schemas,
	})
}

func resolvePlanDatabaseConnection(
	ctx context.Context,
	opts GenerateMigrationOptions,
) (*dbschema.DatabaseConnection, bool, error) {
	if opts.DBConn != nil {
		return opts.DBConn, false, nil
	}
	// The connect budget is spent here and released here. Deferring the cancel
	// in this function rather than in the caller is what keeps it off the rest
	// of the generation, which is the whole distinction the field exists to
	// draw.
	connectCtx, cancelConnect := connectContextFor(ctx, opts)
	defer cancelConnect()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.DatabaseURL)
	if err != nil {
		return nil, false, fmt.Errorf("error connecting to database: %w", err)
	}
	return conn, true, nil
}

// connectContextFor derives the context the connect runs under.
//
// Split from its caller so the derivation can be checked without a server and
// without a clock: a test asks whether the returned context carries a deadline,
// which is decided when it is built. Asserting that a small budget *fires*
// would depend on timer granularity -- on Windows `time.Now()` can return the
// same instant either side of a nanosecond budget, so the deadline is scheduled
// rather than already past and a local connect finishes first
// (stokaro/ptah#1749).
func connectContextFor(
	ctx context.Context,
	opts GenerateMigrationOptions,
) (context.Context, context.CancelFunc) {
	return connectContext(ctx, opts.ConnectTimeout)
}

// connectContext bounds a connect attempt by the configured budget, and leaves
// the context unbounded when no budget was set. The cancel function is returned
// in both cases, so every caller releases the context the same way.
func connectContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

// resolveDesiredSchema answers what the migration should bring the database to:
// a pre-merged schema when the caller assembled one from several sources, and
// otherwise the Go entities directory, parsed through a filesystem rooted at its
// parent so the scan cannot walk out of the directory the caller named.
func resolveDesiredSchema(opts GenerateMigrationOptions) (*schemamodel.Database, error) {
	if opts.Generated != nil {
		return opts.Generated, nil
	}
	entitiesFS := opts.GoEntitiesFS
	entitiesDir := opts.GoEntitiesDir
	if entitiesFS == nil {
		absPath, err := filepath.Abs(opts.GoEntitiesDir)
		if err != nil {
			return nil, fmt.Errorf("error resolving root directory path: %w", err)
		}
		entitiesFS = os.DirFS(filepath.Dir(absPath))
		entitiesDir = filepath.Base(absPath)
	}
	desired, err := goschema.ParseFS(entitiesFS, entitiesDir)
	if err != nil {
		return nil, fmt.Errorf("error parsing Go entities: %w", err)
	}
	return desired, nil
}

func normalizeGenerateMigrationOptions(opts GenerateMigrationOptions) (GenerateMigrationOptions, error) {
	if opts.MigrationName == "" {
		opts.MigrationName = "migration"
	}
	dialect := ""
	if opts.DBConn != nil {
		dialect = opts.DBConn.Info().Dialect
	} else if resolvedDialect, dialectErr := atlasurl.DialectFromURL(opts.DatabaseURL); dialectErr == nil {
		dialect = resolvedDialect
	}
	if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
		return opts, err
	}
	outputDir, err := pathguard.ResolveWithinRoot(opts.OutputDir, opts.AllowedOutputRoot)
	if err != nil {
		return opts, fmt.Errorf("error validating output directory: %w", err)
	}
	opts.OutputDir = outputDir
	return opts, nil
}

func checkDestructiveAllowed(opts GenerateMigrationOptions, assessments []safety.StatementAssessment) error {
	if opts.CheckDestructive && safety.HasDestructiveAssessment(assessments) && !opts.AllowDestructive {
		return fmt.Errorf("destructive migration statements require AllowDestructive")
	}
	return nil
}

// compareOptionsWithDiffPolicy tells the comparison what
// [planGeneratedMigrationSpecs] will do to its answer.
//
// The SQLite virtual-table guard runs inside the comparison and refuses on the
// statements it predicts, while the skip filter that deletes those statements
// runs afterwards, here. Without this the comparison refused a plan the policy
// had already emptied (stokaro/ptah#1028). The caller's options are copied
// rather than written through: GenerateMigrationOptions is a value the caller
// may reuse for another run.
//
// Every skip kind the guard can read is forwarded, not only the table drop.
// `drop_column` empties a table diff's ColumnsRemoved, which is one of the
// fields the rebuild predicate reads, and `drop_index` empties the standalone
// index removals the guard counts the owning table for; forwarding one of the
// three left the other two refusing plans this function had already emptied.
// diffpolicy.DropEnum is the one kind deliberately not forwarded: SQLite has no
// enum type and the guard reads no enum field, and the census in
// internal/sqlitevirtual fails if a new kind appears unclassified.
func compareOptionsWithDiffPolicy(
	opts *config.CompareOptions,
	policy DiffPolicy,
) *config.CompareOptions {
	merged := config.DefaultCompareOptions()
	if opts != nil {
		*merged = *opts
		merged.IgnoredExtensions = slices.Clone(opts.IgnoredExtensions)
	}
	merged.SkipTableDrops = slices.Contains(policy.SkipChangeKinds, diffpolicy.DropTable)
	merged.SkipColumnDrops = slices.Contains(policy.SkipChangeKinds, diffpolicy.DropColumn)
	merged.SkipIndexDrops = slices.Contains(policy.SkipChangeKinds, diffpolicy.DropIndex)
	return merged
}
