// Package migratebaseline implements "ptah migrations baseline", which records
// existing migration files as already applied in the revision table without
// executing them.
package migratebaseline

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/cli/internal/migrateflags"
	"ptah.run/internal/cli/internal/migratelock"
	"ptah.run/internal/cli/internal/schemaops"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/devreach"
	"ptah.run/internal/migrationintegrity"
	"ptah.run/internal/migrationsnapshot"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/internal/undecidednote"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
	"ptah.run/migration/safety"
	"ptah.run/migration/shadow"
)

const (
	dbURLFlag       = "db-url"
	migrationsFlag  = "migrations-dir"
	versionFlag     = "version"
	forceFlag       = "force"
	dryRunFlag      = "dry-run"
	shadowDBFlag    = "shadow-db"
	rootDirFlag     = "root-dir"
	dirFormatFlag   = "dir-format"
	atlasEnvFlag    = "atlas-env"
	lockTimeoutFlag = "migration-lock-timeout"
)

type options struct {
	dbURL               string
	migrationsDir       string
	version             string
	force               bool
	dryRun              bool
	shadowDB            string
	rootDir             string
	dirFormat           string
	atlasEnv            string
	lockTimeout         string
	lockName            string
	connectTimeout      string
	migrationsSchema    string
	migrationsTable     string
	migrationsEngine    string
	revisionTableFormat string
	schemas             string
}

func NewMigrateBaselineCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "baseline",
		Short: "Record existing migrations as already applied",
		Long: `Record existing migrations as already applied without executing their SQL bodies.

Use this when adopting Ptah on a database whose schema already exists. Generate
the initial migration from an empty scratch database, verify the existing
database matches that migration, then baseline the existing database so future
ptah migrations up runs apply only new migrations.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return migrateBaselineCommand(cmd, args, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.StringVar(&opts.migrationsDir, migrationsFlag, "", "Directory containing migration files (required)")
	flags.StringVar(&opts.version, versionFlag, "", "Baseline version. Defaults to the highest version in --migrations-dir")
	flags.BoolVar(&opts.force, forceFlag, false, "Proceed despite existing migration metadata or verification drift")
	flags.BoolVar(&opts.dryRun, dryRunFlag, false, "Show the metadata rows that would be inserted without writing them")
	flags.StringVar(&opts.shadowDB, shadowDBFlag, "", "Disposable shadow database URL used to verify baselined migrations reproduce the target schema")
	flags.StringVar(&opts.rootDir, rootDirFlag, "./", "Root directory to scan for Go entities when --shadow-db is not set")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrationfile.DirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.StringVar(&opts.lockTimeout, lockTimeoutFlag, "", "Timeout for acquiring the session-level migration advisory lock, such as 10s or 2m; "+
		"refused against a dialect that takes no such lock")
	migratelock.RegisterNameFlag(flags, &opts.lockName)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterMigrationsEngineFlag(flags, &opts.migrationsEngine)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
	flags.StringVar(&opts.schemas, dbcli.SchemasFlagName, "",
		"Comma-separated database schemas to verify: PostgreSQL-family and SQL Server schemas, "+
			"or YDB directories relative to the database root. Empty reads each connection's default schema "+
			"with --shadow-db, and without it what ptah schema compare reads. "+dbcli.SchemasQuoting,
	)
	dbcli.RefuseEmptySchemasEnv(flags, dbcli.SchemasFlagName,
		"read each connection's default schema with --shadow-db, and without it what ptah schema compare reads")
}

func migrateBaselineCommand(cmd *cobra.Command, _ []string, opts *options) error {
	ctx := cmd.Context()
	integrityPolicy, err := migrationintegrity.Resolve()
	if err != nil {
		return err
	}
	schemas, err := dbcli.ParseSchemas(opts.schemas)
	if err != nil {
		return err
	}

	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	// `migrations baseline` reads no project config, so the requests reach it
	// from the command line or the PTAH_MIGRATION_LOCK_* variables only.
	lockRequest := migratelock.Requests{
		Timeout: migratelock.Request{
			Cmd:      cmd,
			FlagName: lockTimeoutFlag,
			DBURL:    opts.dbURL,
		},
		Name: migratelock.NameRequest(cmd, false, opts.dbURL),
	}
	lockName, err := migratelock.ResolveName(lockRequest.Name, opts.lockName)
	if err != nil {
		return err
	}
	if dialect, dialectErr := atlasurl.DialectFromURL(opts.dbURL); dialectErr == nil {
		if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
			return err
		}
	}
	if err := lockRequest.DecideFromURL(); err != nil {
		return err
	}
	if opts.migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}

	dirFormat, err := migrationfile.ParseDirFormat(opts.dirFormat)
	if err != nil {
		return err
	}
	// The shared integrity gate, before the shadow replay in verifyBaseline
	// executes the directory's migrations and before any revision row is
	// written. Baseline's whole output is a claim about which migrations a
	// database already contains; deriving that claim from files that do not
	// match the checksum recorded beside them records the wrong history under
	// the right version numbers.
	migrationsFS, err := migrationsnapshot.CaptureExistingDirectory(opts.migrationsDir)
	if err != nil {
		return fmt.Errorf("capture migration directory: %w", err)
	}
	if _, err := migrationintegrity.GateWithPolicy(
		cmd.Context(), cmd.ErrOrStderr(), migrationsFS, dirFormat, integrityPolicy, migrationintegrity.Options{},
	); err != nil {
		return err
	}
	revisionFormat, err := migrateflags.ParseRevisionTableFormat(opts.revisionTableFormat)
	if err != nil {
		return err
	}
	providerOpts := []migrator.FSProviderOption{
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrationfile.AtlasTemplateData{Env: opts.atlasEnv}),
	}
	provider, err := migrator.NewFSMigrationProvider(migrationsFS, providerOpts...)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	// The resolver reads the directory alone; it names a version as the
	// revision table records it and never connects.
	resolver := migrator.NewMigrator(nil, provider).WithRevisionTableFormat(revisionFormat)
	version, err := baselineVersion(opts.version, resolver, provider.Migrations())
	if err != nil {
		return err
	}
	rows := baselineRows(version, provider.Migrations())
	if len(rows) == 0 {
		return fmt.Errorf("no migrations found at or below baseline version %s", resolver.VersionLabel(version))
	}

	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}
	lockTimeout, err := migrateflags.ParseMigrationLockTimeout(opts.lockTimeout)
	if err != nil {
		return err
	}
	connectCtx, cancelConnect := dbcli.ConnectContext(ctx, connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	// Before the migrator is built, so no revision row is written under a lock
	// the operator asked for and the target never took.
	if err := lockRequest.DecideSession(conn); err != nil {
		return err
	}

	conn.SchemaWriter().SetDryRun(opts.dryRun)
	mig := migrator.NewMigrator(conn, provider).
		WithMigrationsTable(opts.migrationsSchema, opts.migrationsTable).
		WithMigrationsEngine(opts.migrationsEngine).
		WithRevisionTableFormat(revisionFormat).
		WithMigrationLockTimeout(lockTimeout).
		WithMigrationLockName(lockName.Value)
	if opts.dryRun {
		// A dry run verifies nothing, and the shadow database is opened all
		// the same, so a --shadow-db nothing answers on fails the dry run
		// rather than the run it rehearses. A docker:// URL is parsed and no
		// container is started.
		if _, err := devreach.Server(ctx, conn.Info().Dialect, shadowDBFlag, opts.shadowDB, connectTimeout); err != nil {
			return err
		}
		printDryRun(opts.dbURL, opts.migrationsDir, version, mig, lockName, rows)
		return nil
	}

	// A `docker://` shadow database is provisioned here rather than handed to
	// the connector, which answered `unsupported database dialect: docker`
	// (stokaro/ptah#1701). It comes after the dry-run return above, so a run
	// that prints a plan and stops never starts a container; a directly
	// connectable URL passes through untouched.
	shadowDB, releaseShadow, err := devdocker.Resolve(ctx, opts.shadowDB, devdocker.Options{})
	if err != nil {
		return err
	}
	defer releaseShadow()

	runtime, err := engineruntime.Resolve(cmd.Context())
	if err != nil {
		return err
	}
	if err := verifyBaseline(ctx, baselineVerifyOptions{
		runtime:        runtime,
		diagnostics:    cmd.ErrOrStderr(),
		dbURL:          opts.dbURL,
		shadowDB:       shadowDB,
		rootDir:        opts.rootDir,
		version:        version,
		force:          opts.force,
		conn:           conn,
		connectTimeout: connectTimeout,
		schemas:        schemas,
		migrationsDir:  opts.migrationsDir,
		migrationsFS:   migrationsFS,
		providerOpts:   providerOpts,
	}); err != nil {
		return err
	}
	if err := mig.BaselineWithOptions(ctx, migrator.BaselineOptions{Version: version, Force: opts.force}); err != nil {
		return err
	}

	fmt.Printf("Baselined %d migration(s) through version %s in %s\n",
		len(rows), resolver.VersionLabel(version), mig.MigrationsTableIdentifier())
	return nil
}

type baselineVerifyOptions struct {
	runtime schemaops.Runtime
	// diagnostics receives the warning for each declared object the entity
	// comparison could not check.
	diagnostics    io.Writer
	dbURL          string
	shadowDB       string
	rootDir        string
	version        int64
	force          bool
	conn           *dbschema.DatabaseConnection
	connectTimeout time.Duration
	schemas        []string
	migrationsDir  string
	migrationsFS   fs.FS
	providerOpts   []migrator.FSProviderOption
}

func verifyBaseline(ctx context.Context, opts baselineVerifyOptions) error {
	if err := schemaext.RequireRuntime(ctx, opts.runtime); err != nil {
		return err
	}
	handler := verificationErrorHandler{force: opts.force}
	if opts.shadowDB != "" {
		err := shadow.VerifyBaseline(ctx, shadow.BaselineVerifyOptions{
			Runtime:           opts.runtime,
			ShadowDatabaseURL: opts.shadowDB,
			TargetConn:        opts.conn,
			MigrationsDir:     opts.migrationsDir,
			MigrationsFS:      opts.migrationsFS,
			Version:           opts.version,
			Dialect:           opts.conn.Info().Dialect,
			Capabilities:      opts.conn.Info().Capabilities,
			Schemas:           opts.schemas,
			ProviderOptions:   opts.providerOpts,
			ConnectTimeout:    opts.connectTimeout,
		})
		handler.kind = "shadow"
		return handler.handle(err)
	}

	fmt.Println("No --shadow-db provided; using weaker entity drift verification.")
	result, err := schemaops.Compare(ctx, schemaops.CompareOptions{
		Runtime:        opts.runtime,
		RootDirs:       []string{opts.rootDir},
		DatabaseURL:    opts.dbURL,
		ConnectTimeout: opts.connectTimeout,
		Schemas:        opts.schemas,
	})
	if err != nil {
		return err
	}
	undecidednote.Report(ctx, opts.diagnostics, result.Undecided, "the database", "the entities")
	handler.kind = "drift"
	return handler.handle(entityDriftError(result))
}

// entityDriftError is the answer of the entity drift verification: nil when
// the database is shown to match the entities, and the reason otherwise.
//
// A declared object the read could not check is not shown to match. Recording
// a baseline says the database already holds what the migrations up to it
// create, so it is refused on one, as on a difference, and --force records it
// anyway.
func entityDriftError(result *schemaops.CompareResult) error {
	if result.Diff.HasChanges() {
		findings := safety.ClassifySchemaDiff(result.Diff)
		return fmt.Errorf("baseline drift verification failed: schema drift detected; findings: %v", findings)
	}
	if !result.Undecided.Empty() {
		return fmt.Errorf("baseline drift verification failed: %s; see the warnings above",
			undecidednote.Summary(result.Undecided))
	}
	return nil
}

type verificationErrorHandler struct {
	kind  string
	force bool
}

func (h verificationErrorHandler) handle(err error) error {
	if err == nil {
		return nil
	}
	if !h.force {
		return err
	}
	fmt.Printf("WARNING: %s verification failed but --force was set: %v\n", h.kind, err)
	return nil
}

// baselineVersion reads --version as the directory's file names spell it, so
// --version v1 names v1_x.sql and the order key it runs under is refused (see
// [migrator.Migrator.ResolveVersion]). Without the flag the baseline is the
// highest migration.
func baselineVersion(value string, resolver *migrator.Migrator, migrations []*migrator.Migration) (int64, error) {
	if value != "" {
		version, err := resolver.ResolveVersion(value)
		if err != nil {
			return 0, fmt.Errorf("invalid baseline version %q: %w", value, err)
		}
		if version <= 0 {
			return 0, fmt.Errorf("invalid baseline version %q", value)
		}
		return version, nil
	}
	var version int64
	for _, migration := range migrations {
		if migration.Version > version {
			version = migration.Version
		}
	}
	if version == 0 {
		return 0, fmt.Errorf("migrations directory contains no migrations")
	}
	return version, nil
}

func baselineRows(version int64, migrations []*migrator.Migration) []*migrator.Migration {
	rows := make([]*migrator.Migration, 0, len(migrations))
	for _, migration := range migrations {
		if migration.Version <= version {
			rows = append(rows, migration)
		}
	}
	return rows
}

func printDryRun(
	dbURL,
	migrationsDir string,
	version int64,
	mig *migrator.Migrator,
	lockName migratelock.Name,
	rows []*migrator.Migration,
) {
	fmt.Println("=== DRY RUN BASELINE ===")
	fmt.Println("No metadata rows will be written.")
	fmt.Printf("Database: %s\n", dburldisplay.Format(dbURL))
	fmt.Printf("Migrations directory: %s\n", migrationsDir)
	fmt.Printf("Metadata table: %s\n", mig.MigrationsTableIdentifier())
	if lockName.Explicit() {
		fmt.Printf("Migration lock: %s\n", lockName.Value)
	}
	// The versions the files spell, as the run that writes the rows names
	// them: in an Atlas directory keyed by file position the order key is a
	// position, and 00_x.sql runs under 1.
	fmt.Printf("Baseline version: %s\n", mig.VersionLabel(version))
	fmt.Println("Rows:")
	for _, migration := range rows {
		fmt.Printf("- version=%s description=%q\n", mig.VersionLabel(migration.Version), migration.Description)
	}
}
