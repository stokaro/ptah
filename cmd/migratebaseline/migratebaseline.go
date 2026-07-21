package migratebaseline

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/schemaops"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/generator"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/safety"
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
	connectTimeout      string
	migrationsSchema    string
	migrationsTable     string
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
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.atlasEnv, atlasEnvFlag, "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	flags.StringVar(&opts.lockTimeout, lockTimeoutFlag, "", "Timeout for acquiring the session-level migration advisory lock, such as 10s or 2m")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
}

func migrateBaselineCommand(cmd *cobra.Command, _ []string, opts *options) error {
	ctx := cmd.Context()
	schemas := dbcli.ParseSchemas(opts.schemas)

	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if opts.migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(opts.dirFormat)
	if err != nil {
		return err
	}
	revisionFormat, err := migrator.ParseRevisionTableFormat(opts.revisionTableFormat)
	if err != nil {
		return err
	}
	providerOpts := []migrator.FSProviderOption{
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: opts.atlasEnv}),
	}
	provider, err := migrator.NewFSMigrationProvider(os.DirFS(opts.migrationsDir), providerOpts...)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	version, err := baselineVersion(opts.version, provider.Migrations())
	if err != nil {
		return err
	}
	rows := baselineRows(version, provider.Migrations())
	if len(rows) == 0 {
		return fmt.Errorf("no migrations found at or below baseline version %d", version)
	}

	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}
	lockTimeout, err := migrator.ParseMigrationLockTimeout(opts.lockTimeout)
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

	conn.SchemaWriter().SetDryRun(opts.dryRun)
	mig := migrator.NewMigrator(conn, provider).
		WithMigrationsTable(opts.migrationsSchema, opts.migrationsTable).
		WithRevisionTableFormat(revisionFormat).
		WithMigrationLockTimeout(lockTimeout)
	if opts.dryRun {
		printDryRun(opts.dbURL, opts.migrationsDir, version, mig, rows)
		return nil
	}

	if err := verifyBaseline(ctx, baselineVerifyOptions{
		dbURL:          opts.dbURL,
		shadowDB:       opts.shadowDB,
		rootDir:        opts.rootDir,
		version:        version,
		force:          opts.force,
		conn:           conn,
		connectTimeout: connectTimeout,
		schemas:        schemas,
		migrationsDir:  opts.migrationsDir,
		providerOpts:   providerOpts,
	}); err != nil {
		return err
	}
	if err := mig.BaselineWithOptions(ctx, migrator.BaselineOptions{Version: version, Force: opts.force}); err != nil {
		return err
	}

	fmt.Printf("Baselined %d migration(s) through version %d in %s\n", len(rows), version, mig.MigrationsTableIdentifier())
	return nil
}

type baselineVerifyOptions struct {
	dbURL          string
	shadowDB       string
	rootDir        string
	version        int64
	force          bool
	conn           *dbschema.DatabaseConnection
	connectTimeout time.Duration
	schemas        []string
	migrationsDir  string
	providerOpts   []migrator.FSProviderOption
}

func verifyBaseline(ctx context.Context, opts baselineVerifyOptions) error {
	handler := verificationErrorHandler{force: opts.force}
	if opts.shadowDB != "" {
		err := generator.VerifyBaselineShadow(ctx, generator.BaselineShadowVerifyOptions{
			ShadowDatabaseURL: opts.shadowDB,
			TargetConn:        opts.conn,
			MigrationsDir:     opts.migrationsDir,
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
		RootDir:        opts.rootDir,
		DatabaseURL:    opts.dbURL,
		ConnectTimeout: opts.connectTimeout,
		Schemas:        opts.schemas,
	})
	if err != nil {
		return err
	}
	if !result.Diff.HasChanges() {
		return nil
	}
	findings := safety.ClassifySchemaDiff(result.Diff)
	err = fmt.Errorf("baseline drift verification failed: schema drift detected; findings: %v", findings)
	handler.kind = "drift"
	return handler.handle(err)
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

func baselineVersion(value string, migrations []*migrator.Migration) (int64, error) {
	if value != "" {
		version, err := strconv.ParseInt(value, 10, 64)
		if err != nil || version <= 0 {
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
	dbURL string,
	migrationsDir string,
	version int64,
	mig *migrator.Migrator,
	rows []*migrator.Migration,
) {
	fmt.Println("=== DRY RUN BASELINE ===")
	fmt.Println("No metadata rows will be written.")
	fmt.Printf("Database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	fmt.Printf("Migrations directory: %s\n", migrationsDir)
	fmt.Printf("Metadata table: %s\n", mig.MigrationsTableIdentifier())
	fmt.Printf("Baseline version: %d\n", version)
	fmt.Println("Rows:")
	for _, migration := range rows {
		fmt.Printf("- version=%d description=%q\n", migration.Version, migration.Description)
	}
}
