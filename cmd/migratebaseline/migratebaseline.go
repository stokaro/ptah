package migratebaseline

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

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

func NewMigrateBaselineCommand() *cobra.Command {
	flags := newMigrateBaselineFlags()
	cmd := &cobra.Command{
		Use:   "migrate-baseline",
		Short: "Record existing migrations as already applied",
		Long: `Record existing migrations as already applied without executing their SQL bodies.

Use this when adopting Ptah on a database whose schema already exists. Generate
the initial migration from an empty scratch database, verify the existing
database matches that migration, then baseline the existing database so future
migrate-up runs apply only new migrations.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return migrateBaselineCommand(cmd, args, flags)
		},
	}
	cobraflags.RegisterMap(cmd, flags)
	return cmd
}

func newMigrateBaselineFlags() map[string]cobraflags.Flag {
	return map[string]cobraflags.Flag{
		dbURLFlag: &cobraflags.StringFlag{
			Name:  dbURLFlag,
			Value: "",
			Usage: "Database URL (required). Example: postgres://localhost:5432/dbname",
		},
		migrationsFlag: &cobraflags.StringFlag{
			Name:  migrationsFlag,
			Value: "",
			Usage: "Directory containing migration files (required)",
		},
		versionFlag: &cobraflags.StringFlag{
			Name:  versionFlag,
			Value: "",
			Usage: "Baseline version. Defaults to the highest version in --migrations-dir",
		},
		forceFlag: &cobraflags.BoolFlag{
			Name:  forceFlag,
			Value: false,
			Usage: "Proceed despite existing migration metadata or verification drift",
		},
		dryRunFlag: &cobraflags.BoolFlag{
			Name:  dryRunFlag,
			Value: false,
			Usage: "Show the metadata rows that would be inserted without writing them",
		},
		shadowDBFlag: &cobraflags.StringFlag{
			Name:  shadowDBFlag,
			Value: "",
			Usage: "Disposable shadow database URL used to verify baselined migrations reproduce the target schema",
		},
		rootDirFlag: &cobraflags.StringFlag{
			Name:  rootDirFlag,
			Value: "./",
			Usage: "Root directory to scan for Go entities when --shadow-db is not set",
		},
		dirFormatFlag: &cobraflags.StringFlag{
			Name:  dirFormatFlag,
			Value: string(migrator.MigrationDirFormatAuto),
			Usage: "Migration directory format: auto, ptah, or atlas",
		},
		atlasEnvFlag: &cobraflags.StringFlag{
			Name:  atlasEnvFlag,
			Value: "",
			Usage: "Value exposed as .Env when rendering Atlas SQL template migrations",
		},
		lockTimeoutFlag: &cobraflags.StringFlag{
			Name:  lockTimeoutFlag,
			Value: "",
			Usage: "Timeout for acquiring the session-level migration advisory lock, such as 10s or 2m",
		},
		dbcli.ConnectTimeoutFlagName:      dbcli.NewConnectTimeoutFlag(),
		dbcli.MigrationsSchemaFlagName:    dbcli.NewMigrationsSchemaFlag(),
		dbcli.MigrationsTableFlagName:     dbcli.NewMigrationsTableFlag(),
		dbcli.RevisionTableFormatFlagName: dbcli.NewRevisionTableFormatFlag(),
		dbcli.SchemasFlagName:             dbcli.NewSchemasFlag(),
	}
}

func migrateBaselineCommand(cmd *cobra.Command, _ []string, flags map[string]cobraflags.Flag) error {
	ctx := cmd.Context()
	dbURL := flags[dbURLFlag].GetString()
	migrationsDir := flags[migrationsFlag].GetString()
	versionValue := flags[versionFlag].GetString()
	force := flags[forceFlag].GetBool()
	dryRun := flags[dryRunFlag].GetBool()
	shadowDB := flags[shadowDBFlag].GetString()
	rootDir := flags[rootDirFlag].GetString()
	dirFormatValue := flags[dirFormatFlag].GetString()
	atlasEnv := flags[atlasEnvFlag].GetString()
	lockTimeoutValue := flags[lockTimeoutFlag].GetString()
	migrationsSchema := flags[dbcli.MigrationsSchemaFlagName].GetString()
	migrationsTable := flags[dbcli.MigrationsTableFlagName].GetString()
	revisionFormatValue := flags[dbcli.RevisionTableFormatFlagName].GetString()
	schemas := dbcli.ParseSchemas(flags[dbcli.SchemasFlagName].GetString())

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(dirFormatValue)
	if err != nil {
		return err
	}
	revisionFormat, err := migrator.ParseRevisionTableFormat(revisionFormatValue)
	if err != nil {
		return err
	}
	providerOpts := []migrator.FSProviderOption{
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: atlasEnv}),
	}
	provider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir), providerOpts...)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	version, err := baselineVersion(versionValue, provider.Migrations())
	if err != nil {
		return err
	}
	rows := baselineRows(version, provider.Migrations())
	if len(rows) == 0 {
		return fmt.Errorf("no migrations found at or below baseline version %d", version)
	}

	connectTimeout, err := dbcli.ParseConnectTimeout(flags[dbcli.ConnectTimeoutFlagName].GetString())
	if err != nil {
		return err
	}
	lockTimeout, err := migrator.ParseMigrationLockTimeout(lockTimeoutValue)
	if err != nil {
		return err
	}
	connectCtx, cancelConnect := dbcli.ConnectContext(ctx, connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	conn.Writer().SetDryRun(dryRun)
	mig := migrator.NewMigrator(conn, provider).
		WithMigrationsTable(migrationsSchema, migrationsTable).
		WithRevisionTableFormat(revisionFormat).
		WithMigrationLockTimeout(lockTimeout)
	if dryRun {
		printDryRun(dbURL, migrationsDir, version, mig, rows)
		return nil
	}

	if err := verifyBaseline(ctx, baselineVerifyOptions{
		dbURL:          dbURL,
		shadowDB:       shadowDB,
		rootDir:        rootDir,
		version:        version,
		force:          force,
		conn:           conn,
		connectTimeout: connectTimeout,
		schemas:        schemas,
		migrationsDir:  migrationsDir,
		providerOpts:   providerOpts,
	}); err != nil {
		return err
	}
	if err := mig.BaselineWithOptions(ctx, migrator.BaselineOptions{Version: version, Force: force}); err != nil {
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
