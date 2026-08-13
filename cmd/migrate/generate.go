package migrate

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/schemasource"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/migrationintegrity"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/schemaload"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	generateRootDirFlag          = "root-dir"
	generateSchemaFileFlag       = "schema-file"
	generateSchemaCmdFlag        = "schema-cmd"
	generateSchemaFormatFlag     = "schema-format"
	generateDBURLFlag            = "db-url"
	generateMigrationsDirFlag    = "migrations-dir"
	generateNameFlag             = "name"
	generateShadowDBFlag         = "shadow-db"
	generateCheckDestructiveFlag = "check-destructive"
	generateAllowDestructiveFlag = "allow-destructive"
	generateReportFormatFlag     = "report"
	generateReplayFlag           = "replay"
	generateDevURLFlag           = "dev-url"
	generateDirFormatFlag        = "dir-format"
	generateQualifierFlag        = "qualifier"
)

func NewMigrateGenerateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate migration files from schema differences",
		Long: `Generate migration files by comparing Go entities with the current database schema.

When --shadow-db is set, or migrate.generate.shadow_db is configured in ptah.yaml, Ptah verifies
the generated candidate on the shadow database before writing files:
it drops all shadow objects, replays existing migrations, applies the candidate, re-introspects the schema,
and performs an up/down/up round-trip.

With --replay, the current state is derived without any access to the target
database: the existing migration directory is replayed on the disposable
--dev-url database (which is reset destructively first), and the next
migration is generated from the difference between that replayed state and
the desired schema sources. This lets CI generate the next migration from the
repository alone.`,
		RunE: migrateGenerateCommand,
	}

	flags := cmd.Flags()
	flags.StringArray(generateRootDirFlag, nil, "Root directory to scan for Go entities (repeatable; multiple roots merge into one composite schema; defaults to ./)")
	flags.StringArray(generateSchemaFileFlag, nil, "YAML, HCL, or SQL schema file to generate a migration toward instead of, or combined with, Go entities (repeatable; multiple sources merge into one composite schema)")
	flags.String(generateSchemaCmdFlag, "", `External program whose stdout is the desired schema; run without a shell, split on whitespace. Example: "go run ./loader"`)
	flags.String(generateSchemaFormatFlag, "sql", "Format of the --schema-cmd output: sql, hcl, or yaml")
	flags.String(generateDBURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.String(generateMigrationsDirFlag, "", "Directory containing existing migrations and receiving generated files (required)")
	flags.String(generateNameFlag, "migration", "Migration name")
	flags.String(generateShadowDBFlag, "", "Shadow database URL used to verify generated migrations before writing files")
	flags.Bool(generateReplayFlag, false, "Derive the current state by replaying --migrations-dir on the --dev-url database instead of introspecting --db-url")
	flags.String(generateDevURLFlag, "", "Disposable dev database URL the migration directory is replayed on with --replay; it is reset destructively")
	flags.String(generateDirFormatFlag, string(migrator.MigrationDirFormatAuto), "Migration directory format used by --replay: auto, ptah, or atlas")
	flags.String(generateQualifierFlag, "", "Qualify every object in the generated statements with a custom schema qualifier (single-schema plans only)")
	flags.Bool(generateCheckDestructiveFlag, false, "Fail when generated migration SQL contains destructive statements")
	flags.Bool(generateAllowDestructiveFlag, false, "Allow destructive statements when --check-destructive is set")
	flags.String(generateReportFormatFlag, "", `Safety report format next to the migration files: "", html, or json`)
	dbcli.RegisterPlainHTTPFlagValue(flags)
	flags.String(dbcli.ConfigFlagName, "", "Path to a ptah.yaml config file (default: ./ptah.yaml when present)")
	flags.String(dbcli.ConnectTimeoutFlagName, dbcli.DefaultConnectTimeout.String(), "Initial database connection timeout")
	dbcli.RegisterProjectEnvFlag(flags)
	flags.String(dbcli.SchemasFlagName, "", "Comma-separated schemas to introspect when supported")
	dbcli.RegisterExternalSchemaOptInFlag(flags)

	cmdutil.ConfigureCommand(cmd)
	return cmd
}

// validateGenerateReplayMode rejects the target database URL in --replay mode,
// so the two current-state sources can never silently mix.
func validateGenerateReplayMode(cmd *cobra.Command) error {
	if cmd.Flags().Changed(generateDBURLFlag) {
		return fmt.Errorf("--%s cannot be combined with --%s: the current state is derived by replaying the migration directory on --%s",
			generateDBURLFlag, generateReplayFlag, generateDevURLFlag)
	}
	return nil
}

// validateGenerateIntrospectMode rejects replay-only flags outside --replay
// mode.
func validateGenerateIntrospectMode(cmd *cobra.Command) error {
	for _, flag := range []string{generateDevURLFlag, generateDirFormatFlag} {
		if cmd.Flags().Changed(flag) {
			return fmt.Errorf("--%s requires --%s", flag, generateReplayFlag)
		}
	}
	return nil
}

type generatePriorMigrationOptions struct {
	migrationsDir string
	dirFormat     string
	shadowDB      string
	replay        bool
	policy        migrationintegrity.Policy
}

func captureGeneratePriorMigrations(
	ctx context.Context,
	notice io.Writer,
	opts generatePriorMigrationOptions,
) (fs.FS, migrator.MigrationDirFormat, error) {
	dirFormat := migrator.MigrationDirFormatPtah
	if opts.replay {
		parsed, err := migrator.ParseMigrationDirFormat(opts.dirFormat)
		if err != nil {
			return nil, "", err
		}
		dirFormat = parsed
	}
	if opts.replay || strings.TrimSpace(opts.shadowDB) == "" {
		return nil, dirFormat, nil
	}
	priorSnapshot, err := recoverAndCaptureGeneratePriorMigrations(
		ctx,
		notice,
		opts.migrationsDir,
		dirFormat,
		opts.policy,
	)
	return priorSnapshot, dirFormat, err
}

// recoverAndCaptureGeneratePriorMigrations settles an interrupted publication
// and captures the authorized snapshot under one directory lock.
//
// The order is the whole point, and it is the same order the replay path holds
// under its own lock. An interrupted publication leaves artifacts the recorded
// checksum does not cover, so a gate that ran first would refuse a directory
// the recovery journal was about to settle — and would refuse it on every
// retry, because the journal is only ever processed further along the run that
// the refusal ends. Recovery first, then a snapshot of what recovery left, then
// the gate over exactly those bytes.
func recoverAndCaptureGeneratePriorMigrations(
	ctx context.Context,
	notice io.Writer,
	migrationsDir string,
	dirFormat migrator.MigrationDirFormat,
	policy migrationintegrity.Policy,
) (fs.FS, error) {
	var priorSnapshot fs.FS
	err := atlasmigrate.WithMigrationDirectoryLock(ctx, migrationsDir, 0, func(context.Context) error {
		if err := atlasmigrate.RecoverPendingPublicationLocked(migrationsDir); err != nil {
			return fmt.Errorf("recover migration publication before shadow verification: %w", err)
		}
		captured, captureErr := captureAndAuthorizeGeneratePriorMigrations(
			notice,
			migrationsDir,
			dirFormat,
			policy,
		)
		priorSnapshot = captured
		return captureErr
	})
	return priorSnapshot, err
}

func captureAndAuthorizeGeneratePriorMigrations(
	notice io.Writer,
	migrationsDir string,
	dirFormat migrator.MigrationDirFormat,
	policy migrationintegrity.Policy,
) (fs.FS, error) {
	priorSnapshot, err := migrationsnapshot.CaptureDirectory(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("capture migration directory before replay: %w", err)
	}
	if _, err := migrationintegrity.GateWithPolicy(
		notice, priorSnapshot, dirFormat, policy, migrationintegrity.Options{},
	); err != nil {
		return nil, err
	}
	return priorSnapshot, nil
}

// generateReplayOptions carries what the locked replay path needs beyond the
// generator options it plans with.
type generateReplayOptions struct {
	migrationsDir  string
	devURL         string
	dirFormat      migrator.MigrationDirFormat
	connectTimeout time.Duration
	policy         migrationintegrity.Policy
}

// planGeneratedMigrationByReplay derives the current state by replaying the
// migration directory on the disposable --dev-url database, and plans the next
// migration from the difference between that state and the desired schema.
//
// Everything happens under the migration-directory lock, in this order: an
// interrupted publication left by an earlier run is recovered FIRST, so the
// snapshot taken next describes a settled directory rather than a half-written
// one; the snapshot is captured and put through the integrity gate; only then is
// the dev database connected and those exact bytes replayed. The connection
// context is derived from the LOCKED context for the same reason — a timeout
// started before the lock was held would be spent waiting for it.
//
// It lives outside migrateGenerateCommand because that function is a linear
// sequence of flag reads and this is the one branch with an ordering contract
// worth stating; keeping it inline also pushed the command past the length
// limit.
func planGeneratedMigrationByReplay(
	cmd *cobra.Command,
	generateOpts generator.GenerateMigrationOptions,
	opts generateReplayOptions,
) (*generator.MigrationPlan, error) {
	var plan *generator.MigrationPlan
	err := atlasmigrate.WithMigrationDirectoryLock(
		cmd.Context(),
		opts.migrationsDir,
		0,
		func(lockedCtx context.Context) error {
			if err := atlasmigrate.RecoverPendingPublicationLocked(opts.migrationsDir); err != nil {
				return fmt.Errorf("recover migration publication before replay: %w", err)
			}
			priorMigrations, err := captureAndAuthorizeGeneratePriorMigrations(
				cmd.ErrOrStderr(),
				opts.migrationsDir,
				opts.dirFormat,
				opts.policy,
			)
			if err != nil {
				return err
			}
			connectCtx, cancelConnect := dbcli.ConnectContext(lockedCtx, opts.connectTimeout)
			defer cancelConnect()
			devConn, connectErr := dbschema.ConnectToDatabase(connectCtx, opts.devURL)
			if connectErr != nil {
				return fmt.Errorf("connect to --%s: %w", generateDevURLFlag, connectErr)
			}
			defer dbschema.CloseAndWarn(devConn)
			return migrationreplay.WithReplayedSnapshot(
				lockedCtx,
				devConn,
				priorMigrations,
				opts.dirFormat,
				func(replayConn *dbschema.DatabaseConnection) error {
					replayOpts := generateOpts
					replayOpts.DBConn = replayConn
					replayOpts.PriorMigrationsFS = priorMigrations
					var planErr error
					plan, planErr = generator.PlanMigration(lockedCtx, replayOpts)
					return planErr
				},
			)
		},
	)
	return plan, err
}

func migrateGenerateCommand(cmd *cobra.Command, _ []string) error {
	integrityPolicy, err := migrationintegrity.Resolve()
	if err != nil {
		return err
	}
	rootDirs, err := cmd.Flags().GetStringArray(generateRootDirFlag)
	if err != nil {
		return err
	}
	schemaFiles, err := cmd.Flags().GetStringArray(generateSchemaFileFlag)
	if err != nil {
		return err
	}
	schemaCmd, err := cmd.Flags().GetString(generateSchemaCmdFlag)
	if err != nil {
		return err
	}
	schemaFormat, err := cmd.Flags().GetString(generateSchemaFormatFlag)
	if err != nil {
		return err
	}
	dbURL, err := cmd.Flags().GetString(generateDBURLFlag)
	if err != nil {
		return err
	}
	replay, err := cmd.Flags().GetBool(generateReplayFlag)
	if err != nil {
		return err
	}
	devURL, err := cmd.Flags().GetString(generateDevURLFlag)
	if err != nil {
		return err
	}
	migrationsDir, err := cmd.Flags().GetString(generateMigrationsDirFlag)
	if err != nil {
		return err
	}
	name, err := cmd.Flags().GetString(generateNameFlag)
	if err != nil {
		return err
	}
	shadowDB, err := cmd.Flags().GetString(generateShadowDBFlag)
	if err != nil {
		return err
	}
	configPath, err := cmd.Flags().GetString(dbcli.ConfigFlagName)
	if err != nil {
		return err
	}
	explicitTargetURL := dbURL
	if replay {
		explicitTargetURL = devURL
	}
	if err := sqlitevirtual.ValidateExplicitURLToggle(explicitTargetURL); err != nil {
		return err
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, configPath)
	if err != nil {
		return err
	}
	commands, err := dbcli.ResolveExternalSchemaCommands(cmd, schemaCmd, schemaFormat, projectCfg)
	if err != nil {
		return err
	}

	dbURL = dbcli.EffectiveString(cmd, generateDBURLFlag, dbURL, projectCfg.StringValue(projectconfig.StringDatabaseURL))
	migrationsDir = dbcli.EffectiveString(cmd, generateMigrationsDirFlag, migrationsDir, projectCfg.StringValue(projectconfig.StringMigrationDir))
	shadowDB = dbcli.EffectiveString(cmd, generateShadowDBFlag, shadowDB, projectCfg.StringValue(projectconfig.StringDevURL))
	reportFormat, err := cmd.Flags().GetString(generateReportFormatFlag)
	if err != nil {
		return err
	}
	if reportFormat != "" && reportFormat != "html" && reportFormat != "json" {
		return fmt.Errorf("unsupported safety report format %q", reportFormat)
	}
	checkDestructive, err := cmd.Flags().GetBool(generateCheckDestructiveFlag)
	if err != nil {
		return err
	}
	allowDestructive, err := cmd.Flags().GetBool(generateAllowDestructiveFlag)
	if err != nil {
		return err
	}
	connectTimeoutValue, err := cmd.Flags().GetString(dbcli.ConnectTimeoutFlagName)
	if err != nil {
		return err
	}
	dirFormatValue, err := cmd.Flags().GetString(generateDirFormatFlag)
	if err != nil {
		return err
	}
	qualifierValue, err := cmd.Flags().GetString(generateQualifierFlag)
	if err != nil {
		return err
	}
	schemasValue, err := cmd.Flags().GetString(dbcli.SchemasFlagName)
	if err != nil {
		return err
	}
	schemasValue = dbcli.EffectiveString(
		cmd,
		dbcli.SchemasFlagName,
		schemasValue,
		dbcli.JoinSchemasValue(projectCfg.SchemasValue()),
	)
	connectTimeoutValue = dbcli.EffectiveString(
		cmd,
		dbcli.ConnectTimeoutFlagName,
		connectTimeoutValue,
		projectCfg.StringValue(projectconfig.StringMigrationConnectTimeout),
	)

	// Early qualifier syntax validation; the generator re-validates the
	// single-schema scope and dialect support once the dialect is known.
	if _, err := atlasmigrate.ParseQualifier(qualifierValue); err != nil {
		return err
	}
	targetURL := dbURL
	if replay {
		if err := validateGenerateReplayMode(cmd); err != nil {
			return err
		}
		if strings.TrimSpace(devURL) == "" {
			return fmt.Errorf("--%s is required with --%s", generateDevURLFlag, generateReplayFlag)
		}
		targetURL = devURL
	} else {
		if err := validateGenerateIntrospectMode(cmd); err != nil {
			return err
		}
		if dbURL == "" {
			return fmt.Errorf("database URL is required")
		}
	}
	dialect, err := atlasurl.DialectFromURL(targetURL)
	if err != nil {
		return err
	}
	if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
		return err
	}
	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	migrationsDir, err = pathguard.ResolveCLIPath(migrationsDir)
	if err != nil {
		return fmt.Errorf("invalid migrations directory: %w", err)
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(connectTimeoutValue)
	if err != nil {
		return err
	}

	generated, err := loadGenerateSchema(cmd, rootDirs, schemaFiles, commands, dialect)
	if err != nil {
		return err
	}

	priorMigrations, dirFormat, err := captureGeneratePriorMigrations(
		cmd.Context(),
		cmd.ErrOrStderr(),
		generatePriorMigrationOptions{
			migrationsDir: migrationsDir,
			dirFormat:     dirFormatValue,
			shadowDB:      shadowDB,
			replay:        replay,
			policy:        integrityPolicy,
		},
	)
	if err != nil {
		return err
	}

	generateOpts := generator.GenerateMigrationOptions{
		Generated:         generated,
		DatabaseURL:       targetURL,
		MigrationName:     name,
		OutputDir:         migrationsDir,
		Schemas:           dbcli.ParseSchemas(schemasValue),
		CheckDestructive:  checkDestructive,
		AllowDestructive:  allowDestructive,
		ReportFormat:      reportFormat,
		ShadowDatabaseURL: shadowDB,
		PriorMigrationsFS: priorMigrations,
		SchemaQualifier:   qualifierValue,
		DiffPolicy: generator.DiffPolicy{
			SkipChangeKinds:     projectCfg.Diff.SkipChangeKinds(),
			ConcurrentIndex:     projectCfg.Diff.ConcurrentIndexCreate(),
			ConcurrentIndexDrop: projectCfg.Diff.ConcurrentIndexDrop(),
		},
	}
	var files *generator.MigrationFiles
	if replay {
		var plan *generator.MigrationPlan
		plan, err = planGeneratedMigrationByReplay(cmd, generateOpts, generateReplayOptions{
			migrationsDir:  migrationsDir,
			devURL:         devURL,
			dirFormat:      dirFormat,
			connectTimeout: connectTimeout,
			policy:         integrityPolicy,
		})
		if err == nil && plan != nil {
			files, err = plan.WriteFilesContext(cmd.Context())
		}
	} else {
		connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
		defer cancelConnect()
		files, err = generator.GenerateMigration(connectCtx, generateOpts)
	}
	if err != nil {
		return err
	}
	if files == nil {
		return nil
	}
	reportGeneratedMigrationFiles(cmd.OutOrStdout(), targetURL, files)
	return nil
}

func reportGeneratedMigrationFiles(out io.Writer, targetURL string, files *generator.MigrationFiles) {
	fmt.Fprintf(out, "Generated migration files for %s:\n", dbschema.FormatDatabaseURL(targetURL))
	for _, pair := range files.Files {
		fmt.Fprintf(out, "UP:   %s\n", pair.UpFile)
		fmt.Fprintf(out, "DOWN: %s\n", pair.DownFile)
		if pair.ReportFile != "" {
			fmt.Fprintf(out, "REPORT: %s\n", pair.ReportFile)
		}
	}
}

// loadGenerateSchema reads the desired schema for `migrations generate`.
//
// It lives outside migrateGenerateCommand because that function is a linear
// sequence of flag reads and grew past the length limit when `--plain-http`
// was added for oci:// sources (stokaro/ptah#928 item 1). The flag is read
// here rather than beside its siblings so the value and its only consumer
// stay in one place.
func loadGenerateSchema(
	cmd *cobra.Command,
	rootDirs, schemaFiles []string,
	commands []schemasource.Command,
	dialect string,
) (*goschema.Database, error) {
	plainHTTP, err := cmd.Flags().GetBool(dbcli.PlainHTTPFlagName)
	if err != nil {
		return nil, err
	}
	return schemaload.LoadContext(cmd.Context(), schemaload.Options{
		RootDirs:    rootDirs,
		SchemaFiles: schemaFiles,
		Commands:    commands,
		Dialect:     dialect,
		PlainHTTP:   plainHTTP,
	})
}
