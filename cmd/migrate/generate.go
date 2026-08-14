package migrate

import (
	"context"
	"fmt"
	"io"
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
	"go.5x5.cz/ptah/internal/migrationreplay"
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

func migrateGenerateCommand(cmd *cobra.Command, _ []string) error {
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
	target, err := resolveGenerateTarget(targetURL, migrationsDir, connectTimeoutValue)
	if err != nil {
		return err
	}
	dialect, migrationsDir, connectTimeout := target.dialect, target.migrationsDir, target.connectTimeout

	generated, err := loadGenerateSchema(cmd, rootDirs, schemaFiles, commands, dialect)
	if err != nil {
		return err
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
	defer cancelConnect()

	var devConn *dbschema.DatabaseConnection
	var dirFormat migrator.MigrationDirFormat
	if replay {
		dirFormat, err = migrator.ParseMigrationDirFormat(dirFormatValue)
		if err != nil {
			return err
		}
		devConn, err = dbschema.ConnectToDatabase(connectCtx, devURL)
		if err != nil {
			return fmt.Errorf("connect to --%s: %w", generateDevURLFlag, err)
		}
		defer dbschema.CloseAndWarn(devConn)
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
		err = atlasmigrate.WithMigrationDirectoryLock(
			cmd.Context(),
			migrationsDir,
			0,
			func(lockedCtx context.Context) error {
				if err := atlasmigrate.RecoverPendingPublicationLocked(migrationsDir); err != nil {
					return fmt.Errorf("recover migration publication before replay: %w", err)
				}
				return migrationreplay.WithReplayedDirectory(
					lockedCtx,
					devConn,
					migrationsDir,
					dirFormat,
					func(replayConn *dbschema.DatabaseConnection) error {
						replayOpts := generateOpts
						replayOpts.DBConn = replayConn
						plan, err = generator.PlanMigration(lockedCtx, replayOpts)
						return err
					},
				)
			},
		)
		if err == nil && plan != nil {
			files, err = plan.WriteFilesContext(cmd.Context())
		}
	} else {
		files, err = generator.GenerateMigration(connectCtx, generateOpts)
	}
	if err != nil {
		return err
	}
	if files == nil {
		return nil
	}

	reportGeneratedFiles(cmd.OutOrStdout(), targetURL, files)
	return nil
}

// generateTarget carries what [resolveGenerateTarget] established, so the caller
// takes one value rather than three positional results nobody can read at a
// glance.
type generateTarget struct {
	dialect        string
	migrationsDir  string
	connectTimeout time.Duration
}

// resolveGenerateTarget turns the raw target URL, migrations directory and
// connect-timeout text into the values the generation run needs, refusing each
// one at the point it is read rather than letting a later step fail on it.
//
// It is separate from migrateGenerateCommand because both halves of this
// command grew: the OCI source surface added its own flags and the SQLite
// virtual-table toggle added a validation, and the two together pushed the
// caller past the length the linter enforces. Splitting on "resolve the target"
// keeps the reads beside the checks that reject them.
//
// stokaro/ptah#1509 shortened the same function on master and chose a different
// cut: it extracted the reporting tail only, as writeGeneratedMigrationFiles.
// That body is byte-identical to reportGeneratedFiles below, so the merge keeps
// one of the two rather than both under two names, and keeps this cut as well
// because it is the one this command's caller reads through.
func resolveGenerateTarget(targetURL, migrationsDir, connectTimeoutValue string) (generateTarget, error) {
	dialect, err := atlasurl.DialectFromURL(targetURL)
	if err != nil {
		return generateTarget{}, err
	}
	if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
		return generateTarget{}, err
	}
	if migrationsDir == "" {
		return generateTarget{}, fmt.Errorf("migrations directory is required")
	}
	resolvedDir, err := pathguard.ResolveCLIPath(migrationsDir)
	if err != nil {
		return generateTarget{}, fmt.Errorf("invalid migrations directory: %w", err)
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(connectTimeoutValue)
	if err != nil {
		return generateTarget{}, err
	}
	return generateTarget{dialect: dialect, migrationsDir: resolvedDir, connectTimeout: connectTimeout}, nil
}

// reportGeneratedFiles prints what the run wrote, one line per artifact.
func reportGeneratedFiles(out io.Writer, targetURL string, files *generator.MigrationFiles) {
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
