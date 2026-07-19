package migrateup

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/lint"
	"github.com/stokaro/ptah/migration/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/onlineddl"
)

var migrateUpCmd = &cobra.Command{
	Use:   "migrate-up",
	Short: "Run pending migrations up to the latest version",
	Long: `Run all pending database migrations up to the latest version.

This command applies all migrations that haven't been applied yet, bringing
the database schema up to the latest version defined in the migration files.

Each migration is run in a transaction unless its file explicitly opts out with
-- +ptah no_transaction, so ordinary migration failures are rolled back and the
migration process stops.`,
	RunE: migrateUpCommand,
}

const (
	dbURLFlag                = "db-url"
	migrationsFlag           = "migrations-dir"
	dryRunFlag               = "dry-run"
	verboseFlag              = "verbose"
	verifySumFlag            = "verify-sum"
	dirFormatFlag            = "dir-format"
	atlasEnvFlag             = "atlas-env"
	execOrderFlag            = "exec-order"
	migrationLockTimeoutFlag = "migration-lock-timeout"
	lockTimeoutFlag          = "lock-timeout"
	statementTimeoutFlag     = "statement-timeout"
	allowDestructiveFlag     = "allow-destructive"
)

var migrateUpFlags = map[string]cobraflags.Flag{
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
	dryRunFlag: &cobraflags.BoolFlag{
		Name:  dryRunFlag,
		Value: false,
		Usage: "Show what migrations would be applied without actually running them",
	},
	verboseFlag: &cobraflags.BoolFlag{
		Name:  verboseFlag,
		Value: false,
		Usage: "Enable verbose output",
	},
	verifySumFlag: &cobraflags.BoolFlag{
		Name:  verifySumFlag,
		Value: false,
		Usage: "Verify the migrations directory against its committed ptah.sum before applying; abort on drift",
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
	execOrderFlag: &cobraflags.StringFlag{
		Name:  execOrderFlag,
		Value: string(migrator.ExecOrderLinear),
		Usage: "Execution order policy for pending migrations below the current version: linear, linear-skip, or non-linear",
	},
	migrationLockTimeoutFlag: &cobraflags.StringFlag{
		Name:  migrationLockTimeoutFlag,
		Value: "",
		Usage: "Timeout for acquiring the session-level migration advisory lock, such as 10s or 2m",
	},
	lockTimeoutFlag: &cobraflags.StringFlag{
		Name:  lockTimeoutFlag,
		Value: "",
		Usage: "Default per-migration lock timeout, such as 3s or 500ms",
	},
	statementTimeoutFlag: &cobraflags.StringFlag{
		Name:  statementTimeoutFlag,
		Value: "",
		Usage: "Default per-migration statement timeout, such as 30s or 2m",
	},
	allowDestructiveFlag: &cobraflags.BoolFlag{
		Name:  allowDestructiveFlag,
		Value: false,
		Usage: "Allow pending migrations that contain destructive statements",
	},
	dbcli.ConnectTimeoutFlagName:      dbcli.NewConnectTimeoutFlag(),
	dbcli.ConfigFlagName:              dbcli.NewConfigFlag(),
	dbcli.EnvFlagName:                 dbcli.NewEnvFlag(),
	dbcli.MigrationsSchemaFlagName:    dbcli.NewMigrationsSchemaFlag(),
	dbcli.MigrationsTableFlagName:     dbcli.NewMigrationsTableFlag(),
	dbcli.RevisionTableFormatFlagName: dbcli.NewRevisionTableFormatFlag(),
}

var migrateUpFlagsRegistered bool

func NewMigrateUpCommand() *cobra.Command {
	if !migrateUpFlagsRegistered {
		cobraflags.RegisterMap(migrateUpCmd, migrateUpFlags)
		migrateUpFlagsRegistered = true
	}
	return migrateUpCmd
}

func migrateUpCommand(cmd *cobra.Command, _ []string) error {
	dbURL := migrateUpFlags[dbURLFlag].GetString()
	migrationsDir := migrateUpFlags[migrationsFlag].GetString()
	dryRun := migrateUpFlags[dryRunFlag].GetBool()
	verbose := migrateUpFlags[verboseFlag].GetBool()
	verifySum := migrateUpFlags[verifySumFlag].GetBool()
	dirFormatValue := migrateUpFlags[dirFormatFlag].GetString()
	atlasEnv := migrateUpFlags[atlasEnvFlag].GetString()
	execOrderValue := migrateUpFlags[execOrderFlag].GetString()
	migrationLockTimeoutValue := migrateUpFlags[migrationLockTimeoutFlag].GetString()
	lockTimeout := migrateUpFlags[lockTimeoutFlag].GetString()
	statementTimeout := migrateUpFlags[statementTimeoutFlag].GetString()
	allowDestructive := migrateUpFlags[allowDestructiveFlag].GetBool()
	migrationsSchema := migrateUpFlags[dbcli.MigrationsSchemaFlagName].GetString()
	migrationsTable := migrateUpFlags[dbcli.MigrationsTableFlagName].GetString()
	revisionFormatValue := migrateUpFlags[dbcli.RevisionTableFormatFlagName].GetString()
	configPath := migrateUpFlags[dbcli.ConfigFlagName].GetString()

	projectCfg, err := dbcli.LoadProjectConfig(cmd, configPath)
	if err != nil {
		return err
	}
	dbURL = dbcli.EffectiveString(cmd, dbURLFlag, dbURL, projectCfg.DatabaseURL)
	migrationsDir = dbcli.EffectiveString(cmd, migrationsFlag, migrationsDir, projectCfg.Migration.Dir)
	dirFormatValue = dbcli.EffectiveString(cmd, dirFormatFlag, dirFormatValue, projectCfg.Migration.Format)
	atlasEnv = dbcli.EffectiveString(cmd, atlasEnvFlag, atlasEnv, projectCfg.EnvName)
	execOrderValue = dbcli.EffectiveString(cmd, execOrderFlag, execOrderValue, projectCfg.Migration.ExecOrder)
	lockTimeout = dbcli.EffectiveString(cmd, lockTimeoutFlag, lockTimeout, projectCfg.Migration.LockTimeout)
	migrationsSchema = dbcli.EffectiveString(cmd, dbcli.MigrationsSchemaFlagName, migrationsSchema, projectCfg.Migration.RevisionsSchema)
	revisionFormatValue = dbcli.EffectiveString(cmd, dbcli.RevisionTableFormatFlagName, revisionFormatValue, projectCfg.Migration.RevisionFormat)

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}

	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	migrationsDir, err = pathguard.ResolveCLIPath(migrationsDir)
	if err != nil {
		return fmt.Errorf("invalid migrations directory: %w", err)
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(dirFormatValue)
	if err != nil {
		return err
	}
	revisionFormat, err := migrator.ParseRevisionTableFormat(revisionFormatValue)
	if err != nil {
		return err
	}

	// Integrity gate: refuse to apply if a committed migration was edited
	// out of band. Runs before connecting so a tampered directory fails fast.
	if verifySum {
		result, err := migratesum.VerifyDirWithFormat(migrationsDir, dirFormat)
		if err != nil {
			return fmt.Errorf("migration sum verification failed: %w", err)
		}
		if !result.OK() {
			return fmt.Errorf("migration sum verification failed:\n%s", result.Describe())
		}
		if verbose {
			fmt.Printf("%s verified: migrations directory is intact\n", result.SumFileName)
		}
	}

	if verbose {
		fmt.Printf("Connecting to database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	}

	timeouts, err := migrator.ParseMigrationTimeouts(lockTimeout, statementTimeout)
	if err != nil {
		return err
	}
	execOrder, err := migrator.ParseExecOrder(execOrderValue)
	if err != nil {
		return err
	}
	migrationLockTimeout, err := migrator.ParseMigrationLockTimeout(migrationLockTimeoutValue)
	if err != nil {
		return err
	}

	connectTimeout, err := dbcli.ParseConnectTimeout(migrateUpFlags[dbcli.ConnectTimeoutFlagName].GetString())
	if err != nil {
		return err
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	// Set dry run mode if requested
	conn.Writer().SetDryRun(dryRun)

	if dryRun {
		fmt.Println("=== DRY RUN MODE ===")
		fmt.Println("No actual changes will be made to the database")
		fmt.Println()
	}

	fmt.Println("=== MIGRATE UP ===")
	fmt.Printf("Database: %s\n", dbschema.FormatDatabaseURL(dbURL))
	fmt.Printf("Dialect: %s\n", conn.Info().Dialect)
	fmt.Printf("Migrations directory: %s\n", migrationsDir)
	fmt.Printf("Migration directory format: %s\n", dirFormat)
	fmt.Println()

	// Create filesystem from migrations directory
	migrationsFS := os.DirFS(migrationsDir)

	// Online-DDL routing: `-- +ptah online_ddl_tool=...` directives always
	// work; the ptah.yaml online_ddl section adds automatic routing of
	// ALTERs on tables above the configured row threshold.
	onlineCfg, err := dbcli.LoadOnlineDDLConfig(configPath)
	if err != nil {
		return err
	}
	if onlineCfg.Enabled() {
		fmt.Printf("Online DDL: tool=%s threshold_rows=%d\n", onlineCfg.Tool, onlineCfg.ThresholdRows)
	}
	interceptor := onlineddl.New(*onlineCfg).WithDryRun(dryRun)

	mig, err := migrator.NewFSMigrator(
		conn,
		migrationsFS,
		migrator.WithStatementInterceptor(interceptor),
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: atlasEnv}),
	)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(migrationsSchema, migrationsTable).
		WithRevisionTableFormat(revisionFormat).
		WithDefaultTimeouts(timeouts).
		WithExecOrder(execOrder).
		WithMigrationLockTimeout(migrationLockTimeout)

	// Get migration status before running
	status, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting migration status: %w", err)
	}

	fmt.Printf("Current version: %d\n", status.CurrentVersion)
	fmt.Printf("Total migrations: %d\n", status.TotalMigrations)
	fmt.Printf("Pending migrations: %d\n", len(status.PendingMigrations))
	if len(status.OutOfOrderMigrations) > 0 {
		fmt.Printf("Out-of-order migrations: %v\n", status.OutOfOrderMigrations)
	}

	if !status.HasPendingChanges {
		fmt.Println("✅ Database is already up to date!")
		return nil
	}

	if verbose {
		fmt.Printf("Pending migration versions: %v\n", status.PendingMigrations)
		if len(status.OutOfOrderMigrations) > 0 {
			fmt.Printf("Out-of-order migration versions: %v\n", status.OutOfOrderMigrations)
		}
	}
	if execOrder == migrator.ExecOrderLinear && len(status.OutOfOrderMigrations) > 0 {
		return migrator.NewOutOfOrderError(status.CurrentVersion, status.OutOfOrderMigrations)
	}

	if !allowDestructive {
		findings, err := lintPendingDestructive(migrationsFS, pendingMigrationsForSafetyCheck(status, execOrder), conn.Info().Dialect)
		if err != nil {
			return fmt.Errorf("error checking pending migration safety: %w", err)
		}
		if len(findings) > 0 {
			return fmt.Errorf("pending migrations contain destructive statements; rerun with --allow-destructive after review:\n%s", formatDestructiveFindings(findings))
		}
	}

	fmt.Println()

	// Run migrations
	err = mig.MigrateUp(context.Background())
	if err != nil {
		return fmt.Errorf("error running migrations: %w", err)
	}

	// Get final status
	finalStatus, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		return fmt.Errorf("error getting final migration status: %w", err)
	}

	fmt.Println()
	if dryRun {
		fmt.Println("✅ Dry run completed successfully!")
		fmt.Printf("Would have applied %d migrations\n", len(status.PendingMigrations))
	} else {
		fmt.Println("✅ Migrations completed successfully!")
		fmt.Printf("Database is now at version: %d\n", finalStatus.CurrentVersion)
	}

	return nil
}

func pendingMigrationsForSafetyCheck(status *migrator.MigrationStatus, execOrder migrator.ExecOrder) []int64 {
	if execOrder != migrator.ExecOrderLinearSkip {
		return status.PendingMigrations
	}

	outOfOrder := make(map[int64]struct{}, len(status.OutOfOrderMigrations))
	for _, version := range status.OutOfOrderMigrations {
		outOfOrder[version] = struct{}{}
	}

	pending := make([]int64, 0, len(status.PendingMigrations))
	for _, version := range status.PendingMigrations {
		if _, ok := outOfOrder[version]; ok {
			continue
		}
		pending = append(pending, version)
	}
	return pending
}

func lintPendingDestructive(fsys fs.FS, pending []int64, dialect string) ([]lint.Finding, error) {
	findings, err := lint.LintFS(fsys, lint.Options{
		Dialect:  dialect,
		Disabled: []string{"MF", "BC", "PG", "MY"},
		Versions: pending,
	})
	if err != nil {
		return nil, err
	}
	var destructive []lint.Finding
	for _, finding := range findings {
		if strings.HasPrefix(finding.Rule, "DS") {
			destructive = append(destructive, finding)
		}
	}
	return destructive, nil
}

func formatDestructiveFindings(findings []lint.Finding) string {
	var b strings.Builder
	for _, finding := range findings {
		if finding.Line > 0 {
			fmt.Fprintf(&b, "- %s:%d %s %s: %s\n", finding.File, finding.Line, finding.Rule, finding.Severity, finding.Message)
			continue
		}
		fmt.Fprintf(&b, "- %s %s %s: %s\n", finding.File, finding.Rule, finding.Severity, finding.Message)
	}
	return strings.TrimRight(b.String(), "\n")
}
