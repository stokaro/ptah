package atlas

import (
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/migrator"
)

type atlasMigrateApplyOptions struct {
	url             string
	dir             string
	dryRun          bool
	txMode          string
	execOrder       string
	toVersion       string
	allowDirty      bool
	baseline        string
	revisionsSchema string
	lockName        string
	lockTimeout     string
	format          string
}

func newAtlasMigrateApplyCommand() *cobra.Command {
	opts := atlasMigrateApplyOptions{
		txMode:    string(migrator.MigrationTxModeFile),
		execOrder: string(migrator.ExecOrderLinear),
	}
	cmd := &cobra.Command{
		Use:   "apply [amount]",
		Short: "Apply pending migrations",
		Long: `Apply pending Atlas migrations to the target database.

By default, all pending migrations are applied. The optional amount argument
limits the run to the first N pending migrations. Use --to-version to apply up
to a specific migration version instead.

Native Ptah equivalent: ptah migrations up.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAtlasMigrateApply(cmd, opts, args)
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL to apply migrations to")
	flags.StringVar(&opts.dir, "dir", "", "Migration directory URL")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Show migrations without applying them")
	flags.StringVar(&opts.txMode, "tx-mode", opts.txMode, "Transaction mode: file, all, or none")
	flags.StringVar(&opts.execOrder, "exec-order", opts.execOrder, "Execution order: linear, linear-skip, or non-linear")
	flags.StringVar(&opts.toVersion, "to-version", "", "Target migration version to apply up to")
	flags.BoolVar(&opts.allowDirty, "allow-dirty", false, "Allow applying migrations when the revision table is dirty")
	flags.StringVar(&opts.baseline, "baseline", "", "Baseline version to mark applied before running pending migrations")
	flags.StringVar(&opts.revisionsSchema, "revisions-schema", "", "Schema for the Atlas revisions table")
	flags.StringVar(&opts.lockName, "lock-name", "", "Migration lock name")
	flags.StringVar(&opts.lockTimeout, "lock-timeout", "", "Timeout for acquiring the migration lock, such as 10s or 2m")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")

	cmdutil.ConfigureCommandArgs(cmd, atlasMigrateApplyArgs)
	return cmd
}

func atlasMigrateApplyArgs(cmd *cobra.Command, args []string) error {
	if len(args) > 1 {
		return cmdutil.Fail(cmd, fmt.Errorf("accepts at most one amount argument"))
	}
	return nil
}

func runAtlasMigrateApply(cmd *cobra.Command, opts atlasMigrateApplyOptions, args []string) error {
	if cmd.Flags().Changed("format") {
		return fmt.Errorf("atlas migrate apply accepts --format, but Ptah does not implement its behavior yet")
	}
	if cmd.Flags().Changed("lock-name") {
		return fmt.Errorf("atlas migrate apply accepts --lock-name, but Ptah does not implement its behavior yet")
	}
	if opts.url == "" {
		return fmt.Errorf("database URL is required")
	}
	if opts.dir == "" {
		return fmt.Errorf("migrations directory is required")
	}

	dir, err := atlasLocalDirValue(opts.dir)
	if err != nil {
		return fmt.Errorf("atlas migrate apply --dir: %w", err)
	}
	dir, err = pathguard.ResolveCLIPath(dir)
	if err != nil {
		return fmt.Errorf("invalid migration directory: %w", err)
	}

	amount, err := parseAtlasMigrateApplyAmount(args)
	if err != nil {
		return err
	}
	toVersion, err := parseAtlasMigrationVersionFlag("to-version", opts.toVersion)
	if err != nil {
		return err
	}
	baselineVersion, err := parseAtlasMigrationVersionFlag("baseline", opts.baseline)
	if err != nil {
		return err
	}
	if amount > 0 && toVersion > 0 {
		return fmt.Errorf("amount argument and --to-version cannot both be set")
	}

	txMode, err := migrator.ParseMigrationTxMode(opts.txMode)
	if err != nil {
		return err
	}
	execOrder, err := migrator.ParseExecOrder(opts.execOrder)
	if err != nil {
		return err
	}
	migrationLockTimeout, err := migrator.ParseMigrationLockTimeout(opts.lockTimeout)
	if err != nil {
		return err
	}

	conn, err := dbschema.ConnectToDatabase(cmd.Context(), opts.url)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)
	conn.SchemaWriter().SetDryRun(opts.dryRun)

	mig, err := newAtlasApplyMigrator(conn, os.DirFS(dir), atlasApplyMigratorOptions{
		execOrder:            execOrder,
		txMode:               txMode,
		revisionsSchema:      opts.revisionsSchema,
		migrationLockTimeout: migrationLockTimeout,
	})
	if err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	if opts.dryRun {
		fmt.Fprintln(out, "Dry run mode: no changes will be made.")
	}
	var assumedAppliedVersions []int64
	if baselineVersion > 0 {
		if opts.dryRun {
			assumedAppliedVersions, err = atlasApplyBaselineVersions(mig, baselineVersion)
			if err != nil {
				return err
			}
			fmt.Fprintf(out, "Would baseline migrations at version %d.\n", baselineVersion)
		} else if err := mig.BaselineWithOptions(cmd.Context(), migrator.BaselineOptions{Version: baselineVersion}); err != nil {
			return fmt.Errorf("error baselining migrations: %w", err)
		}
	}

	status, err := mig.GetMigrationStatus(cmd.Context())
	if err != nil {
		return fmt.Errorf("error getting migration status: %w", err)
	}
	pending := status.PendingMigrations
	if len(assumedAppliedVersions) > 0 {
		pending = pendingAfterAssumedApplied(status.PendingMigrations, assumedAppliedVersions)
	}
	selected := selectedAtlasApplyVersions(pending, amount, toVersion)
	if len(selected) == 0 && status.DirtyRevision == nil {
		fmt.Fprintln(out, "No migration files to execute.")
		return nil
	}
	if len(selected) > 0 {
		fmt.Fprintf(out, "Migrating to version %d from %d pending migrations.\n", selected[len(selected)-1], len(selected))
	}

	if err := mig.MigrateUpWithOptions(cmd.Context(), migrator.MigrateUpOptions{
		TargetVersion:          toVersion,
		Amount:                 amount,
		AllowDirty:             opts.allowDirty,
		AssumedAppliedVersions: assumedAppliedVersions,
	}); err != nil {
		return fmt.Errorf("error applying migrations: %w", err)
	}

	if opts.dryRun {
		fmt.Fprintf(out, "Would have applied %d migrations.\n", len(selected))
		return nil
	}
	finalStatus, err := mig.GetMigrationStatus(cmd.Context())
	if err != nil {
		return fmt.Errorf("error getting final migration status: %w", err)
	}
	fmt.Fprintf(out, "Migration complete. Current version: %d\n", finalStatus.CurrentVersion)
	return nil
}

type atlasApplyMigratorOptions struct {
	execOrder            migrator.ExecOrder
	txMode               migrator.MigrationTxMode
	revisionsSchema      string
	migrationLockTimeout time.Duration
}

func newAtlasApplyMigrator(
	conn *dbschema.DatabaseConnection,
	fsys fs.FS,
	opts atlasApplyMigratorOptions,
) (*migrator.Migrator, error) {
	mig, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	if err != nil {
		return nil, fmt.Errorf("error registering migrations: %w", err)
	}
	return mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationsTable(opts.revisionsSchema, "").
		WithExecOrder(opts.execOrder).
		WithTransactionMode(opts.txMode).
		WithMigrationLockTimeout(opts.migrationLockTimeout).
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), nil
}

func parseAtlasMigrateApplyAmount(args []string) (uint64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	value, err := strconv.ParseUint(strings.TrimSpace(args[0]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("amount argument %q is not a valid unsigned integer: %w", args[0], err)
	}
	return value, nil
}

func parseAtlasMigrationVersionFlag(name, value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("--%s %q is not a valid migration version: %w", name, value, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("--%s must be greater than zero", name)
	}
	return parsed, nil
}

func atlasApplyBaselineVersions(mig *migrator.Migrator, baselineVersion int64) ([]int64, error) {
	versions := make([]int64, 0)
	for _, migration := range mig.MigrationProvider().Migrations() {
		if migration.Version <= baselineVersion {
			versions = append(versions, migration.Version)
		}
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("no migrations found at or below baseline version %d", baselineVersion)
	}
	return versions, nil
}

func pendingAfterAssumedApplied(pending []int64, assumedApplied []int64) []int64 {
	assumed := make(map[int64]struct{}, len(assumedApplied))
	for _, version := range assumedApplied {
		assumed[version] = struct{}{}
	}
	filtered := make([]int64, 0, len(pending))
	for _, version := range pending {
		if _, ok := assumed[version]; !ok {
			filtered = append(filtered, version)
		}
	}
	return filtered
}

func selectedAtlasApplyVersions(pending []int64, amount uint64, toVersion int64) []int64 {
	selected := make([]int64, 0, len(pending))
	for _, version := range pending {
		if toVersion > 0 && version > toVersion {
			continue
		}
		selected = append(selected, version)
		if amount > 0 && uint64(len(selected)) == amount {
			break
		}
	}
	return selected
}
