package atlasmigrate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

type ApplyOptions struct {
	Dir                  string
	DryRun               bool
	ExecOrder            migrator.ExecOrder
	TxMode               migrator.MigrationTxMode
	RevisionsSchema      string
	MigrationLockName    string
	MigrationLockTimeout time.Duration
	Amount               uint64
	ToVersion            int64
	AllowDirty           bool
	BaselineVersion      int64
}

// ApplyPlan is the selected Atlas migrate apply work prepared from the
// migration directory and current revision state.
type ApplyPlan struct {
	Status           *migrator.MigrationStatus
	Migrations       []*migrator.Migration
	SelectedVersions []int64
	CurrentVersion   int64
	DryRun           bool
	StartedAt        time.Time

	mig  *migrator.Migrator
	opts ApplyOptions
}

// ApplyResult contains execution metadata needed by CLI output and Atlas
// template rendering.
type ApplyResult struct {
	Status           *migrator.MigrationStatus
	FinalStatus      *migrator.MigrationStatus
	Migrations       []*migrator.Migration
	SelectedVersions []int64
	CurrentVersion   int64
	Applied          bool
	DryRun           bool
	StartedAt        time.Time
	EndedAt          time.Time
	ErrorText        string
	ApplyError       error
}

// PrepareApply builds the Atlas-format migrator, applies real baseline
// metadata when requested, and selects the pending migrations to execute.
func PrepareApply(ctx context.Context, conn *dbschema.DatabaseConnection, opts ApplyOptions) (ApplyPlan, error) {
	if err := validateApplyOptions(conn, opts); err != nil {
		return ApplyPlan{}, err
	}
	startedAt := time.Now()

	conn.SchemaWriter().SetDryRun(opts.DryRun)
	mig, err := newApplyMigrator(conn, os.DirFS(opts.Dir), applyMigratorOptions{
		execOrder:            opts.ExecOrder,
		txMode:               opts.TxMode,
		revisionsSchema:      opts.RevisionsSchema,
		migrationLockName:    opts.MigrationLockName,
		migrationLockTimeout: opts.MigrationLockTimeout,
	})
	if err != nil {
		return ApplyPlan{}, err
	}

	var assumedAppliedVersions []int64
	if opts.BaselineVersion > 0 {
		if opts.DryRun {
			assumedAppliedVersions, err = applyBaselineVersions(mig, opts.BaselineVersion)
			if err != nil {
				return ApplyPlan{}, err
			}
		} else if err := mig.BaselineWithOptions(ctx, migrator.BaselineOptions{Version: opts.BaselineVersion}); err != nil {
			return ApplyPlan{}, fmt.Errorf("error baselining migrations: %w", err)
		}
	}

	status, err := mig.GetMigrationStatus(ctx)
	if err != nil {
		return ApplyPlan{}, fmt.Errorf("error getting migration status: %w", err)
	}
	plannedCurrentVersion := statusCurrentAfterAssumedApplied(status.CurrentVersion, assumedAppliedVersions)
	pending := status.PendingMigrations
	if len(assumedAppliedVersions) > 0 {
		pending = pendingAfterAssumedApplied(status.PendingMigrations, assumedAppliedVersions)
	}

	return ApplyPlan{
		Status:           status,
		Migrations:       mig.MigrationProvider().Migrations(),
		SelectedVersions: selectedApplyVersions(pending, opts.Amount, opts.ToVersion),
		CurrentVersion:   plannedCurrentVersion,
		DryRun:           opts.DryRun,
		StartedAt:        startedAt,
		mig:              mig,
		opts:             opts,
	}, nil
}

// Noop reports whether the selected plan has no migrations to execute and no
// dirty revision requiring recovery.
func (p ApplyPlan) Noop() bool {
	return len(p.SelectedVersions) == 0 && p.Status != nil && p.Status.DirtyRevision == nil
}

// Execute applies the selected plan. Dry-run and no-op plans return metadata
// without modifying schema state.
func (p ApplyPlan) Execute(ctx context.Context) (ApplyResult, error) {
	result := ApplyResult{
		Status:           p.Status,
		Migrations:       p.Migrations,
		SelectedVersions: p.SelectedVersions,
		CurrentVersion:   p.CurrentVersion,
		DryRun:           p.DryRun,
		StartedAt:        p.StartedAt,
	}
	if p.Noop() || p.opts.DryRun {
		result.EndedAt = time.Now()
		return result, nil
	}

	err := p.mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{
		TargetVersion: p.opts.ToVersion,
		Amount:        p.opts.Amount,
		AllowDirty:    p.opts.AllowDirty,
	})
	result.EndedAt = time.Now()
	if err != nil {
		result.Applied = true
		result.ApplyError = err
		result.ErrorText = err.Error()
		return result, fmt.Errorf("error applying migrations: %w", err)
	}

	finalStatus, err := p.mig.GetMigrationStatus(ctx)
	if err != nil {
		result.Applied = true
		result.ErrorText = err.Error()
		return result, fmt.Errorf("error getting final migration status: %w", err)
	}
	result.FinalStatus = finalStatus
	result.Applied = true
	return result, nil
}

// ParseApplyAmount parses the optional Atlas migrate apply amount argument.
func ParseApplyAmount(args []string) (uint64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	if len(args) > 1 {
		return 0, errors.New("accepts at most one amount argument")
	}
	value, err := strconv.ParseUint(strings.TrimSpace(args[0]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("amount argument %q is not a valid unsigned integer: %w", args[0], err)
	}
	return value, nil
}

// ParseMigrationVersionFlag parses positive Atlas migration version flags such
// as --to-version and --baseline.
func ParseMigrationVersionFlag(name, value string) (int64, error) {
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

func validateApplyOptions(conn *dbschema.DatabaseConnection, opts ApplyOptions) error {
	if conn == nil {
		return errors.New("migrate apply requires database connection")
	}
	if strings.TrimSpace(opts.Dir) == "" {
		return errors.New("migrate apply requires migration directory")
	}
	if opts.Amount > 0 && opts.ToVersion > 0 {
		return errors.New("amount argument and --to-version cannot both be set")
	}
	if opts.ToVersion < 0 {
		return errors.New("migrate apply target version must be greater than or equal to zero")
	}
	if opts.BaselineVersion < 0 {
		return errors.New("migrate apply baseline version must be greater than or equal to zero")
	}
	return nil
}

type applyMigratorOptions struct {
	execOrder            migrator.ExecOrder
	txMode               migrator.MigrationTxMode
	revisionsSchema      string
	migrationLockName    string
	migrationLockTimeout time.Duration
}

func newApplyMigrator(
	conn *dbschema.DatabaseConnection,
	fsys fs.FS,
	opts applyMigratorOptions,
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
		WithMigrationLockName(opts.migrationLockName).
		WithMigrationLockTimeout(opts.migrationLockTimeout).
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), nil
}

func applyBaselineVersions(mig *migrator.Migrator, baselineVersion int64) ([]int64, error) {
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

func selectedApplyVersions(pending []int64, amount uint64, toVersion int64) []int64 {
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

func statusCurrentAfterAssumedApplied(current int64, assumedApplied []int64) int64 {
	for _, version := range assumedApplied {
		if version > current {
			current = version
		}
	}
	return current
}
