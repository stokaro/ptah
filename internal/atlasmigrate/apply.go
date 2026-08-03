package atlasmigrate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

type ApplyOptions struct {
	// Dir is the resolved migration directory path used for diagnostics.
	Dir string
	// FS is the already captured migration filesystem to execute.
	FS        fs.FS
	DryRun    bool
	ExecOrder migrator.ExecOrder
	// OutOfOrderExempt lists converted versions whose position below the
	// current high-water mark is an artifact of the layout projection rather
	// than a claim about authoring order. Only the Flyway converted path sets
	// it; see atlasmigrateimport.FlywayBaselineAtlasVersion.
	OutOfOrderExempt     []int64
	TxMode               migrator.MigrationTxMode
	RevisionsSchema      string
	MigrationLockTimeout time.Duration
	// MigrationLockName overrides the session advisory lock name the migration
	// run coordinates on. Empty keeps the migrator default. It carries Atlas's
	// `migrate apply --lock-name`.
	MigrationLockName string
	// SkipMigrationLock runs without the session advisory lock entirely. It
	// carries Atlas's `migrate apply --skip-lock`, and is the only way this
	// path executes migrations without serializing against another runner.
	SkipMigrationLock bool
	Amount            uint64
	// ToVersion bounds the apply at a migration version: every pending
	// migration up to and including it runs, and nothing above it does. Zero
	// means unbounded. It is the version bound Atlas spells --to-version, and
	// it is enforced by the migrator inside the migration lock rather than by
	// the preview below, so a run that races another writer applies the
	// versions the bound names rather than a count computed before the lock.
	ToVersion       int64
	AllowDirty      bool
	BaselineVersion int64
	// SkipChecks bypasses pre-migration check evaluation. Atlas registers no
	// flag for this on `migrate apply` (measured on CE v1.2.0 and on
	// Atlas's own help surface), so the compat command resolves it from
	// PTAH_SKIP_CHECKS rather than from the Atlas flag surface.
	SkipChecks bool
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

	mig                    *migrator.Migrator
	opts                   ApplyOptions
	assumedAppliedVersions []int64
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
	// ChecksDeferred lists versions whose pre-migration checks were parsed and
	// statically validated but not evaluated, because a dry run cannot produce
	// the state they assert on. Empty outside a dry run.
	ChecksDeferred []int64
}

// PrepareApply builds the Atlas-format migrator, applies real baseline
// metadata when requested, and selects the pending migrations to execute.
func PrepareApply(ctx context.Context, conn *dbschema.DatabaseConnection, opts ApplyOptions) (ApplyPlan, error) {
	if err := validateApplyOptions(conn, opts); err != nil {
		return ApplyPlan{}, err
	}
	startedAt := time.Now()

	conn.SchemaWriter().SetDryRun(opts.DryRun)
	mig, err := newApplyMigrator(conn, opts.FS, applyMigratorOptions{
		execOrder:            opts.ExecOrder,
		outOfOrderExempt:     opts.OutOfOrderExempt,
		txMode:               opts.TxMode,
		revisionsSchema:      opts.RevisionsSchema,
		migrationLockTimeout: opts.MigrationLockTimeout,
		migrationLockName:    opts.MigrationLockName,
		skipMigrationLock:    opts.SkipMigrationLock,
		skipChecks:           opts.SkipChecks,
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
		Status:                 status,
		Migrations:             mig.MigrationProvider().Migrations(),
		SelectedVersions:       selectedApplyVersions(pending, opts.Amount, opts.ToVersion),
		CurrentVersion:         plannedCurrentVersion,
		DryRun:                 opts.DryRun,
		StartedAt:              startedAt,
		mig:                    mig,
		opts:                   opts,
		assumedAppliedVersions: assumedAppliedVersions,
	}, nil
}

// Noop reports whether the selected plan has no migrations to execute and no
// dirty revision requiring recovery.
func (p ApplyPlan) Noop() bool {
	return len(p.SelectedVersions) == 0 && p.Status != nil && p.Status.DirtyRevision == nil
}

// MigrationLockName reports the advisory lock name the prepared migrator will
// acquire, read back from the migrator rather than from the request, so a
// caller reporting the lock names what the machinery resolved.
func (p ApplyPlan) MigrationLockName() string {
	if p.mig == nil {
		return ""
	}
	return p.mig.MigrationLockName()
}

// MigrationLockSkipped reports whether the prepared migrator will run without
// the advisory lock.
func (p ApplyPlan) MigrationLockSkipped() bool {
	return p.mig != nil && p.mig.MigrationLockSkipped()
}

// Execute applies the selected plan. Dry-run and no-op plans return metadata
// without modifying schema state.
func (p ApplyPlan) Execute(ctx context.Context) (ApplyResult, error) {
	return p.execute(ctx, nil)
}

// ExecuteWithPreflight applies the selected plan after running hook inside the
// migration lock, after transaction-mode validation and before execution.
func (p ApplyPlan) ExecuteWithPreflight(
	ctx context.Context,
	hook migrator.PreMigrationHook,
) (ApplyResult, error) {
	return p.execute(ctx, hook)
}

func (p ApplyPlan) execute(ctx context.Context, hook migrator.PreMigrationHook) (ApplyResult, error) {
	result := ApplyResult{
		Status:           p.Status,
		Migrations:       p.Migrations,
		SelectedVersions: p.SelectedVersions,
		CurrentVersion:   p.CurrentVersion,
		DryRun:           p.DryRun,
		StartedAt:        p.StartedAt,
	}
	executionStarted := false
	lockedPlanObserved := false
	err := p.mig.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{
		Amount:                 p.opts.Amount,
		TargetVersion:          p.opts.ToVersion,
		AllowDirty:             p.opts.AllowDirty,
		AssumedAppliedVersions: p.assumedAppliedVersions,
		PlanObserver: func(_ context.Context, plan migrator.MigrationPlan) {
			lockedPlanObserved = true
			result.SelectedVersions = slices.Clone(plan.Versions)
			result.CurrentVersion = plan.CurrentVersion
		},
		Preflight: func(ctx context.Context, plan migrator.MigrationPlan) error {
			if err := runApplyPreflight(ctx, hook, plan); err != nil {
				return err
			}
			executionStarted = len(plan.Versions) > 0
			return nil
		},
		ChecksDeferredObserver: func(_ context.Context, versions []int64) {
			result.ChecksDeferred = versions
		},
	})
	result.EndedAt = time.Now()
	if err != nil {
		result.Applied = executionStarted && !p.DryRun
		result.ApplyError = err
		result.ErrorText = err.Error()
		var txModeErr *migrator.AtlasTxModeDirectiveError
		if errors.As(err, &txModeErr) {
			return result, txModeErr
		}
		return result, fmt.Errorf("error applying migrations: %w", err)
	}
	if !executionStarted {
		finalStatus, statusErr := p.mig.GetMigrationStatus(ctx)
		if statusErr != nil {
			result.ErrorText = statusErr.Error()
			return result, fmt.Errorf("error getting final migration status: %w", statusErr)
		}
		result.FinalStatus = finalStatus
		result.CurrentVersion = finalStatus.CurrentVersion
		if !lockedPlanObserved {
			result.SelectedVersions = nil
		}
		return result, nil
	}
	if p.DryRun {
		return result, nil
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

func runApplyPreflight(
	ctx context.Context,
	hook migrator.PreMigrationHook,
	plan migrator.MigrationPlan,
) error {
	if hook == nil {
		return nil
	}
	return hook(ctx, plan)
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
// as --baseline.
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
	if opts.FS == nil {
		return errors.New("migrate apply requires migration filesystem")
	}
	if opts.BaselineVersion < 0 {
		return errors.New("migrate apply baseline version must be greater than or equal to zero")
	}
	if opts.ToVersion < 0 {
		return errors.New("migrate apply target version must be greater than or equal to zero")
	}
	// Refused here rather than left to the migrator, which raises the same
	// conflict only once it holds the migration lock. The two bounds select
	// different prefixes and there is no defensible precedence between them, so
	// the run must not start.
	if opts.ToVersion > 0 && opts.Amount > 0 {
		return errors.New("--to-version and the amount argument cannot both be set")
	}
	return nil
}

type applyMigratorOptions struct {
	execOrder            migrator.ExecOrder
	outOfOrderExempt     []int64
	txMode               migrator.MigrationTxMode
	revisionsSchema      string
	migrationLockTimeout time.Duration
	migrationLockName    string
	skipMigrationLock    bool
	skipChecks           bool
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
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationsTable(opts.revisionsSchema, "").
		WithExecOrder(opts.execOrder).
		WithOutOfOrderExempt(opts.outOfOrderExempt).
		WithTransactionMode(opts.txMode).
		WithMigrationLockTimeout(opts.migrationLockTimeout).
		WithMigrationLockName(opts.migrationLockName).
		WithSkipChecks(opts.skipChecks).
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if opts.skipMigrationLock {
		mig = mig.WithoutMigrationLock()
	}
	return mig, nil
}

func applyBaselineVersions(mig *migrator.Migrator, baselineVersion int64) ([]int64, error) {
	versions := make([]int64, 0)
	found := false
	for _, migration := range mig.MigrationProvider().Migrations() {
		if migration.Version <= baselineVersion {
			versions = append(versions, migration.Version)
		}
		if migration.Version == baselineVersion {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("baseline version %q not found", strconv.FormatInt(baselineVersion, 10))
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

// selectedApplyVersions previews the versions an apply will run, before the
// migration lock is taken.
//
// The toVersion bound skips over a version above the bound rather than
// stopping at it, because that is what the migrator does inside the lock
// (migrationsToApply continues past an out-of-bound version). Stopping instead
// would make the preview disagree with the execution on a directory whose
// pending versions are not monotonically ordered.
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
