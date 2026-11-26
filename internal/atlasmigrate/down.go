package atlasmigrate

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"time"

	"ptah.run/dbschema"
	"ptah.run/internal/rollbacklimit"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// DownOptions configures PrepareDown against an Atlas-format migration
// directory and Atlas revision metadata, mirroring PrepareApply.
type DownOptions struct {
	// Dir is the resolved migration directory path used for diagnostics.
	Dir string
	// FS is the already captured migration filesystem to roll back.
	FS            fs.FS
	TargetVersion int64
	// Limit, when above 0, reverts the last Limit applied migrations, and the
	// version the rollback stops at is read from the history; see
	// [rollbacklimit.Target]. It is refused beside a TargetVersion above 0.
	Limit                uint64
	DryRun               bool
	RevisionsSchema      string
	MigrationLockTimeout time.Duration
	// MigrationsEngine names the storage engine the revision table is created
	// with, threaded for the same reason SkipChecks below is.
	MigrationsEngine string
	AtlasEnv         string
	// SkipChecks bypasses the pre-migration checks the down bodies carry.
	//
	// The formatted path builds its own migrator here rather than going through
	// the native command, so an option the native path applies has to be
	// threaded in explicitly or it is silently dropped -- which is how
	// --skip-checks came to be accepted and ignored on this path
	// (stokaro/ptah#1621).
	SkipChecks bool
	// ValueRounding selects how the rollback treats rounded-values checks.
	// The zero value evaluates them; see [migrator.Migrator.WithValueRounding].
	ValueRounding migrator.ValueRounding
	// OwnTransactions rolls the directory back the way a runner that opens no
	// transaction around a file runs it, as golang-migrate does; see
	// [migrator.WithOwnTransactions] and [ApplyOptions.OwnTransactions].
	OwnTransactions bool
}

// DownPlan is the selected Atlas migrate down work prepared from the migration
// directory and current revision state. PlannedVersions is in revert order
// (newest first).
type DownPlan struct {
	Status          *migrator.MigrationStatus
	Migrations      []*migrator.Migration
	PlannedVersions []int64
	CurrentVersion  int64
	TargetVersion   int64
	DryRun          bool
	StartedAt       time.Time

	mig  *migrator.Migrator
	opts DownOptions
}

// DownResult contains execution metadata needed by CLI output and Atlas
// template rendering. RevertedVersions lists the versions that were actually
// reverted, in revert order; on failure it is the successfully reverted prefix
// of PlannedVersions.
type DownResult struct {
	Status           *migrator.MigrationStatus
	FinalStatus      *migrator.MigrationStatus
	Migrations       []*migrator.Migration
	PlannedVersions  []int64
	RevertedVersions []int64
	CurrentVersion   int64
	TargetVersion    int64
	Reverted         bool
	DryRun           bool
	StartedAt        time.Time
	EndedAt          time.Time
	ErrorText        string
	DownError        error
}

// PrepareDown builds the Atlas-format migrator and selects the applied
// migrations above the target version, in revert order.
func PrepareDown(ctx context.Context, conn *dbschema.DatabaseConnection, opts DownOptions) (DownPlan, error) {
	if err := validateDownOptions(conn, opts); err != nil {
		return DownPlan{}, err
	}
	startedAt := time.Now()

	providerOptions := []migrator.FSProviderOption{
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
		migrator.WithAtlasTemplateData(migrationfile.AtlasTemplateData{Env: opts.AtlasEnv}),
		// Read as the native rollback reads it: the Atlas format gives an
		// R__ file the version R either way, and a database whose record of
		// repeatable migrations holds a row is refused by the rollback, which
		// says how to clear it, rather than as a run that ignores the record.
		migrator.WithRepeatableMigrations(),
		// A revision named in a refusal reads as the community binary names it.
		migrator.WithAtlasVerbatimDescriptions(),
	}
	if opts.OwnTransactions {
		providerOptions = append(providerOptions, migrator.WithOwnTransactions())
	}
	mig, err := migrator.NewFSMigrator(conn, opts.FS, providerOptions...)
	if err != nil {
		return DownPlan{}, fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationsTable(opts.RevisionsSchema, "").
		WithMigrationsEngine(opts.MigrationsEngine).
		WithMigrationLockTimeout(opts.MigrationLockTimeout).
		WithSkipChecks(opts.SkipChecks).
		WithValueRounding(opts.ValueRounding).
		WithLogger(compatMigratorLogger())

	conn.SchemaWriter().SetDryRun(opts.DryRun)
	status, err := mig.GetMigrationStatus(ctx)
	if err != nil {
		return DownPlan{}, fmt.Errorf("error getting migration status: %w", err)
	}
	applied, err := mig.GetAppliedMigrations(ctx)
	if err != nil {
		return DownPlan{}, fmt.Errorf("error getting applied migrations: %w", err)
	}
	if opts.Limit > 0 {
		opts.TargetVersion = rollbacklimit.Target(applied, opts.Limit)
	}
	return DownPlan{
		Status:          status,
		Migrations:      mig.MigrationProvider().Migrations(),
		PlannedVersions: plannedDownVersions(applied, opts.TargetVersion),
		CurrentVersion:  status.CurrentVersion,
		TargetVersion:   opts.TargetVersion,
		DryRun:          opts.DryRun,
		StartedAt:       startedAt,
		mig:             mig,
		opts:            opts,
	}, nil
}

// Noop reports whether the plan has no applied migrations above the target
// version.
func (p DownPlan) Noop() bool {
	return len(p.PlannedVersions) == 0
}

// ErrDownHistoryChanged reports a rollback whose applied history changed
// between [PrepareDown] and [DownPlan.Execute]. Nothing is reverted.
var ErrDownHistoryChanged = errors.New("the applied history changed after the rollback was planned")

// Execute rolls back the planned migrations. Dry-run and no-op plans return
// metadata without modifying schema state. On failure the returned result
// still carries the reverted prefix so callers can render a partial report.
//
// PrepareDown reads the history without the migration lock, so Execute reads
// it again under the lock and refuses, with [ErrDownHistoryChanged] and before
// anything is reverted, when the migrations above the target are no longer
// PlannedVersions. A run that counts back would otherwise revert a migration
// applied in between, beside the ones it reports, and a run to a version would
// revert migrations its plan did not show. This is the check native
// `migrations down` makes when it takes the lock again after its prompt.
func (p DownPlan) Execute(ctx context.Context) (DownResult, error) {
	result := DownResult{
		Status:          p.Status,
		Migrations:      p.Migrations,
		PlannedVersions: p.PlannedVersions,
		CurrentVersion:  p.CurrentVersion,
		TargetVersion:   p.TargetVersion,
		DryRun:          p.DryRun,
		StartedAt:       p.StartedAt,
	}
	executionStarted := false
	err := p.mig.MigrateDownToWithPreflight(ctx, p.opts.TargetVersion, func(_ context.Context, plan migrator.MigrationPlan) error {
		if err := p.refuseChangedHistory(plan.Versions); err != nil {
			return err
		}
		executionStarted = len(plan.Versions) > 0
		return nil
	})
	result.EndedAt = time.Now()
	if err != nil {
		result.DownError = err
		result.ErrorText = err.Error()
		result.Reverted = executionStarted && !p.DryRun
		if result.Reverted {
			result.RevertedVersions = p.revertedVersionsAfterError(ctx)
		}
		return result, fmt.Errorf("error rolling back migrations: %w", err)
	}
	if p.DryRun || !executionStarted {
		return result, nil
	}
	result.Reverted = true
	result.RevertedVersions = p.PlannedVersions

	finalStatus, err := p.mig.GetMigrationStatus(ctx)
	if err != nil {
		result.ErrorText = err.Error()
		return result, fmt.Errorf("error getting final migration status: %w", err)
	}
	result.FinalStatus = finalStatus
	return result, nil
}

// refuseChangedHistory returns [ErrDownHistoryChanged] when locked, the
// versions the migrator reverts under the lock, are not PlannedVersions.
func (p DownPlan) refuseChangedHistory(locked []int64) error {
	planned := newestFirst(p.PlannedVersions)
	now := newestFirst(locked)
	if slices.Equal(planned, now) {
		return nil
	}
	return fmt.Errorf("%w: it reverts %v, and the migrations applied above version %s are now %v; "+
		"nothing was rolled back, run migrate down again", ErrDownHistoryChanged,
		p.mig.VersionLabels(planned), p.mig.VersionLabel(p.TargetVersion), p.mig.VersionLabels(now))
}

// newestFirst returns a copy of versions in revert order.
func newestFirst(versions []int64) []int64 {
	return slices.SortedFunc(slices.Values(versions), func(a, b int64) int { return cmp.Compare(b, a) })
}

// revertedVersionsAfterError re-reads the revision state so a partial failure
// reports exactly the versions that were cleanly reverted: versions that are
// no longer applied, excluding the dirty revision the failure left behind
// (a failed rollback drops out of the applied set but was not reverted). When
// even that read fails, no version is reported as reverted.
func (p DownPlan) revertedVersionsAfterError(ctx context.Context) []int64 {
	stillApplied, err := p.mig.GetAppliedMigrations(ctx)
	if err != nil {
		return nil
	}
	status, err := p.mig.GetMigrationStatus(ctx)
	if err != nil {
		return nil
	}
	reverted := make([]int64, 0, len(p.PlannedVersions))
	for _, version := range p.PlannedVersions {
		if slices.Contains(stillApplied, version) {
			continue
		}
		if status.DirtyRevision != nil && status.DirtyRevision.Version == version {
			continue
		}
		reverted = append(reverted, version)
	}
	return reverted
}

func validateDownOptions(conn *dbschema.DatabaseConnection, opts DownOptions) error {
	if conn == nil {
		return errors.New("migrate down requires database connection")
	}
	if strings.TrimSpace(opts.Dir) == "" {
		return errors.New("migrate down requires migration directory")
	}
	if opts.FS == nil {
		return errors.New("migrate down requires migration filesystem")
	}
	if opts.TargetVersion < 0 {
		return errors.New("migrate down target version must be greater than or equal to zero")
	}
	if opts.Limit > 0 && opts.TargetVersion > 0 {
		return errors.New("migrate down takes a target version or a limit, not both")
	}
	return nil
}

// DownStop is what one `migrate down` invocation says about where the
// rollback stops: the amount argument, --to-version and --to-tag.
type DownStop struct {
	// Amount is the amount argument, 0 when none was given.
	Amount uint64
	// ToVersion is the --to-version value. ToVersionGiven reports that the
	// flag was typed, an empty value included, or that PTAH_TO_VERSION gave a
	// value. The command refuses an empty or blank PTAH_TO_VERSION before it
	// builds a DownStop, so an empty value here was typed.
	ToVersion      string
	ToVersionGiven bool
	// ToTag is the --to-tag value, and ToTagGiven reports the same for --to-tag
	// and PTAH_TO_TAG.
	ToTag      string
	ToTagGiven bool
}

// Limit returns how many applied migrations the rollback reverts, counting
// back from the last one: the amount when one was given, 1 when nothing names
// where to stop, and 0 when --to-version or --to-tag names the stop instead.
// The default of 1 is the documented one, which reverts the last applied
// file: https://atlasgo.io/versioned/down.
//
// It refuses two of the three together, since each names where to stop and
// preferring one would roll back to a version the operator did not choose,
// and a version or tag that is empty or blank, which names no stop and would
// otherwise read as absent.
func (s DownStop) Limit() (uint64, error) {
	if s.ToVersionGiven && strings.TrimSpace(s.ToVersion) == "" {
		return 0, errors.New("--to-version is empty: give it a version, or leave it out to revert the last applied migration")
	}
	if s.ToTagGiven && strings.TrimSpace(s.ToTag) == "" {
		return 0, errors.New("--to-tag is empty: give it a tag, or leave it out to revert the last applied migration")
	}
	if s.ToVersionGiven && s.ToTagGiven {
		return 0, errors.New("--to-version and --to-tag both name where to stop; pass one")
	}
	if s.Amount > 0 && s.ToVersionGiven {
		return 0, errors.New("the amount argument and --to-version both name where to stop; pass one")
	}
	if s.Amount > 0 && s.ToTagGiven {
		return 0, errors.New("the amount argument and --to-tag both name where to stop; pass one")
	}
	if s.ToVersionGiven || s.ToTagGiven {
		return 0, nil
	}
	return max(s.Amount, 1), nil
}

// plannedDownVersions selects the applied versions above the target, newest
// first, matching the order the migrator reverts them in.
func plannedDownVersions(applied []int64, targetVersion int64) []int64 {
	planned := make([]int64, 0, len(applied))
	for _, version := range applied {
		if version > targetVersion {
			planned = append(planned, version)
		}
	}
	slices.SortFunc(planned, func(a, b int64) int { return cmp.Compare(b, a) })
	return planned
}
