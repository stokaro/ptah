package atlasmigrate

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

// DownOptions configures PrepareDown against an Atlas-format migration
// directory and Atlas revision metadata, mirroring PrepareApply.
type DownOptions struct {
	// Dir is the resolved migration directory path used for diagnostics.
	Dir string
	// FS is the already captured migration filesystem to roll back.
	FS                   fs.FS
	TargetVersion        int64
	DryRun               bool
	RevisionsSchema      string
	MigrationLockTimeout time.Duration
	AtlasEnv             string
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

	mig, err := migrator.NewFSMigrator(
		conn,
		opts.FS,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: opts.AtlasEnv}),
	)
	if err != nil {
		return DownPlan{}, fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationsTable(opts.RevisionsSchema, "").
		WithMigrationLockTimeout(opts.MigrationLockTimeout).
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	// Unlike PrepareApply, the writer's dry-run mode is enabled only after the
	// revision state is read: the migrator short-circuits reads to empty
	// results under writer dry-run, and a down plan is meaningless without the
	// real applied set. Execute skips the rollback under DryRun, and the
	// writer's dry-run mode then guards any residual write path.
	status, err := mig.GetMigrationStatus(ctx)
	if err != nil {
		return DownPlan{}, fmt.Errorf("error getting migration status: %w", err)
	}
	applied, err := mig.GetAppliedMigrations(ctx)
	if err != nil {
		return DownPlan{}, fmt.Errorf("error getting applied migrations: %w", err)
	}
	conn.SchemaWriter().SetDryRun(opts.DryRun)

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

// Execute rolls back the planned migrations. Dry-run and no-op plans return
// metadata without modifying schema state. On failure the returned result
// still carries the reverted prefix so callers can render a partial report.
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
	if p.Noop() || p.DryRun {
		result.EndedAt = time.Now()
		return result, nil
	}

	err := p.mig.MigrateDownTo(ctx, p.opts.TargetVersion)
	result.EndedAt = time.Now()
	result.Reverted = true
	if err != nil {
		result.DownError = err
		result.ErrorText = err.Error()
		result.RevertedVersions = p.revertedVersionsAfterError(ctx)
		return result, fmt.Errorf("error rolling back migrations: %w", err)
	}
	result.RevertedVersions = p.PlannedVersions

	finalStatus, err := p.mig.GetMigrationStatus(ctx)
	if err != nil {
		result.ErrorText = err.Error()
		return result, fmt.Errorf("error getting final migration status: %w", err)
	}
	result.FinalStatus = finalStatus
	return result, nil
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
	return nil
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
