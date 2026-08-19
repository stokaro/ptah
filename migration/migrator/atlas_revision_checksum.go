package migrator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/internal/atlashash"
)

type ownedAtlasSumContribution struct {
	migration    *Migration
	contribution atlasSumContribution
}

// verifyAppliedMigrationChecksums verifies clean applied revisions without
// confusing an atlas.sum running hash with a per-file content hash. The bool
// reports that at least one row matched a provable applied-history projection
// rather than the current full-directory entry and should be reconciled after
// the requested state change succeeds.
func (m *Migrator) verifyAppliedMigrationChecksums(
	ctx context.Context,
	migrations []*Migration,
) (bool, error) {
	if !m.metadataAvailable || m.legacyRevisionTable {
		return false, nil
	}
	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to read revisions for checksum verification: %w", err)
	}
	revisionsByKey := appliedRevisionsByKey(revisions)
	implicitFloor := implicitAtlasProjectionFloor(migrations, revisions)
	currentProjection, err := atlasAppliedProjectionHashes(migrations, revisionsByKey, implicitFloor)
	if err != nil {
		return false, err
	}

	needsReconcile := false
	var mismatch *ChecksumMismatchError
	for _, migration := range migrations {
		if migration.isAtlasRepeatable() {
			continue
		}
		revision := revisionsByKey[migration.RevisionVersion()]
		if revision.State != migrationStateApplied || revision.Checksum == "" {
			continue
		}
		stored := normalizeAtlasRevisionHash(revision.Checksum)
		if revisionChecksumMatches(stored, migration) {
			continue
		}
		if stored == currentProjection[migration.RevisionVersion()] {
			needsReconcile = true
			continue
		}
		if mismatch == nil {
			mismatch = &ChecksumMismatchError{
				Version:  migration.Version,
				Stored:   stored,
				Computed: migrationRevisionHash(migration),
			}
		}
	}
	if mismatch == nil {
		return needsReconcile, nil
	}
	if atlasCoherentHistoricalProjectionMatches(
		migrations,
		revisionsByKey,
		implicitFloor,
		currentProjection,
	) {
		return true, nil
	}
	return false, mismatch
}

func appliedRevisionsByKey(revisions []MigrationRevision) map[string]MigrationRevision {
	byKey := make(map[string]MigrationRevision, len(revisions))
	for _, revision := range revisions {
		if revision.State == migrationStateApplied {
			byKey[revision.RevisionVersion()] = revision
		}
	}
	return byKey
}

func implicitAtlasProjectionFloor(migrations []*Migration, revisions []MigrationRevision) int64 {
	floor := atlasRevisionBoundary(revisions)
	revisionsByKey := appliedRevisionsByKey(revisions)
	for _, migration := range migrations {
		if !migration.IsCheckpoint {
			continue
		}
		if revision := revisionsByKey[migration.RevisionVersion()]; revision.State == migrationStateApplied && migration.Version > floor {
			floor = migration.Version
		}
	}
	return floor
}

func atlasAppliedProjectionHashes(
	migrations []*Migration,
	revisionsByKey map[string]MigrationRevision,
	implicitFloor int64,
) (map[string]string, error) {
	return atlasProjectionHashes(migrations, func(migration *Migration) bool {
		if migration.Version <= implicitFloor {
			return true
		}
		_, applied := revisionsByKey[migration.RevisionVersion()]
		return applied
	})
}

// atlasCoherentHistoricalProjectionMatches tries each applied-time cutoff as
// one prior applied-set projection. Duplicate non-zero timestamps stay in the
// same cutoff group, which preserves MySQL-family second-precision histories
// without guessing an order inside the group.
//
// A projection matches only when every row whose hash would change still has
// the prior hash. This whole-cohort requirement distinguishes an atomic
// reconciliation rollback from a partially committed or edited history. Rows
// applied after the candidate cutoff must retain the projection from their own
// application-time group; a current hash alone is not historical evidence.
func atlasCoherentHistoricalProjectionMatches(
	migrations []*Migration,
	revisionsByKey map[string]MigrationRevision,
	implicitFloor int64,
	currentProjection map[string]string,
) bool {
	cutoffs := make([]time.Time, 0, len(revisionsByKey))
	seen := make(map[string]struct{}, len(revisionsByKey))
	for _, revision := range revisionsByKey {
		if revision.AppliedAt.IsZero() {
			return false
		}
		key := revision.AppliedAt.UTC().Format(time.RFC3339Nano)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		cutoffs = append(cutoffs, revision.AppliedAt)
	}
	sort.Slice(cutoffs, func(i, j int) bool { return cutoffs[i].Before(cutoffs[j]) })

	projections := make([]map[string]string, len(cutoffs))
	projectionsByAppliedAt := make(map[string]map[string]string, len(cutoffs))
	for index, cutoff := range cutoffs {
		projection, err := atlasProjectionHashes(migrations, func(migration *Migration) bool {
			if migration.Version <= implicitFloor {
				return true
			}
			revision, applied := revisionsByKey[migration.RevisionVersion()]
			return applied && !revision.AppliedAt.After(cutoff)
		})
		if err != nil {
			return false
		}
		projections[index] = projection
		projectionsByAppliedAt[cutoff.UTC().Format(time.RFC3339Nano)] = projection
	}

	matches := 0
	for _, previousProjection := range projections {
		if atlasStoredChecksumsMatchProjectionCohort(
			migrations,
			revisionsByKey,
			previousProjection,
			currentProjection,
			projectionsByAppliedAt,
		) {
			matches++
		}
	}
	return matches == 1
}

func atlasStoredChecksumsMatchProjectionCohort(
	migrations []*Migration,
	revisionsByKey map[string]MigrationRevision,
	previousProjection,
	currentProjection map[string]string,
	projectionsByAppliedAt map[string]map[string]string,
) bool {
	evidence := false
	for _, migration := range migrations {
		if migration.isAtlasRepeatable() || migration.Checksum == "" {
			continue
		}
		revision := revisionsByKey[migration.RevisionVersion()]
		if revision.State != migrationStateApplied || revision.Checksum == "" {
			continue
		}
		stored := normalizeAtlasRevisionHash(revision.Checksum)
		if stored == migrationChecksum(migration.UpSQL) {
			continue
		}
		current, currentOK := currentProjection[migration.RevisionVersion()]
		if !currentOK {
			return false
		}
		previous, previouslyApplied := previousProjection[migration.RevisionVersion()]
		if !previouslyApplied {
			appliedAtKey := revision.AppliedAt.UTC().Format(time.RFC3339Nano)
			applicationProjection := projectionsByAppliedAt[appliedAtKey]
			atApplication, ok := applicationProjection[migration.RevisionVersion()]
			if !ok || stored != atApplication {
				return false
			}
			continue
		}
		if previous == current {
			if stored != current && !revisionChecksumMatches(stored, migration) {
				return false
			}
			continue
		}
		if stored != previous {
			return false
		}
		evidence = true
	}
	return evidence
}

func atlasProjectionHashes(
	migrations []*Migration,
	include func(*Migration) bool,
) (map[string]string, error) {
	contributions := make([]ownedAtlasSumContribution, 0)
	for _, migration := range migrations {
		if !include(migration) {
			continue
		}
		for _, contribution := range migration.atlasSumContributions {
			contributions = append(contributions, ownedAtlasSumContribution{
				migration: migration, contribution: contribution,
			})
		}
	}
	sort.Slice(contributions, func(i, j int) bool {
		return contributions[i].contribution.name < contributions[j].contribution.name
	})

	hashes := make(map[string]string, len(migrations))
	chain := atlashash.NewChain()
	for index, owned := range contributions {
		if index > 0 && owned.contribution.name == contributions[index-1].contribution.name {
			return nil, fmt.Errorf("ambiguous Atlas checksum projection: duplicate source %q", owned.contribution.name)
		}
		var hash string
		if owned.contribution.includeData {
			hash = chain.Add(owned.contribution.name, owned.contribution.data)
		} else {
			hash = chain.AddName(owned.contribution.name)
		}
		if owned.contribution.revisionEntry {
			hashes[owned.migration.RevisionVersion()] = normalizeAtlasRevisionHash(hash)
		}
	}
	return hashes, nil
}

func (m *Migrator) reconcileAppliedMigrationChecksums(
	ctx context.Context,
	migrations []*Migration,
) error {
	// Reconcile only after the requested migration or rollback completed. A
	// pre-execution rewrite would put later clean rows on a prospective chain;
	// a crash or txmode=none failure could then leave that chain claiming a
	// pending migration contributed to the applied history. Post-success
	// reconciliation may leave an older provable projection after a crash, but
	// never advances metadata ahead of database state.
	if m.conn.Writer().IsDryRun() || !m.metadataAvailable || m.legacyRevisionTable {
		return nil
	}
	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return fmt.Errorf("failed to read revisions for checksum reconciliation: %w", err)
	}
	revisionsByKey := appliedRevisionsByKey(revisions)
	projection, err := atlasAppliedProjectionHashes(migrations, revisionsByKey, implicitAtlasProjectionFloor(migrations, revisions))
	if err != nil {
		return err
	}
	updates, err := m.planAppliedMigrationChecksumReconciliation(migrations, revisionsByKey, projection)
	if err != nil {
		return err
	}
	if len(updates) == 0 {
		return nil
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.updateRevisionChecksumSQL())
	if platform.NormalizeDialect(m.connectionDialect()) == platform.ClickHouse {
		if len(updates) > 1 {
			return fmt.Errorf(
				"cannot atomically reconcile %d Atlas checksums on ClickHouse: multi-row transactions are unavailable",
				len(updates),
			)
		}
		update := updates[0]
		if err := executeSQLOn(ctx, m.conn, query, update.checksum, update.revisionArg); err != nil {
			return fmt.Errorf("failed to reconcile checksum for migration %s: %w", update.version, err)
		}
		return nil
	}

	tx, err := m.conn.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin checksum reconciliation transaction: %w", err)
	}
	for _, update := range updates {
		if err := tx.ExecuteSQL(ctx, query, update.checksum, update.revisionArg); err != nil {
			updateErr := fmt.Errorf("failed to reconcile checksum for migration %s: %w", update.version, err)
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return errors.Join(
					updateErr,
					fmt.Errorf("failed to roll back checksum reconciliation transaction: %w", rollbackErr),
				)
			}
			return updateErr
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit checksum reconciliation transaction: %w", err)
	}
	return nil
}

type atlasChecksumUpdate struct {
	version     string
	checksum    string
	revisionArg any
}

func (m *Migrator) planAppliedMigrationChecksumReconciliation(
	migrations []*Migration,
	revisionsByKey map[string]MigrationRevision,
	projection map[string]string,
) ([]atlasChecksumUpdate, error) {
	updates := make([]atlasChecksumUpdate, 0)
	for _, migration := range migrations {
		if migration.isAtlasRepeatable() || migration.Checksum == "" {
			continue
		}
		revision := revisionsByKey[migration.RevisionVersion()]
		if revision.State != migrationStateApplied || revision.Checksum == "" {
			continue
		}
		stored := normalizeAtlasRevisionHash(revision.Checksum)
		if stored == migrationChecksum(migration.UpSQL) {
			continue
		}
		desired, ok := projection[migration.RevisionVersion()]
		if !ok {
			return nil, fmt.Errorf("cannot reconcile Atlas checksum for migration %s: applied projection is unavailable", migration.RevisionVersion())
		}
		if stored == desired {
			continue
		}
		updates = append(updates, atlasChecksumUpdate{
			version:     migration.RevisionVersion(),
			checksum:    desired,
			revisionArg: m.migrationRevisionVersionArg(migration),
		})
	}
	return updates, nil
}

func (m *Migrator) updateRevisionChecksumSQL() string {
	column := "checksum"
	if m.revisionTableFormat.isAtlas() {
		column = "hash"
	}
	return revisionUpdateSQL(m.connectionDialect(), m.qualifiedMigrationsTable(), column+" = ?")
}
