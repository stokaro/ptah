package migrator

import (
	"context"
	"fmt"
	"sort"
	"time"

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
		if atlasHistoricalProjectionMatches(
			migrations,
			revisionsByKey,
			implicitFloor,
			migration,
			revision,
			stored,
		) {
			needsReconcile = true
			continue
		}
		return false, &ChecksumMismatchError{
			Version:  migration.Version,
			Stored:   stored,
			Computed: migrationRevisionHash(migration),
		}
	}
	return needsReconcile, nil
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

// atlasHistoricalProjectionMatches tries each provable applied-history cutoff
// from the target's own application through the newest row. A later cutoff is
// needed when Ptah reconciled this target after one insertion and Atlas then
// applied another insertion without rewriting the target row.
func atlasHistoricalProjectionMatches(
	migrations []*Migration,
	revisionsByKey map[string]MigrationRevision,
	implicitFloor int64,
	target *Migration,
	targetRevision MigrationRevision,
	stored string,
) bool {
	if !atlasAppliedOrderIsProvable(revisionsByKey) || targetRevision.AppliedAt.IsZero() {
		return false
	}

	cutoffs := make([]time.Time, 0, len(revisionsByKey))
	for _, revision := range revisionsByKey {
		if !revision.AppliedAt.Before(targetRevision.AppliedAt) {
			cutoffs = append(cutoffs, revision.AppliedAt)
		}
	}
	sort.Slice(cutoffs, func(i, j int) bool { return cutoffs[i].Before(cutoffs[j]) })
	for _, cutoff := range cutoffs {
		hashes, err := atlasProjectionHashes(migrations, func(migration *Migration) bool {
			if migration.Version <= implicitFloor || migration.RevisionVersion() == target.RevisionVersion() {
				return true
			}
			revision, applied := revisionsByKey[migration.RevisionVersion()]
			return applied && !revision.AppliedAt.After(cutoff)
		})
		if err != nil {
			return false
		}
		if hashes[target.RevisionVersion()] == stored {
			return true
		}
	}
	return false
}

// atlasAppliedOrderIsProvable rejects historical reconstruction when the
// revision rows do not establish a strict order. The current applied-set
// projection does not need timestamps; this guard applies only to recovery
// after a migration was recorded successfully but checksum reconciliation did
// not finish, including a later insertion by an interoperating Atlas process.
// Guessing through a zero or duplicate timestamp could turn an edited applied
// file into an accepted history.
func atlasAppliedOrderIsProvable(revisionsByKey map[string]MigrationRevision) bool {
	seen := make(map[string]struct{}, len(revisionsByKey))
	for _, revision := range revisionsByKey {
		if revision.AppliedAt.IsZero() {
			return false
		}
		key := revision.AppliedAt.UTC().Format(time.RFC3339Nano)
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
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
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.updateRevisionChecksumSQL())
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
			return fmt.Errorf("cannot reconcile Atlas checksum for migration %s: applied projection is unavailable", migration.RevisionVersion())
		}
		if stored == desired {
			continue
		}
		if err := executeSQLOutsideTransaction(
			ctx,
			m.conn,
			query,
			desired,
			m.migrationRevisionVersionArg(migration),
		); err != nil {
			return fmt.Errorf("failed to reconcile checksum for migration %s: %w", migration.RevisionVersion(), err)
		}
	}
	return nil
}

func (m *Migrator) updateRevisionChecksumSQL() string {
	column := "checksum"
	if m.revisionTableFormat.isAtlas() {
		column = "hash"
	}
	return revisionUpdateSQL(m.connectionDialect(), m.qualifiedMigrationsTable(), column+" = ?")
}
