package migrator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/atlashash"
)

type ownedAtlasSumContribution struct {
	migration    *Migration
	contribution atlasSumContribution
}

// VerifyAppliedChecksums reports whether the checksums recorded for the applied
// revisions are ones the current migration directory accounts for.
//
// It is the read-only half of what MigrateUp does before it applies anything,
// exported so a caller deciding whether native Ptah may take over an existing
// history asks the question through the rule that history was written under
// (stokaro/ptah#1215). An adoption check that re-derived "does this hash match"
// would be a second interpreter of Atlas revision checksums, and the two would
// disagree the first time either learned something -- the atlas.sum running
// hash accepted for a history the Atlas community binary wrote, say, which a
// per-file content hash never reproduces.
//
// Two error types name a row this directory cannot vouch for, and a caller
// matching only one of them handles half the question:
//
//   - *MissingMigrationError, for the first applied revision the directory
//     holds no file for. It is reported before any hash is compared, because
//     there is nothing to compare;
//   - *ChecksumMismatchError, for the first applied revision whose file no
//     longer accounts for what the row recorded.
//
// Any other non-nil error is a failure to ask the question rather than an
// answer to it. The bool reports that at least one row matched a provable
// applied-history projection rather than the current full-directory entry:
// those rows verify, and a writer that proceeds reconciles them. Nothing here
// writes, so a caller in a preflight sees the fact without acting on it.
//
// Callers wanting the guarantee that a preflight cannot alter what it inspects
// should put the connection in dry-run mode first: this calls Initialize, which
// creates an absent revision table and alters an existing one into the current
// layout.
func (m *Migrator) VerifyAppliedChecksums(ctx context.Context) (bool, error) {
	if err := m.Initialize(ctx); err != nil {
		return false, fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	return m.verifyAppliedMigrationChecksums(ctx, m.MigrationProvider().Migrations())
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
	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to read revisions for checksum verification: %w", err)
	}
	classified, err := m.classifyAppliedChecksums(migrations, revisions)
	if err != nil {
		return false, err
	}
	// Before the mismatches: a revision with no file cannot be compared at all,
	// so no projection explains it and reporting a hash difference elsewhere
	// first would name the smaller problem.
	if len(classified.missing) > 0 {
		return false, newMissingMigrationError(classified.missing[0])
	}
	if len(classified.mismatches) == 0 {
		return classified.needsReconcile, nil
	}
	if classified.coherentProjection {
		return true, nil
	}
	return false, classified.mismatches[0]
}

// appliedChecksumClassification is one pass of the applied-checksum rule over
// every migration, rather than the first answer that rule produces.
//
// VerifyAppliedChecksums wants the first mismatch and the status contract wants
// all of them, and both have to be the same rule: an Atlas revision hash is a
// running hash over every preceding file, and a second interpreter of it
// disagrees with the first the moment either learns something.
type appliedChecksumClassification struct {
	// mismatches are the applied revisions no current file accounts for, in
	// directory order.
	mismatches []*ChecksumMismatchError
	// needsReconcile reports that a row matched a provable applied-history
	// projection rather than the current full-directory entry.
	needsReconcile bool
	// coherentProjection reports that the mismatches are explained by a
	// coherent historical projection, which makes them not mismatches at all.
	coherentProjection bool
	// missing are the applied revisions the directory holds no migration for,
	// in recorded order. They are a different failure from a mismatch: there is
	// no file to compare, so no projection can explain them.
	missing []MigrationRevision
}

func (c appliedChecksumClassification) mismatchedKeys() map[string]struct{} {
	if len(c.mismatches) == 0 || c.coherentProjection {
		return nil
	}
	keys := make(map[string]struct{}, len(c.mismatches))
	for _, mismatch := range c.mismatches {
		keys[mismatch.RevisionKey] = struct{}{}
	}
	return keys
}

func (m *Migrator) classifyAppliedChecksums(
	migrations []*Migration,
	revisions []MigrationRevision,
) (appliedChecksumClassification, error) {
	if !m.metadataAvailable || m.legacyRevisionTable {
		return appliedChecksumClassification{}, nil
	}
	revisionsByKey := appliedRevisionsByKey(revisions)
	implicitFloor := implicitAtlasProjectionFloor(migrations, revisions)
	currentProjection, err := atlasAppliedProjectionHashes(migrations, revisionsByKey, implicitFloor)
	if err != nil {
		return appliedChecksumClassification{}, err
	}

	classified := appliedChecksumClassification{
		missing: m.missingAppliedRevisions(migrations, revisions),
	}
	for _, migration := range migrations {
		if migration.isAtlasRepeatable() {
			continue
		}
		key := migration.RevisionVersion()
		revision := revisionsByKey[key]
		if revision.State != migrationStateApplied || revision.Checksum == "" {
			continue
		}
		stored := normalizeAtlasRevisionHash(revision.Checksum)
		if revisionChecksumMatches(stored, migration) {
			continue
		}
		if stored == currentProjection[key] {
			classified.needsReconcile = true
			continue
		}
		classified.mismatches = append(classified.mismatches, &ChecksumMismatchError{
			Version:             migration.Version,
			RevisionKey:         key,
			Stored:              stored,
			Computed:            migrationRevisionHash(migration),
			Description:         migration.Description,
			ConvertedRepeatable: migration.Version == ConvertedFlywayRepeatableVersion,
		})
	}
	if len(classified.mismatches) > 0 {
		classified.coherentProjection = atlasCoherentHistoricalProjectionMatches(
			migrations,
			revisionsByKey,
			implicitFloor,
			currentProjection,
		)
	}
	return classified, nil
}

// missingAppliedRevisions returns the applied revisions the migration directory
// holds no file for, in recorded order.
//
// The absence is the whole finding: the history records a migration the
// directory cannot show, so nothing can say what it did, replay it, or roll it
// back. It is only a finding where the provider is the directory, so a provider
// that does not promise to be the whole history is not asked; see
// [wholeHistoryProvider].
//
// There is no exemption for a checkpoint, and none is needed. A checkpoint
// bootstraps a database that has applied nothing and records its own revision
// alone, writing no row for any version it covers, so a directory pruned down
// to the checkpoint leaves nothing here to be absent. `migrations baseline`
// records the directory's own migrations. Neither produces a row without a
// file, which is what lets this ask the plain question.
//
// Identity is the revision key rather than the numeric version, because the two
// answer different questions: the key is the exact token the row and the file
// agree on, while the version is an ordering number a converted directory may
// assign far from it.
//
// The rule is asked of a native revision history only. An Atlas-format history
// is reshaped by compatibility features this package cannot see the marks of --
// a surviving Flyway `B` file squashes the migrations it supersedes, and the
// rows they wrote stay recorded as ordinary applied rows carrying no baseline
// type, while the file that replaced them can hold any ordering number. What
// makes that legitimate lives in internal/cli/atlas, so deciding it here would
// refuse an intact directory. stokaro/ptah#3442 carries the Atlas half.
func (m *Migrator) missingAppliedRevisions(
	migrations []*Migration,
	revisions []MigrationRevision,
) []MigrationRevision {
	provider, ok := m.migrationProvider.(wholeHistoryProvider)
	if !ok || !provider.describesWholeHistory() {
		return nil
	}
	if m.revisionTableFormat.isAtlas() {
		return nil
	}
	held := make(map[string]struct{}, len(migrations))
	for _, migration := range migrations {
		held[migration.RevisionVersion()] = struct{}{}
	}
	var missing []MigrationRevision
	for _, revision := range revisions {
		if revision.State != migrationStateApplied {
			continue
		}
		if _, ok := held[revision.RevisionVersion()]; ok {
			continue
		}
		missing = append(missing, revision)
	}
	return missing
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
