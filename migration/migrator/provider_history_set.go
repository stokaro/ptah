package migrator

import (
	"context"
	"fmt"
	"math"

	"ptah.run/dbschema"
)

func (m *Migrator) setProviderRevisionRows(ctx context.Context, store dbschema.HistoryStore, migrations []*Migration, version int64) (AtlasRevisionSetResult, error) {
	var result AtlasRevisionSetResult
	err := withProviderHistoryTransaction(ctx, store, "set revision", dbschema.HistoryIsolationSerializable, func(tx dbschema.HistoryStore) error {
		snapshot, err := tx.Read(ctx, m.providerHistoryTable())
		if err != nil {
			return err
		}
		if !snapshot.Exists {
			return fmt.Errorf("provider history store disappeared before set revision")
		}
		existing, err := m.providerHistoryRevisions(snapshot)
		if err != nil {
			return err
		}
		result, _, err = m.planRevisionSet(existing, migrations, version)
		if err != nil {
			return err
		}
		return m.writeProviderRevisionSet(ctx, tx, snapshot.Records, migrations, result)
	})
	if err != nil {
		// A failed or uncertain commit must not report a successful history set.
		return AtlasRevisionSetResult{}, err
	}
	return result, nil
}

func (m *Migrator) writeProviderRevisionSet(ctx context.Context, store dbschema.HistoryStore, records []dbschema.HistoryRecord, migrations []*Migration, plan AtlasRevisionSetResult) error {
	before := make(map[string]dbschema.HistoryRecord, len(records))
	for _, record := range records {
		before[record.Version] = record
	}
	for _, removal := range plan.Removed {
		record, exists := before[removal.RevisionVersion]
		if !exists {
			return fmt.Errorf("provider revision %q disappeared before removal", removal.RevisionVersion)
		}
		if err := store.CompareAndSwap(ctx, m.providerHistoryTable(), &record, nil); err != nil {
			return fmt.Errorf("failed to remove provider revision %q: %w", record.Version, err)
		}
	}
	selected := make(map[string]*Migration, len(migrations))
	for _, migration := range migrations {
		selected[m.persistedRevisionIdentity(migration)] = migration
	}
	for _, change := range plan.Set {
		migration := selected[change.RevisionVersion]
		if migration == nil {
			return fmt.Errorf("provider set revision %q has no selected migration", change.RevisionVersion)
		}
		var expected *dbschema.HistoryRecord
		if record, exists := before[change.RevisionVersion]; exists {
			expected = &record
		}
		after, err := m.providerSetRecord(migration, expected)
		if err != nil {
			return err
		}
		if _, err := m.providerHistoryRevision(after); err != nil {
			return err
		}
		if err := store.CompareAndSwap(ctx, m.providerHistoryTable(), expected, &after); err != nil {
			return fmt.Errorf("failed to set provider revision %q: %w", change.RevisionVersion, err)
		}
	}
	return nil
}

func (m *Migrator) providerSetRecord(migration *Migration, before *dbschema.HistoryRecord) (dbschema.HistoryRecord, error) {
	var after dbschema.HistoryRecord
	if before != nil {
		after = *before
	} else {
		after = m.providerBaselineRecord(migration)
	}
	if !m.revisionTableFormat.isAtlas() {
		after.State, after.Direction = migrationStateApplied, string(MigrationDirectionUp)
		after.Applied, after.Checksum = after.Total, migrationRevisionHash(migration)
		return after, nil
	}
	// Atlas set changes an existing row's type only, retaining its progress
	// and diagnostics. Missing rows carry the current source hash and operator.
	revisionType := migration.manuallySetRevisionType()
	if revisionType > math.MaxUint32 {
		return dbschema.HistoryRecord{}, fmt.Errorf("Atlas revision type exceeds the provider history range")
	}
	after.AtlasType = uint32(revisionType)
	if before == nil {
		after.Checksum, after.OperatorVersion = migrationRevisionHash(migration), migration.atlasOperatorVersion()
	}
	return after, nil
}
