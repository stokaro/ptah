package migrator

import (
	"context"
	"fmt"
	"time"

	"ptah.run/dbschema"
	"ptah.run/internal/revisiontable"
)

func (m *Migrator) baselineProviderMigrations(ctx context.Context, store dbschema.HistoryStore, migrations []*Migration, opts BaselineOptions) error {
	return withProviderHistoryTransaction(ctx, store, "baseline", func(tx dbschema.HistoryStore) error {
		return m.writeProviderBaseline(ctx, tx, migrations, opts)
	})
}

func (m *Migrator) writeProviderBaseline(ctx context.Context, store dbschema.HistoryStore, migrations []*Migration, opts BaselineOptions) error {
	snapshot, err := store.Read(ctx, m.providerHistoryTable())
	if err != nil {
		return err
	}
	if !snapshot.Exists {
		return fmt.Errorf("provider history store disappeared before baseline")
	}
	revisions, err := m.providerHistoryRevisions(snapshot)
	if err != nil {
		return err
	}
	if err := validateBaselineRevisions(revisions, opts); err != nil {
		return err
	}
	for _, migration := range migrations {
		err := m.mutateStoredProviderRevision(ctx, store, migration, func(_ *dbschema.HistoryRecord) (*dbschema.HistoryRecord, error) {
			record := m.providerBaselineRecord(migration)
			return &record, nil
		})
		if err != nil {
			return fmt.Errorf("failed to record baseline revision %d: %w", migration.Version, err)
		}
	}
	return nil
}

func (m *Migrator) providerBaselineRecord(migration *Migration) dbschema.HistoryRecord {
	total := m.migrationStatementCount(migration.UpSQL)
	record := dbschema.HistoryRecord{Version: m.persistedRevisionIdentity(migration), Description: migration.Description,
		State: migrationStateApplied, Direction: string(MigrationDirectionUp), Applied: total, Total: total,
		AppliedAt: time.Now().UTC(), Checksum: migrationRevisionHash(migration)}
	if m.revisionTableFormat.isAtlas() {
		record.Description = migration.atlasFilenameDescription()
		record.AtlasType = uint32(AtlasRevisionTypeBaseline)
		record.Applied, record.Total, record.Checksum = 0, 0, ""
		record.OperatorVersion = migration.atlasOperatorVersion()
		if migration.revisionType() == AtlasRevisionTypeBaseline|AtlasRevisionTypeApplied {
			record.OperatorVersion = revisiontable.SourceBaselineOperatorVersion
		}
	}
	return record
}
