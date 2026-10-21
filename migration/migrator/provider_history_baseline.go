package migrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/revisiontable"
)

func (m *Migrator) baselineProviderMigrations(ctx context.Context, store dbschema.HistoryStore, migrations []*Migration, opts BaselineOptions) error {
	owner, ok := store.(dbschema.HistoryTransactor)
	if !ok {
		return fmt.Errorf("%w: baseline requires provider history transactions", ptaherr.ErrUnsupportedFeature)
	}
	tx, err := owner.BeginHistoryTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin baseline transaction: %w", err)
	}
	if err := m.writeProviderBaseline(ctx, tx, migrations, opts); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit baseline transaction: %w", errors.Join(err, tx.Rollback()))
	}
	return nil
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
