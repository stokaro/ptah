package migrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
)

// withProviderHistoryTransaction keeps maintenance batches atomic without
// granting schema execution or retrying an uncertain mutation or commit.
func withProviderHistoryTransaction(ctx context.Context, store dbschema.HistoryStore, operation string, isolation dbschema.HistoryIsolation, use func(dbschema.HistoryStore) error) error {
	owner, ok := store.(dbschema.HistoryTransactor)
	if !ok {
		return fmt.Errorf("%w: %s requires provider history transactions", ptaherr.ErrUnsupportedFeature, operation)
	}
	tx, err := owner.BeginHistoryTransaction(ctx, isolation)
	if err != nil {
		return fmt.Errorf("failed to begin %s transaction: %w", operation, err)
	}
	if err := use(tx); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit %s transaction: %w", operation, errors.Join(err, tx.Rollback()))
	}
	return nil
}

func (m *Migrator) forceAppliedProviderRevision(ctx context.Context, migration *Migration) (bool, error) {
	store, err := m.conn.HistoryStore()
	if err != nil || store == nil {
		return err != nil, err
	}
	if m.conn.Writer().IsDryRun() {
		return true, nil
	}
	return true, m.mutateStoredProviderRevision(ctx, store, migration, func(_ *dbschema.HistoryRecord) (*dbschema.HistoryRecord, error) {
		record, err := m.pendingProviderRevision(migration, time.Now(), 0)
		if err != nil {
			return nil, err
		}
		record.State, record.Applied = migrationStateApplied, record.Total
		record.Error, record.ErrorStatement = "", ""
		record.Checksum, record.PartialHashes = migrationRevisionHash(migration), nil
		return &record, nil
	})
}

func (m *Migrator) reconcileProviderChecksums(ctx context.Context, store dbschema.HistoryStore, migrations []*Migration) error {
	return withProviderHistoryTransaction(ctx, store, "checksum reconciliation", dbschema.HistoryIsolationDefault, func(tx dbschema.HistoryStore) error {
		snapshot, err := tx.Read(ctx, m.providerHistoryTable())
		if err != nil {
			return fmt.Errorf("failed to read revisions for checksum reconciliation: %w", err)
		}
		if !snapshot.Exists {
			return fmt.Errorf("provider history store disappeared before checksum reconciliation")
		}
		revisions, err := m.providerHistoryRevisions(snapshot)
		if err != nil {
			return err
		}
		updates, err := m.checksumReconciliationPlan(migrations, revisions)
		if err != nil {
			return err
		}
		return m.writeProviderChecksums(ctx, tx, snapshot.Records, updates)
	})
}

func (m *Migrator) writeProviderChecksums(ctx context.Context, store dbschema.HistoryStore, records []dbschema.HistoryRecord, updates []atlasChecksumUpdate) error {
	byIdentity := make(map[string]dbschema.HistoryRecord, len(records))
	for _, record := range records {
		byIdentity[record.Version] = record
	}
	for _, update := range updates {
		before, ok := byIdentity[update.identity]
		if !ok || m.providerRecordState(before) != migrationStateApplied {
			return fmt.Errorf("provider revision %s is not applied during checksum reconciliation", update.version)
		}
		after := before
		after.Checksum = update.checksum
		if err := store.CompareAndSwap(ctx, m.providerHistoryTable(), &before, &after); err != nil {
			return fmt.Errorf("failed to reconcile checksum for migration %s: %w", update.version, err)
		}
	}
	return nil
}
