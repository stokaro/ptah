package migrator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/revisiontext"
)

// mutateProviderRevision reads and replaces an exact record on the supplied
// execution handle. It never retries a conflict or an uncertain acknowledgment.
// The shared migrator decides the transition; storage only compares its facts.
func (m *Migrator) mutateProviderRevision(ctx context.Context, conn *dbschema.DatabaseConnection, migration *Migration, change func(*dbschema.HistoryRecord) (*dbschema.HistoryRecord, error)) (bool, error) {
	store, err := conn.HistoryStore()
	if err != nil || store == nil {
		return err != nil, err
	}
	return true, m.mutateStoredProviderRevision(ctx, store, migration, change)
}

func (m *Migrator) mutateStoredProviderRevision(ctx context.Context, store dbschema.HistoryStore, migration *Migration, change func(*dbschema.HistoryRecord) (*dbschema.HistoryRecord, error)) error {
	snapshot, err := store.Read(ctx, m.providerHistoryTable())
	if err != nil {
		return err
	}
	if !snapshot.Exists {
		return fmt.Errorf("provider history store disappeared before revision mutation")
	}
	var before *dbschema.HistoryRecord
	seen := make(map[string]bool, len(snapshot.Records))
	for _, record := range snapshot.Records {
		if seen[record.Version] {
			return fmt.Errorf("provider history contains duplicate revision identities")
		}
		seen[record.Version] = true
		if _, err := m.providerHistoryRevision(record); err != nil {
			return err
		}
		if record.Version == m.persistedRevisionIdentity(migration) {
			before = &record
		}
	}
	after, err := change(before)
	if err != nil {
		return err
	}
	if after != nil {
		if m.revisionTableFormat.isAtlas() {
			after.State = atlasRevisionState(MigrationRevision{Applied: after.Applied, Total: after.Total, Error: after.Error, OperatorVersion: after.OperatorVersion})
		}
		if _, err := m.providerHistoryRevision(*after); err != nil {
			return err
		}
	}
	return store.CompareAndSwap(ctx, m.providerHistoryTable(), before, after)
}

func (m *Migrator) editProviderRevision(ctx context.Context, conn *dbschema.DatabaseConnection, migration *Migration, edit func(*dbschema.HistoryRecord) error) (bool, error) {
	return m.mutateProviderRevision(ctx, conn, migration, func(before *dbschema.HistoryRecord) (*dbschema.HistoryRecord, error) {
		if before == nil {
			return nil, fmt.Errorf("provider history revision %s disappeared before update", m.persistedRevisionIdentity(migration))
		}
		after := *before
		after.PartialHashes = slices.Clone(before.PartialHashes)
		if err := edit(&after); err != nil {
			return nil, err
		}
		return &after, nil
	})
}

func (m *Migrator) pendingProviderRevision(migration *Migration, startedAt time.Time, applied int) (dbschema.HistoryRecord, error) {
	record := dbschema.HistoryRecord{Version: m.persistedRevisionIdentity(migration), Description: migration.Description,
		State: migrationStatePending, Direction: string(MigrationDirectionUp), Applied: applied, Total: m.migrationStatementCount(migration.UpSQL),
		AppliedAt: startedAt.UTC().Round(0), Error: unknownStatementOutcomeError,
		Checksum: m.dirtyRevisionChecksum(migration, MigrationDirectionUp, applied)}
	if m.revisionTableFormat.isAtlas() {
		record.Description = migration.atlasFilenameDescription()
		revisionType := migration.revisionType()
		if revisionType > math.MaxUint32 {
			return dbschema.HistoryRecord{}, fmt.Errorf("Atlas revision type exceeds the provider history range")
		}
		record.AtlasType = uint32(revisionType)
		record.Checksum = migrationRevisionHash(migration)
		record.OperatorVersion = migration.atlasOperatorVersion()
		record.PartialHashes, _ = atlasPartialHashValues(migration.UpSQL, m.connectionDialect(), applied, record.Total)
	}
	return record, nil
}

func (m *Migrator) beginProviderRevision(ctx context.Context, conn *dbschema.DatabaseConnection, migration *Migration, startedAt time.Time) (bool, error) {
	return m.mutateProviderRevision(ctx, conn, migration, func(before *dbschema.HistoryRecord) (*dbschema.HistoryRecord, error) {
		if before != nil {
			return nil, fmt.Errorf("provider history revision %s already exists", before.Version)
		}
		record, err := m.pendingProviderRevision(migration, startedAt, 0)
		return &record, err
	})
}

func (m *Migrator) restartProviderRevision(ctx context.Context, conn *dbschema.DatabaseConnection, migration *Migration, startedAt time.Time, applied int) (bool, error) {
	return m.editProviderRevision(ctx, conn, migration, func(record *dbschema.HistoryRecord) error {
		if record.State == migrationStateApplied || record.Error == unknownStatementOutcomeError {
			return fmt.Errorf("provider history revision %s changed before retry; inspect durable state", record.Version)
		}
		pending, err := m.pendingProviderRevision(migration, startedAt, applied)
		*record = pending
		return err
	})
}

func (m *Migrator) completeProviderRevision(ctx context.Context, conn *dbschema.DatabaseConnection, migration *Migration, startedAt time.Time) (bool, error) {
	return m.editProviderRevision(ctx, conn, migration, func(record *dbschema.HistoryRecord) error {
		record.State, record.Direction = migrationStateApplied, string(MigrationDirectionUp)
		record.Total = m.migrationStatementCount(migration.UpSQL)
		record.Applied, record.Error, record.ErrorStatement = record.Total, "", ""
		record.ExecutionTime, record.Checksum = time.Since(startedAt), migrationRevisionHash(migration)
		record.PartialHashes = nil
		if m.revisionTableFormat.isAtlas() {
			record.OperatorVersion = migration.atlasOperatorVersion()
		} else {
			record.AppliedAt = time.Now().UTC()
		}
		return nil
	})
}

func (m *Migrator) providerRevisionProgress(record *dbschema.HistoryRecord, migration *Migration, startedAt time.Time, applied, total int, direction MigrationDirection) {
	record.State, record.Direction = migrationStatePending, string(direction)
	record.Applied, record.Total, record.ExecutionTime = applied, total, time.Since(startedAt)
	record.Error, record.ErrorStatement = "", ""
	if !m.revisionTableFormat.isAtlas() {
		record.Checksum = m.dirtyRevisionChecksum(migration, direction, applied)
		return
	}
	record.OperatorVersion = atlasOperatorVersionForMigration(migration, direction)
	sqlText := migrationSQLForDirection(migration, direction)
	if direction == MigrationDirectionDown {
		record.PartialHashes, _ = cumulativePartialHashValues(sqlText, m.connectionDialect(), applied)
	} else {
		record.PartialHashes, _ = atlasPartialHashValues(sqlText, m.connectionDialect(), applied, total)
	}
}

func (m *Migrator) checkpointProviderRevision(ctx context.Context, conn *dbschema.DatabaseConnection, migration *Migration, startedAt time.Time, event StatementEvent, direction MigrationDirection) (bool, error) {
	return m.editProviderRevision(ctx, conn, migration, func(record *dbschema.HistoryRecord) error {
		m.providerRevisionProgress(record, migration, startedAt, event.Index, event.Total, direction)
		return nil
	})
}

func (m *Migrator) markProviderStatementInFlight(ctx context.Context, conn *dbschema.DatabaseConnection, migration *Migration, startedAt time.Time, event StatementEvent, direction MigrationDirection) (bool, error) {
	return m.editProviderRevision(ctx, conn, migration, func(record *dbschema.HistoryRecord) error {
		m.providerRevisionProgress(record, migration, startedAt, event.Index-1, event.Total, direction)
		record.Error, record.ErrorStatement = unknownStatementOutcomeError, revisiontext.ValidUTF8(event.Statement)
		return nil
	})
}

func (m *Migrator) beginProviderRollback(ctx context.Context, migration *Migration, startedAt time.Time) (bool, error) {
	return m.editProviderRevision(ctx, m.conn, migration, func(record *dbschema.HistoryRecord) error {
		m.providerRevisionProgress(record, migration, startedAt, 0, m.migrationStatementCount(migration.DownSQL), MigrationDirectionDown)
		record.Error = unknownStatementOutcomeError
		if m.revisionTableFormat.isAtlas() {
			record.AppliedAt = startedAt.UTC().Round(0)
		}
		return nil
	})
}

func (m *Migrator) failProviderRevision(ctx context.Context, failed failedRevision, applied int, txMode MigrationTxMode) (bool, error) {
	store, err := m.conn.HistoryStore()
	if err != nil || store == nil {
		return err != nil, err
	}
	_, rolledBack := migrationTransactionRollbackVersion(failed.failure)
	if !rolledBack && (txMode != MigrationTxModeNone || errors.Is(failed.failure, ptaherr.ErrOutcomeUnknown)) {
		// A pending row already records uncertainty. If commit actually succeeded,
		// the applied row or deletion is durable instead. Overwrite neither.
		return true, nil
	}
	if rolledBack && txMode != MigrationTxModeNone && m.conn.Info().Capabilities.Has(capability.TransactionalDDL) {
		// The provider established atomic DDL. A private engine name cannot
		// use the built-in dialect table to reconstruct rollback progress.
		applied = migrationAppliedFloor(ctx)
	}
	return m.editProviderRevision(ctx, m.conn, failed.migration, func(record *dbschema.HistoryRecord) error {
		if record.State == migrationStateApplied && txMode != MigrationTxModeNone {
			return fmt.Errorf("refusing to replace completed provider revision %s with failure", record.Version)
		}
		m.providerRevisionProgress(record, failed.migration, failed.startedAt, applied, failed.total, failed.direction)
		record.State = migrationStateFailed
		record.Error = revisiontext.ValidUTF8(strings.TrimSpace(failed.failure.Error()))
		record.ErrorStatement = revisiontext.ValidUTF8(failed.statement)
		if m.revisionTableFormat.isAtlas() {
			record.Error = atlasFailureError(failed.failure)
			record.ErrorStatement = atlasFailureStatement(failed.sqlText, m.connectionDialect(), failed.failedIndex, failed.statement)
		}
		return nil
	})
}

func (m *Migrator) deleteMigrationRevisionOn(ctx context.Context, conn *dbschema.DatabaseConnection, migration *Migration, deleteSQL string) error {
	if conn.Writer().IsDryRun() {
		return nil
	}
	if handled, err := m.mutateProviderRevision(ctx, conn, migration, func(before *dbschema.HistoryRecord) (*dbschema.HistoryRecord, error) {
		if before == nil {
			return nil, fmt.Errorf("provider history revision %s disappeared before deletion", m.persistedRevisionIdentity(migration))
		}
		return nil, nil
	}); handled {
		return err
	}
	return executeSQLOn(ctx, conn, deleteSQL, m.migrationRevisionVersionArg(migration))
}
