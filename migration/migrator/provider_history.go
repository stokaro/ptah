package migrator

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
)

func (m *Migrator) providerHistoryTable() dbschema.HistoryTable {
	format := dbschema.NativeHistory
	if m.revisionTableFormat.isAtlas() {
		format = dbschema.AtlasHistory
	}
	return dbschema.HistoryTable{Schema: m.metadataTableSchemaName(), Name: m.migrationsTableName(), Format: format}
}
func (m *Migrator) initializeProviderHistory(ctx context.Context, store dbschema.HistoryStore) error {
	if m.migrationsEngine != "" {
		return fmt.Errorf("%w: provider history does not accept a host SQL storage engine clause", ptaherr.ErrUnsupportedFeature)
	}
	dry := m.conn.Writer().IsDryRun()
	available := true
	if dry {
		snapshot, err := store.Read(ctx, m.providerHistoryTable())
		if err != nil {
			return err
		}
		available = snapshot.Exists
	} else if err := store.Prepare(ctx, m.providerHistoryTable()); err != nil {
		return err
	}
	m.initialized = true
	m.initializedDryRun = dry
	m.metadataAvailable = available
	m.legacyRevisionTable = false
	return nil
}

func (m *Migrator) readRevisionRows(ctx context.Context) ([]MigrationRevision, error) {
	store, err := m.conn.HistoryStore()
	if err != nil {
		return nil, err
	}
	if store == nil {
		return queryMigrationRows(ctx, m, (*Migrator).getRevisionsSQL, m.scanRevisionRow, "failed to query migration revisions", "failed to scan migration revision", "error iterating migration revisions")
	}
	if err := m.Initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if !m.metadataAvailable {
		return make([]MigrationRevision, 0), nil
	}
	snapshot, err := store.Read(ctx, m.providerHistoryTable())
	if err != nil {
		return nil, err
	}
	if !snapshot.Exists {
		return nil, fmt.Errorf("provider history store disappeared after initialization")
	}
	return m.providerHistoryRevisions(snapshot)
}

func (m *Migrator) providerHistoryRevisions(snapshot dbschema.HistorySnapshot) ([]MigrationRevision, error) {
	revisions := make([]MigrationRevision, 0, len(snapshot.Records))
	seen := make(map[string]bool, len(snapshot.Records))
	for _, record := range snapshot.Records {
		if seen[record.Version] {
			return nil, fmt.Errorf("provider history contains duplicate revision identities")
		}
		seen[record.Version] = true
		revision, err := m.providerHistoryRevision(record)
		if err != nil {
			return nil, err
		}
		revisions = append(revisions, revision)
	}
	slices.SortFunc(revisions, func(left, right MigrationRevision) int {
		if order := cmp.Compare(left.Version, right.Version); order != 0 {
			return order
		}
		return cmp.Compare(left.RevisionVersion(), right.RevisionVersion())
	})
	return revisions, nil
}

// Storage does not decide whether a record is resumable or applied. Keep the
// same state and progress checks used by SQL-backed history at this boundary.
func (m *Migrator) providerHistoryRevision(record dbschema.HistoryRecord) (MigrationRevision, error) {
	revision := MigrationRevision{Description: record.Description, State: record.State, Direction: MigrationDirection(record.Direction), Applied: record.Applied, Total: record.Total,
		Error: record.Error, ErrorStatement: record.ErrorStatement, ExecutionTime: record.ExecutionTime, Checksum: record.Checksum, AppliedAt: record.AppliedAt,
		OperatorVersion: record.OperatorVersion, AtlasType: AtlasRevisionType(record.AtlasType), partialHashes: slices.Clone(record.PartialHashes)}
	if revision.Direction != MigrationDirectionUp && revision.Direction != MigrationDirectionDown {
		return MigrationRevision{}, fmt.Errorf("provider history contains an invalid revision direction")
	}
	if err := m.bindProviderRevisionIdentity(record, &revision); err != nil {
		return MigrationRevision{}, err
	}
	if err := validateRevisionProgress(revision, "read revision metadata"); err != nil {
		return MigrationRevision{}, err
	}
	revision.Dirty = revision.State != migrationStateApplied
	return revision, nil
}
func (m *Migrator) providerRevisionByVersion(ctx context.Context, version any) (*MigrationRevision, error) {
	var key string
	switch value := version.(type) {
	case int64:
		key = strconv.FormatInt(value, 10)
	case string:
		key = value
	default:
		return nil, fmt.Errorf("invalid provider revision identity type")
	}
	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return nil, err
	}
	for _, revision := range revisions {
		identity := strconv.FormatInt(revision.Version, 10)
		if m.revisionTableFormat.isAtlas() {
			identity = revision.RevisionVersion()
		}
		if identity == key {
			return &revision, nil
		}
	}
	return nil, nil
}

func (m *Migrator) bindProviderRevisionIdentity(record dbschema.HistoryRecord, revision *MigrationRevision) error {
	if m.revisionTableFormat.isAtlas() {
		revision.AtlasVersion = record.Version
		revision.hasAtlasVersion = true
		version, err := m.atlasRuntimeVersion(record.Version)
		if err != nil {
			return err
		}
		revision.Version = version
		if revision.State != atlasRevisionState(*revision) || revision.Direction != atlasRevisionDirection(revision.OperatorVersion) {
			return fmt.Errorf("provider Atlas history state disagrees with stored metadata")
		}
		return nil
	}

	version, err := strconv.ParseInt(record.Version, 10, 64)
	if err != nil || strconv.FormatInt(version, 10) != record.Version {
		return fmt.Errorf("provider history contains a non-canonical numeric revision identity")
	}
	revision.Version = version
	if revision.State == migrationStateApplied && revision.Direction != MigrationDirectionUp {
		return fmt.Errorf("provider native history has an applied rollback record")
	}
	revision.fileSpelling = m.atlasVersionSpelling(version)
	if err := validateNativeRevisionState(*revision, encodeRevisionState(revision.State, revision.Direction), "read revision metadata"); err != nil {
		return err
	}
	return nil
}
