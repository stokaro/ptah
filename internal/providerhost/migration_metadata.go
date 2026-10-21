package providerhost

import (
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/dbschema"
)

type metadataStore struct {
	historyStore
	kind protocol.MetadataScope_Kind
}
type migrationLogStore struct{ metadataStore }
type migrationTagsStore struct{ metadataStore }

func (s *session) metadataStore(kind protocol.MetadataScope_Kind) (metadataStore, error) {
	if !slices.Contains(s.target.engine.GetFeatures(), provider.MetadataFeature) || !slices.Contains(s.target.host.description.GetFeatures(), provider.MetadataFeature) {
		return metadataStore{}, unsupported("migration metadata for the selected engine")
	}
	return metadataStore{historyStore: historyStore{session: s}, kind: kind}, nil
}
func (s *session) MigrationLogStore() (dbschema.MigrationLogStore, error) {
	store, err := s.metadataStore(protocol.MetadataScope_LOG)
	if err != nil {
		return nil, err
	}
	return migrationLogStore{store}, nil
}
func (s *session) MigrationTagsStore() (dbschema.MigrationTagsStore, error) {
	store, err := s.metadataStore(protocol.MetadataScope_TAGS)
	if err != nil {
		return nil, err
	}
	return migrationTagsStore{store}, nil
}
func (t *transaction) MigrationTagsStore() (dbschema.MigrationTagsStore, error) {
	store, err := t.session.metadataStore(protocol.MetadataScope_TAGS)
	if err != nil {
		return nil, err
	}
	store.transaction = t
	return migrationTagsStore{store}, nil
}
func (m metadataStore) scope(table dbschema.MigrationMetadataTable) (*protocol.MetadataScope, error) {
	scope := &protocol.MetadataScope{SessionId: m.session.remote.GetId(), Schema: table.Schema, Table: table.Name, Kind: m.kind}
	if m.transaction != nil {
		scope.TransactionId = m.transaction.id
	}
	return scope, provider.ValidateMetadataScope(scope)
}

// Metadata shares history's ordering with transaction finish and execution.
// A dispatched tag operation failure prevents commit; cleanup remains available.
func (m metadataStore) Prepare(ctx context.Context, table dbschema.MigrationMetadataTable) error {
	if m.session.IsDryRun() {
		return unsupported("metadata preparation during a dry run")
	}
	scope, err := m.scope(table)
	if err != nil {
		return err
	}
	request := &protocol.PrepareMetadataRequest{Scope: scope, RequestId: rand.Text()}
	if err := provider.ValidatePrepareMetadataRequest(request); err != nil {
		return err
	}
	return m.use(ctx, func() error { _, err := m.session.target.host.client.PrepareMetadata(ctx, request); return err })
}
func (m metadataStore) mutate(ctx context.Context, request *protocol.MutateMetadataRequest) (*protocol.MetadataResult, error) {
	if m.session.IsDryRun() {
		return nil, unsupported("metadata writes during a dry run")
	}
	if err := provider.ValidateMutateMetadataRequest(request); err != nil {
		return nil, err
	}
	var result *protocol.MetadataResult
	err := m.use(ctx, func() error {
		var err error
		result, err = m.session.target.host.client.MutateMetadata(ctx, request)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
func (m migrationLogStore) Append(ctx context.Context, table dbschema.MigrationMetadataTable, record dbschema.MigrationLogRecord) error {
	scope, err := m.scope(table)
	if err != nil {
		return err
	}
	row, err := metadataWireLog(record)
	if err != nil {
		return err
	}
	_, err = m.mutate(ctx, &protocol.MutateMetadataRequest{Scope: scope, RequestId: rand.Text(), Mutation: &protocol.MutateMetadataRequest_AppendLog{AppendLog: row}})
	return err
}
func (m migrationTagsStore) Put(ctx context.Context, table dbschema.MigrationMetadataTable, record dbschema.MigrationTagRecord) error {
	scope, err := m.scope(table)
	if err != nil {
		return err
	}
	row, err := metadataWireTag(record)
	if err != nil {
		return err
	}
	_, err = m.mutate(ctx, &protocol.MutateMetadataRequest{Scope: scope, RequestId: rand.Text(), Mutation: &protocol.MutateMetadataRequest_PutTag{PutTag: row}})
	return err
}
func (m migrationTagsStore) Delete(ctx context.Context, table dbschema.MigrationMetadataTable, name string) (bool, error) {
	scope, err := m.scope(table)
	if err != nil {
		return false, err
	}
	result, err := m.mutate(ctx, &protocol.MutateMetadataRequest{Scope: scope, RequestId: rand.Text(), Mutation: &protocol.MutateMetadataRequest_DeleteTag{DeleteTag: name}})
	if err != nil {
		return false, err
	}
	return result.GetDeleted(), nil
}
func (m migrationTagsStore) Lookup(ctx context.Context, table dbschema.MigrationMetadataTable, name string) (dbschema.MigrationTagRecord, bool, error) {
	scope, err := m.scope(table)
	if err != nil {
		return dbschema.MigrationTagRecord{}, false, err
	}
	request := &protocol.LookupMetadataTagRequest{Scope: scope, RequestId: rand.Text(), Name: name}
	if err := provider.ValidateLookupMetadataTagRequest(request); err != nil {
		return dbschema.MigrationTagRecord{}, false, err
	}
	var result *protocol.MetadataTagResult
	err = m.use(ctx, func() error {
		var err error
		result, err = m.session.target.host.client.LookupMetadataTag(ctx, request)
		return err
	})
	if err != nil {
		return dbschema.MigrationTagRecord{}, false, err
	}
	if !result.GetFound() {
		return dbschema.MigrationTagRecord{}, false, nil
	}
	return metadataTag(result.GetRecord()), true, nil
}
func metadataTimestamp(value time.Time) (int64, int32, error) {
	nanos := value.Nanosecond()
	if value.IsZero() || nanos < 0 || nanos >= 1e9 {
		return 0, 0, fmt.Errorf("invalid migration metadata timestamp")
	}
	return value.Unix(), int32(nanos), nil
}
func metadataWireLog(row dbschema.MigrationLogRecord) (*protocol.MigrationLogRecord, error) {
	seconds, nanos, err := metadataTimestamp(row.At)
	if err != nil {
		return nil, err
	}
	result := &protocol.MigrationLogRecord{RunId: row.RunID, Sequence: row.Seq, Operation: row.Operation, Version: row.Version, State: row.State,
		Actor: row.Actor, ActorSource: row.ActorSource, Checksum: row.Checksum, AtSeconds: seconds, AtNanos: nanos, Error: row.Error}
	return result, provider.ValidateMigrationLogRecord(result)
}
func metadataWireTag(row dbschema.MigrationTagRecord) (*protocol.MigrationTagRecord, error) {
	seconds, nanos, err := metadataTimestamp(row.RecordedAt)
	if err != nil {
		return nil, err
	}
	result := &protocol.MigrationTagRecord{Name: row.Name, Version: row.Version, RecordedAtSeconds: seconds, RecordedAtNanos: nanos}
	return result, provider.ValidateMigrationTagRecord(result)
}
func metadataLog(row *protocol.MigrationLogRecord) dbschema.MigrationLogRecord {
	return dbschema.MigrationLogRecord{RunID: row.GetRunId(), Seq: row.GetSequence(), Operation: row.GetOperation(), Version: row.GetVersion(), State: row.GetState(),
		Actor: row.GetActor(), ActorSource: row.GetActorSource(), Checksum: row.GetChecksum(), At: time.Unix(row.GetAtSeconds(), int64(row.GetAtNanos())).UTC(), Error: row.GetError()}
}
func metadataTag(row *protocol.MigrationTagRecord) dbschema.MigrationTagRecord {
	return dbschema.MigrationTagRecord{Name: row.GetName(), Version: row.GetVersion(), RecordedAt: time.Unix(row.GetRecordedAtSeconds(), int64(row.GetRecordedAtNanos())).UTC()}
}
