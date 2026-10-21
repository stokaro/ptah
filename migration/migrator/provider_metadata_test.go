package migrator_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

type metadataReader[T any] struct {
	pages         []dbschema.MigrationMetadataPage[T]
	calls, closes int
	closeCanceled bool
	closeErr      error
	cancel        context.CancelFunc
}

func (r *metadataReader[T]) Next(_ context.Context, _ uint32) (dbschema.MigrationMetadataPage[T], error) {
	r.calls++
	if r.cancel != nil {
		r.cancel()
	}
	if len(r.pages) == 0 {
		return dbschema.MigrationMetadataPage[T]{}, fmt.Errorf("read past terminal metadata page")
	}
	page := r.pages[0]
	r.pages = r.pages[1:]
	return page, nil
}
func (r *metadataReader[T]) Close(ctx context.Context) error {
	r.closes++
	r.closeCanceled = ctx.Err() != nil
	return r.closeErr
}
func metadataPages[T any](records []T, exists bool) *metadataReader[T] {
	reader := &metadataReader[T]{}
	for _, record := range records {
		reader.pages = append(reader.pages, dbschema.MigrationMetadataPage[T]{Exists: exists, Records: []T{record}})
	}
	reader.pages = append(reader.pages, dbschema.MigrationMetadataPage[T]{Exists: exists, Complete: true})
	return reader
}

type metadataLog struct {
	prepared          bool
	prepares, appends int
	records           []dbschema.MigrationLogRecord
	appendErr         error
	reader            *metadataReader[dbschema.MigrationLogRecord]
}

func (l *metadataLog) Prepare(context.Context, dbschema.MigrationMetadataTable) error {
	l.prepared = true
	l.prepares++
	return nil
}
func (l *metadataLog) Append(_ context.Context, _ dbschema.MigrationMetadataTable, record dbschema.MigrationLogRecord) error {
	l.appends++
	if l.appendErr != nil {
		return l.appendErr
	}
	l.records = append(l.records, record)
	return nil
}
func (l *metadataLog) OpenRead(context.Context, dbschema.MigrationMetadataTable) (dbschema.MigrationMetadataReader[dbschema.MigrationLogRecord], error) {
	if l.reader == nil {
		l.reader = metadataPages(slices.Clone(l.records), l.prepared)
	}
	return l.reader, nil
}

type metadataTags struct {
	prepares, puts, deletes int
	records                 map[string]dbschema.MigrationTagRecord
	putErr, deleteErr       error
	deleteFound             bool
}

func (t *metadataTags) Prepare(context.Context, dbschema.MigrationMetadataTable) error {
	t.prepares++
	return nil
}
func (t *metadataTags) Put(_ context.Context, _ dbschema.MigrationMetadataTable, record dbschema.MigrationTagRecord) error {
	t.puts++
	if t.putErr != nil {
		return t.putErr
	}
	if t.records == nil {
		t.records = make(map[string]dbschema.MigrationTagRecord)
	}
	t.records[record.Name] = record
	return nil
}
func (t *metadataTags) Lookup(_ context.Context, _ dbschema.MigrationMetadataTable, name string) (dbschema.MigrationTagRecord, bool, error) {
	record, found := t.records[name]
	return record, found, nil
}
func (t *metadataTags) Delete(_ context.Context, _ dbschema.MigrationMetadataTable, name string) (bool, error) {
	t.deletes++
	if t.deleteErr != nil {
		return false, t.deleteErr
	}
	delete(t.records, name)
	return t.deleteFound, nil
}
func (t *metadataTags) OpenRead(context.Context, dbschema.MigrationMetadataTable) (dbschema.MigrationMetadataReader[dbschema.MigrationTagRecord], error) {
	records := make([]dbschema.MigrationTagRecord, 0, len(t.records))
	for _, record := range t.records {
		records = append(records, record)
	}
	return metadataPages(records, t.records != nil), nil
}

type metadataSession struct {
	log         metadataLog
	tags        metadataTags
	history     []dbschema.HistoryRecord
	exists, dry bool
	statements  int
}

func (s *metadataSession) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return s, nil
}
func (s *metadataSession) OpenSession(context.Context) (dbschema.NativeSession, error) { return s, nil }
func (*metadataSession) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: "sqlite", Schema: "main", IdentifierSemantics: identifier.ForDialect("sqlite"), Provider: catalog.ProviderIdentity{ID: "metadata.fixture", Version: "test", SemanticIdentity: "metadata-1", ProtocolRevision: 19, ArtifactSHA256: strings.Repeat("a", 64), ContractSHA256: strings.Repeat("b", 64)}}
}
func (s *metadataSession) Reader([]string) catalog.SchemaReader { return s }
func (s *metadataSession) Writer() catalog.SchemaWriter         { return s }
func (*metadataSession) ReadSchema() (*catalog.Database, error) { return &catalog.Database{}, nil }
func (*metadataSession) ReadSchemaContext(context.Context) (*catalog.Database, error) {
	return &catalog.Database{}, nil
}
func (*metadataSession) Close() error { return nil }
func (*metadataSession) RestrictUntrustedSQL(context.Context) error {
	return ptaherr.ErrUnsupportedFeature
}
func (s *metadataSession) IsDryRun() bool                    { return s.dry }
func (s *metadataSession) SetDryRun(value bool)              { s.dry = value }
func (*metadataSession) DropAllTables(context.Context) error { return ptaherr.ErrUnsupportedFeature }
func (*metadataSession) BeginTransaction(context.Context) (catalog.SchemaTransaction, error) {
	return nil, ptaherr.ErrUnsupportedFeature
}
func (s *metadataSession) ExecuteSQL(context.Context, string, ...any) error {
	s.statements++
	return nil
}
func (s *metadataSession) MigrationLogStore() (dbschema.MigrationLogStore, error) { return &s.log, nil }
func (s *metadataSession) MigrationTagsStore() (dbschema.MigrationTagsStore, error) {
	return &s.tags, nil
}
func (s *metadataSession) HistoryStore() (dbschema.HistoryStore, error) {
	return metadataHistory{s}, nil
}

type metadataHistory struct{ owner *metadataSession }

func (h metadataHistory) Prepare(context.Context, dbschema.HistoryTable) error {
	h.owner.exists = true
	return nil
}
func (h metadataHistory) Read(context.Context, dbschema.HistoryTable) (dbschema.HistorySnapshot, error) {
	return dbschema.HistorySnapshot{Exists: h.owner.exists, Records: slices.Clone(h.owner.history)}, nil
}
func (h metadataHistory) CompareAndSwap(_ context.Context, _ dbschema.HistoryTable, expected, replacement *dbschema.HistoryRecord) error {
	index := slices.IndexFunc(h.owner.history, func(record dbschema.HistoryRecord) bool { return expected != nil && record.Version == expected.Version })
	if expected != nil && (index < 0 || !reflect.DeepEqual(h.owner.history[index], *expected)) {
		return errors.New("history record changed")
	}
	if index >= 0 {
		h.owner.history = slices.Delete(h.owner.history, index, index+1)
	}
	if replacement != nil {
		h.owner.history = append(h.owner.history, *replacement)
	}
	return nil
}

func metadataMigrator(c *qt.C, owner *metadataSession) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{"metadata": {Target: "sqlite", Connector: owner}})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "metadata://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	migration := migrator.CreateMigrationFromSQL(1, "records", `CREATE TABLE records (id INTEGER);`, `DROP TABLE records;`)
	migration.DownTxMode = migrationfile.FileTxModeNone
	runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).WithMigrationsTable("main", "_revisions").
		WithTransactionMode(migrator.MigrationTxModeNone).WithoutMigrationLock().WithActor("release-bot")
	return conn, runner
}

func TestProviderMetadataLogRetainsCompletedRollback(t *testing.T) {
	c := qt.New(t)
	owner := &metadataSession{}
	_, runner := metadataMigrator(c, owner)
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(runner.MigrateDown(c.Context()), qt.IsNil)
	c.Assert(owner.history, qt.HasLen, 0)
	attempts, err := runner.MigrationLog(c.Context(), 0)
	c.Assert(err, qt.IsNil)
	c.Assert(attempts, qt.HasLen, 2)
	c.Assert(attempts[0].Outcome.State, qt.Equals, migrator.MigrationLogRolledBack)
	c.Assert(attempts[1].Outcome.State, qt.Equals, migrator.MigrationLogApplied)
	c.Assert(attempts[1].Start.Actor, qt.Equals, "release-bot")
	c.Assert(attempts[1].Start.ActorSource, qt.Equals, migrator.ActorProvided)
	c.Assert(owner.log.prepares, qt.Equals, 1)
	c.Assert(owner.log.reader.closes, qt.Equals, 1)
}

func TestProviderMetadataLogFailureDoesNotReplayMigration(t *testing.T) {
	c := qt.New(t)
	owner := &metadataSession{log: metadataLog{appendErr: ptaherr.ErrOutcomeUnknown}}
	_, runner := metadataMigrator(c, owner)
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(owner.statements, qt.Equals, 1)
	c.Assert(owner.log.appends, qt.Equals, 2)
	c.Assert(owner.history, qt.HasLen, 1)
	c.Assert(owner.history[0].State, qt.Equals, "applied")
}

func TestProviderMetadataTagsPreserveZeroAndAtomicDelete(t *testing.T) {
	c := qt.New(t)
	owner := &metadataSession{}
	_, runner := metadataMigrator(c, owner)
	_, err := runner.ResolveMigrationTag(c.Context(), "missing")
	c.Assert(err, qt.ErrorIs, migrator.ErrMigrationTagNotFound)
	c.Assert(owner.tags.prepares, qt.Equals, 0)
	c.Assert(runner.RecordMigrationTag(c.Context(), " zero ", 0), qt.IsNil)
	version, err := runner.ResolveMigrationTag(c.Context(), "zero")
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(0))
	c.Assert(runner.RecordMigrationTag(c.Context(), "alpha", 9), qt.IsNil)
	tags, err := runner.MigrationTags(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(tags, qt.HasLen, 2)
	c.Assert(tags[0].Tag, qt.Equals, "alpha")
	c.Assert(tags[1].Tag, qt.Equals, "zero")
	c.Assert(runner.DeleteMigrationTag(c.Context(), "zero"), qt.ErrorIs, migrator.ErrMigrationTagNotFound)
	c.Assert(owner.tags.deletes, qt.Equals, 1)
	owner.tags.deleteFound = true
	c.Assert(runner.DeleteMigrationTag(c.Context(), "alpha"), qt.IsNil)
}

func TestProviderMetadataTagUnknownWriteIsNotRetried(t *testing.T) {
	c := qt.New(t)
	owner := &metadataSession{tags: metadataTags{putErr: ptaherr.ErrOutcomeUnknown, deleteErr: ptaherr.ErrOutcomeUnknown}}
	_, runner := metadataMigrator(c, owner)
	c.Assert(runner.RecordMigrationTag(c.Context(), "release", 1), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(owner.tags.puts, qt.Equals, 1)
	c.Assert(runner.DeleteMigrationTag(c.Context(), "release"), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(owner.tags.deletes, qt.Equals, 1)
}

func TestProviderMetadataReadClosesOnInvalidPages(t *testing.T) {
	record := dbschema.MigrationLogRecord{RunID: "run", Seq: 1, Operation: "up", Version: 1, State: "started", ActorSource: "unknown", At: time.Unix(100, 0)}
	for _, test := range []struct {
		name  string
		pages []dbschema.MigrationMetadataPage[dbschema.MigrationLogRecord]
	}{
		{"no progress", []dbschema.MigrationMetadataPage[dbschema.MigrationLogRecord]{{Exists: true}}},
		{"absent with records", []dbschema.MigrationMetadataPage[dbschema.MigrationLogRecord]{{Complete: true, Records: []dbschema.MigrationLogRecord{record}}}},
		{"duplicate identities", []dbschema.MigrationMetadataPage[dbschema.MigrationLogRecord]{{Exists: true, Records: []dbschema.MigrationLogRecord{record}}, {Exists: true, Complete: true, Records: []dbschema.MigrationLogRecord{record}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := &metadataReader[dbschema.MigrationLogRecord]{pages: test.pages}
			_, runner := metadataMigrator(c, &metadataSession{log: metadataLog{reader: reader}})
			attempts, err := runner.MigrationLog(c.Context(), 0)
			c.Assert(err, qt.IsNotNil)
			c.Assert(attempts, qt.IsNil)
			c.Assert(reader.closes, qt.Equals, 1)
		})
	}
}

func TestProviderMetadataReadRetainsCleanupFailure(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("snapshot cleanup failed")
	reader := &metadataReader[dbschema.MigrationLogRecord]{pages: []dbschema.MigrationMetadataPage[dbschema.MigrationLogRecord]{{Complete: true}}, closeErr: failure}
	_, runner := metadataMigrator(c, &metadataSession{log: metadataLog{reader: reader}})
	attempts, err := runner.MigrationLog(c.Context(), 0)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(attempts, qt.IsNil)
	c.Assert(reader.closes, qt.Equals, 1)
}

func TestProviderMetadataReadCancellationDiscardsLatePage(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()
	reader := &metadataReader[dbschema.MigrationLogRecord]{pages: []dbschema.MigrationMetadataPage[dbschema.MigrationLogRecord]{{Complete: true}}, cancel: cancel}
	_, runner := metadataMigrator(c, &metadataSession{log: metadataLog{reader: reader}})
	attempts, err := runner.MigrationLog(ctx, 0)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(attempts, qt.IsNil)
	c.Assert(reader.closes, qt.Equals, 1)
	c.Assert(reader.closeCanceled, qt.IsFalse)
}

// metadataExecutor exposes tags on an execution handle without exposing the
// session's durable log. Selection must preserve those different lifetimes.
type metadataExecutor struct {
	catalog.SchemaExecutor
	tags metadataTags
}

func (e *metadataExecutor) MigrationTagsStore() (dbschema.MigrationTagsStore, error) {
	return &e.tags, nil
}

func TestProviderMetadataSelectsActiveTagsAndDurableSessionLog(t *testing.T) {
	c := qt.New(t)
	owner := &metadataSession{}
	conn, _ := metadataMigrator(c, owner)
	executor := &metadataExecutor{SchemaExecutor: owner}
	bound := conn.WithExecutor(executor)
	log, err := bound.MigrationLogStore()
	c.Assert(err, qt.IsNil)
	c.Assert(log, qt.Equals, &owner.log)
	tags, err := bound.MigrationTagsStore()
	c.Assert(err, qt.IsNil)
	c.Assert(tags, qt.Equals, &executor.tags)
}

func TestProviderMetadataRefusesParentTagsForUnsupportedExecutor(t *testing.T) {
	c := qt.New(t)
	conn, _ := metadataMigrator(c, &metadataSession{})
	bound := conn.WithExecutor(struct{ catalog.SchemaExecutor }{conn.Writer()})
	tags, err := bound.MigrationTagsStore()
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(tags, qt.IsNil)
}

func TestProviderMetadataDryRunDoesNotWriteTags(t *testing.T) {
	c := qt.New(t)
	owner := &metadataSession{dry: true}
	_, runner := metadataMigrator(c, owner)
	owner.dry = true
	c.Assert(runner.RecordMigrationTag(c.Context(), "release", 1), qt.IsNil)
	c.Assert(owner.tags.prepares, qt.Equals, 0)
	c.Assert(owner.tags.puts, qt.Equals, 0)
	c.Assert(runner.DeleteMigrationTag(c.Context(), "missing"), qt.ErrorIs, migrator.ErrMigrationTagNotFound)
	c.Assert(owner.tags.deletes, qt.Equals, 0)
}

func TestProviderMetadataRejectsInvalidTagBeforePreparation(t *testing.T) {
	c := qt.New(t)
	owner := &metadataSession{}
	_, runner := metadataMigrator(c, owner)
	c.Assert(runner.RecordMigrationTag(c.Context(), "release\x00hidden", 1), qt.IsNotNil)
	c.Assert(owner.tags.prepares, qt.Equals, 0)
	c.Assert(owner.tags.puts, qt.Equals, 0)
}
