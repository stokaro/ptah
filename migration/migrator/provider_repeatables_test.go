package migrator_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// memoryRecord is a provider's record of repeatable migrations held in memory.
// It applies the contract's preconditions, so a claim against a row another
// run holds conflicts as a provider's would.
type memoryRecord struct {
	exists  bool
	rows    map[string]dbschema.RepeatableRow
	bodies  map[string][]byte
	limit   int
	claims  int
	removed []string
	// lostClaim applies the claim and then reports an unknown outcome, as a
	// lost acknowledgment does.
	lostClaim bool
	// raceWinner is a run that claims the identity just before this run does.
	raceWinner string
	// goneOnRemove deletes the row just before a removal reaches it, as
	// another run would, so the removal finds none.
	goneOnRemove bool
}

func (r *memoryRecord) Read(context.Context, dbschema.MigrationMetadataTable) (dbschema.RepeatableSnapshot, error) {
	rows := slices.Collect(maps.Values(r.rows))
	slices.SortFunc(rows, func(a, b dbschema.RepeatableRow) int { return strings.Compare(a.Identity, b.Identity) })
	return dbschema.RepeatableSnapshot{Exists: r.exists, Rows: rows}, nil
}

func (r *memoryRecord) Prepare(context.Context, dbschema.MigrationMetadataTable) error {
	r.exists = true
	if r.rows == nil {
		r.rows, r.bodies = make(map[string]dbschema.RepeatableRow), make(map[string][]byte)
	}
	return nil
}

func (r *memoryRecord) ReadBody(_ context.Context, _ dbschema.MigrationMetadataTable, identity string) (dbschema.RepeatableBody, error) {
	row, found := r.rows[identity]
	return dbschema.RepeatableBody{Found: found, Kept: row.BodyKept, Body: r.bodies[identity]}, nil
}

func (r *memoryRecord) Claim(_ context.Context, _ dbschema.MigrationMetadataTable, claim dbschema.RepeatableClaim) error {
	r.claims++
	if r.raceWinner != "" {
		r.rows[claim.Identity] = dbschema.RepeatableRow{Identity: claim.Identity, State: dbschema.RepeatableRunning, RunID: r.raceWinner}
	}
	row, found := r.rows[claim.Identity]
	if found == claim.Insert || found && row.RunID != claim.ExpectedRunID {
		return &dbschema.RepeatableConflictError{Holder: row.RunID}
	}
	row.Identity, row.State, row.Error = claim.Identity, dbschema.RepeatableRunning, ""
	row.AttemptChecksum, row.AttemptOrder, row.Transactional = claim.AttemptChecksum, claim.AttemptOrder, claim.Transactional
	row.RunID, row.AttemptedAt = claim.RunID, claim.AttemptedAt
	r.rows[claim.Identity] = row
	if r.lostClaim {
		return fmt.Errorf("%w: claim acknowledgment lost", ptaherr.ErrOutcomeUnknown)
	}
	return nil
}

func (r *memoryRecord) Complete(_ context.Context, _ dbschema.MigrationMetadataTable, completion dbschema.RepeatableCompletion) error {
	row, found := r.rows[completion.Identity]
	if !found || row.RunID != completion.RunID {
		return &dbschema.RepeatableConflictError{Holder: row.RunID}
	}
	row.State, row.Checksum, row.AppliedAt, row.Error, row.BodyKept = dbschema.RepeatableApplied, completion.Checksum, completion.AppliedAt, "", true
	r.rows[completion.Identity] = row
	r.bodies[completion.Identity] = slices.Clone(completion.Body)
	return nil
}

func (r *memoryRecord) Fail(_ context.Context, _ dbschema.MigrationMetadataTable, failure dbschema.RepeatableFailure) error {
	row, found := r.rows[failure.Identity]
	if !found || row.RunID != failure.RunID {
		return &dbschema.RepeatableConflictError{Holder: row.RunID}
	}
	row.State, row.Error = dbschema.RepeatableFailed, failure.Error
	r.rows[failure.Identity] = row
	return nil
}

func (r *memoryRecord) Remove(_ context.Context, _ dbschema.MigrationMetadataTable, identity string) (bool, error) {
	if r.goneOnRemove {
		delete(r.rows, identity)
	}
	_, found := r.rows[identity]
	delete(r.rows, identity)
	r.removed = append(r.removed, identity)
	return found, nil
}

func (r *memoryRecord) MaxBodyBytes() int { return r.limit }

// recordSession is a provider session whose engine keeps the record in
// memory, or keeps none when record is nil. A statement holding failOn fails.
type recordSession struct {
	*metadataSession
	record *memoryRecord
	failOn string
}

func (s *recordSession) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return s, nil
}
func (s *recordSession) OpenSession(context.Context) (dbschema.NativeSession, error) { return s, nil }
func (s *recordSession) Writer() catalog.SchemaWriter                                { return s }
func (s *recordSession) ExecuteSQL(ctx context.Context, sql string, args ...any) error {
	if s.failOn != "" && strings.Contains(sql, s.failOn) {
		return errors.New("the engine refused the statement")
	}
	return s.metadataSession.ExecuteSQL(ctx, sql, args...)
}

func (s *recordSession) RepeatableStore() (dbschema.RepeatableStore, error) {
	if s.record == nil {
		return nil, fmt.Errorf("%w: provider metadata.fixture keeps no record of repeatable migrations", ptaherr.ErrUnsupportedFeature)
	}
	return s.record, nil
}

// providerRepeatableDirectory holds one versioned migration and the view of
// repeatableViewUp under the identity user_names.
var providerRepeatableDirectory = repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})

// providerRepeatableMigrator runs fsys on a provider session, one run of the
// tool per call.
func providerRepeatableMigrator(c *qt.C, owner *recordSession, fsys fstest.MapFS) *migrator.Migrator {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{"metadata": {Target: "sqlite", Connector: owner}})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "metadata://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	runner, err := migrator.NewFSMigrator(conn, fsys, migrator.WithRepeatableMigrations())
	c.Assert(err, qt.IsNil)
	return runner.WithMigrationsTable("main", "_revisions").WithTransactionMode(migrator.MigrationTxModeNone).WithoutMigrationLock()
}

// newRecordSession is a provider session with an empty record that keeps
// bodies of up to limit bytes.
func newRecordSession(limit int) *recordSession {
	return &recordSession{metadataSession: &metadataSession{}, record: &memoryRecord{limit: limit}}
}

// On a provider that keeps the record, a repeatable migration runs after the
// versioned history, and the provider's record keeps the attempt's digest,
// order and run, then the content that applied with its bytes. The log entries
// about it carry its identity with version 0.
func TestProviderRepeatables_RecordsTheContentThatApplied(t *testing.T) {
	c := qt.New(t)
	owner := newRecordSession(1 << 20)

	err := providerRepeatableMigrator(c, owner, providerRepeatableDirectory).MigrateUp(c.Context())

	c.Assert(err, qt.IsNil)
	row := owner.record.rows["user_names"]
	c.Assert(row.State, qt.Equals, dbschema.RepeatableApplied)
	c.Assert(row.Checksum, qt.Equals, h1(repeatableViewUp))
	c.Assert(row.AttemptChecksum, qt.Equals, h1(repeatableViewUp))
	c.Assert(row.AttemptOrder, qt.Equals, 1)
	c.Assert(row.RunID, qt.Not(qt.Equals), "")
	c.Assert(string(owner.record.bodies["user_names"]), qt.Equals, repeatableViewUp)
	c.Assert(owner.statements, qt.Equals, 5)
	repeatable := slices.DeleteFunc(slices.Clone(owner.log.records), func(record dbschema.MigrationLogRecord) bool { return record.Repeatable == "" })
	c.Assert(repeatable, qt.HasLen, 2)
	c.Assert(repeatable[0].Version, qt.Equals, int64(0))
	c.Assert(repeatable[0].State, qt.Equals, string(migrator.MigrationLogStarted))
	c.Assert(repeatable[1].State, qt.Equals, string(migrator.MigrationLogApplied))
	c.Assert(repeatable[1].RunID, qt.Equals, row.RunID)
}

// A second run over the same content claims nothing and runs nothing; a
// changed file is claimed under the row the first run left and runs again.
func TestProviderRepeatables_RunsAgainOnlyWhenTheContentChanges(t *testing.T) {
	c := qt.New(t)
	owner := newRecordSession(1 << 20)
	c.Assert(providerRepeatableMigrator(c, owner, providerRepeatableDirectory).MigrateUp(c.Context()), qt.IsNil)
	first := owner.record.rows["user_names"]

	unchanged := providerRepeatableMigrator(c, owner, providerRepeatableDirectory).MigrateUp(c.Context())
	claimsUnchanged := owner.record.claims
	changed := providerRepeatableMigrator(c, owner, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewChangedUp})).MigrateUp(c.Context())

	c.Assert(unchanged, qt.IsNil)
	c.Assert(claimsUnchanged, qt.Equals, 1)
	c.Assert(changed, qt.IsNil)
	c.Assert(owner.record.claims, qt.Equals, 2)
	second := owner.record.rows["user_names"]
	c.Assert(second.Checksum, qt.Equals, h1(repeatableViewChangedUp))
	c.Assert(second.RunID, qt.Not(qt.Equals), first.RunID)
	c.Assert(string(owner.record.bodies["user_names"]), qt.Equals, repeatableViewChangedUp)
}

// A claim whose acknowledgment is lost is settled by reading the row back: it
// names this run, so the claim holds and the body runs once.
func TestProviderRepeatables_SettlesALostClaimByTheRow(t *testing.T) {
	c := qt.New(t)
	owner := newRecordSession(1 << 20)
	owner.record.lostClaim = true

	err := providerRepeatableMigrator(c, owner, providerRepeatableDirectory).MigrateUp(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(owner.record.rows["user_names"].State, qt.Equals, dbschema.RepeatableApplied)
	c.Assert(owner.statements, qt.Equals, 5)
}

// ptah migrations repair --repeatable removes the provider's row.
func TestProviderRepeatables_RepairRemovesTheRow(t *testing.T) {
	c := qt.New(t)
	owner := newRecordSession(1 << 20)
	c.Assert(providerRepeatableMigrator(c, owner, providerRepeatableDirectory).MigrateUp(c.Context()), qt.IsNil)

	record, err := providerRepeatableMigrator(c, owner, providerRepeatableDirectory).RemoveRepeatableRecord(c.Context(),
		migrator.RemoveRepeatableRecordOptions{Identity: "user_names", Force: true})

	c.Assert(err, qt.IsNil)
	c.Assert(record.Identity, qt.Equals, "user_names")
	c.Assert(owner.record.removed, qt.DeepEquals, []string{"user_names"})
	c.Assert(owner.record.rows, qt.HasLen, 0)
}

// A removal that finds no row, although the read under the same lock found
// one, is an error: the repair reports the identity as not recorded rather
// than claiming a removal the provider did not make.
func TestProviderRepeatables_RepairRefusesARowThatIsGone(t *testing.T) {
	c := qt.New(t)
	owner := newRecordSession(1 << 20)
	c.Assert(providerRepeatableMigrator(c, owner, providerRepeatableDirectory).MigrateUp(c.Context()), qt.IsNil)
	owner.record.goneOnRemove = true

	record, err := providerRepeatableMigrator(c, owner, providerRepeatableDirectory).RemoveRepeatableRecord(c.Context(),
		migrator.RemoveRepeatableRecordOptions{Identity: "user_names", Force: true})

	c.Assert(err, qt.ErrorIs, migrator.ErrRepeatableNotRecorded)
	c.Assert(err, qt.ErrorMatches, `failed to delete the row of repeatable migration user_names: .*: the record holds no row for it`)
	c.Assert(record, qt.IsNil)
	c.Assert(owner.record.removed, qt.DeepEquals, []string{"user_names"})
}

// A provider that keeps no record refuses the directory before anything runs,
// and the refusal carries the provider's reason.
func TestProviderRepeatables_RefusesAProviderWithoutTheRecord(t *testing.T) {
	c := qt.New(t)
	owner := newRecordSession(1 << 20)
	owner.record = nil

	err := providerRepeatableMigrator(c, owner, providerRepeatableDirectory).MigrateUp(c.Context())

	c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableMigrationsUnsupportedError))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `this directory holds repeatable migrations, which this run cannot execute on sqlite: `+
		`unsupported feature: provider metadata.fixture keeps no record of repeatable migrations`)
	c.Assert(owner.statements, qt.Equals, 0)
	c.Assert(owner.history, qt.HasLen, 0)
	c.Assert(owner.exists, qt.IsFalse)
	c.Assert(owner.log.prepared, qt.IsFalse)
}

// A content longer than the provider's record keeps is refused before
// anything runs, so no body runs whose completion would be refused.
func TestProviderRepeatables_RefusesABodyOverTheRecordLimit(t *testing.T) {
	c := qt.New(t)
	owner := newRecordSession(len(repeatableViewUp) - 1)

	err := providerRepeatableMigrator(c, owner, providerRepeatableDirectory).MigrateUp(c.Context())

	c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableBodyTooLongError))
	c.Assert(err, qt.ErrorMatches, fmt.Sprintf(`repeatable migration R__user_names.sql is %d bytes, and the record of repeatable `+
		`migrations on this engine keeps at most %d; split the file into smaller repeatable migrations`, len(repeatableViewUp), len(repeatableViewUp)-1))
	c.Assert(owner.statements, qt.Equals, 0)
	c.Assert(owner.record.claims, qt.Equals, 0)
}

// A claim another run won stops this run before any statement of the body,
// and names the run that holds the row.
func TestProviderRepeatables_StopsWhenAnotherRunWinsTheClaim(t *testing.T) {
	c := qt.New(t)
	owner := newRecordSession(1 << 20)
	owner.record.raceWinner = "other-run"

	err := providerRepeatableMigrator(c, owner, providerRepeatableDirectory).MigrateUp(c.Context())

	c.Assert(err, qt.ErrorIs, migrator.ErrRepeatableClaimLost)
	c.Assert(err, qt.ErrorMatches, `(?s).*run "other-run" holds the row.*`)
	c.Assert(owner.statements, qt.Equals, 2)
	c.Assert(owner.record.rows["user_names"].RunID, qt.Equals, "other-run")
}

// A body that fails records the failure and its error in the provider's
// row, and the next run refuses the dirty record.
func TestProviderRepeatables_RecordsAFailedBody(t *testing.T) {
	c := qt.New(t)
	owner := newRecordSession(1 << 20)
	owner.failOn = "CREATE VIEW"

	failed := providerRepeatableMigrator(c, owner, providerRepeatableDirectory).MigrateUp(c.Context())
	owner.failOn = ""
	refused := providerRepeatableMigrator(c, owner, providerRepeatableDirectory).MigrateUp(c.Context())

	c.Assert(failed, qt.ErrorAs, new(*migrator.RepeatableMigrationError))
	row := owner.record.rows["user_names"]
	c.Assert(row.State, qt.Equals, dbschema.RepeatableFailed)
	c.Assert(row.Error, qt.Contains, "the engine refused the statement")
	c.Assert(row.Checksum, qt.Equals, "")
	c.Assert(refused, qt.ErrorAs, new(*migrator.DirtyRepeatableError))
}
