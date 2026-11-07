package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"ptah.run/core/sqlutil"
	"ptah.run/internal/revisiontable"
	"ptah.run/internal/servercode"
)

// The revision table answers what is applied. It cannot answer what happened:
// a completed rollback deletes the row, so a database that was on version 43
// yesterday and is on 42 today reads exactly like one that never reached 43.
//
// The log answers the second question and only the second question. It is
// never read to decide the current version, and nothing reconciles the two: a
// reader that had to compare them to learn where the database stands would be
// worse off than with no log at all (stokaro/ptah#3406).
//
// It is a log and not an audit trail. It is an ordinary table in the database
// it describes, writable by the account that runs migrations, and nothing here
// makes it tamper-evident. Whoever can apply a migration can edit its record
// of having done so.

// actorName and actorSource are the process-wide fallback a new migrator
// starts with: the user the process runs as, recorded as that and not as a
// name anybody supplied.
var actorName, actorSource = resolveActor("")

// MigrationLogState is the outcome an entry records.
type MigrationLogState string

const (
	// MigrationLogStarted is written before the body runs. An entry that never
	// gained an outcome is an attempt whose process stopped mid-flight, and
	// [MigrationLogAttempt.Undetermined] is how a reader says so rather than
	// guessing which way it went.
	MigrationLogStarted MigrationLogState = "started"
	// MigrationLogApplied is a migration whose body ran and whose revision
	// says applied.
	MigrationLogApplied MigrationLogState = "applied"
	// MigrationLogFailed is a migration whose body did not complete.
	MigrationLogFailed MigrationLogState = "failed"
	// MigrationLogRolledBack is a rollback that completed.
	MigrationLogRolledBack MigrationLogState = "rolled_back"
)

// ActorSource says where an actor's name came from.
//
// The provenance travels with the name because the two are not the same claim.
// A name typed on a command line is what somebody wrote; a name read from the
// operating system is what the process ran as. Storing either without saying
// which it was turns a note into evidence it cannot support.
type ActorSource string

const (
	// ActorUnknown is the absence of a name.
	ActorUnknown ActorSource = "unknown"
	// ActorProvided is a name the caller supplied, normally from a flag or an
	// environment variable. It is unverified.
	ActorProvided ActorSource = "provided"
	// ActorProcessUser is the user the process runs as, read from the
	// environment the operating system set. It says which account started the
	// run, and nothing about who was at the keyboard.
	ActorProcessUser ActorSource = "process-user"
)

// MigrationLogEntry is one row of the log.
type MigrationLogEntry struct {
	// RunID identifies one invocation of the migrator. Every entry a single
	// run writes shares it.
	RunID string
	// Seq orders the entries of one run. It is assigned by the migrator rather
	// than by the database, so the ordering needs no per-dialect identity
	// column and survives a table copied between engines.
	Seq int64
	// Operation is the verb that wrote the entry: `up` or `down`.
	Operation string
	// Version is the migration the entry is about.
	Version int64
	// State is the outcome. See [MigrationLogStarted].
	State MigrationLogState
	// Actor is the name the run was given, empty when it was given none.
	Actor string
	// ActorSource says where Actor came from. See [ActorSource].
	ActorSource ActorSource
	// Checksum is the migration's content hash, so a later reader can tell
	// which bytes ran.
	Checksum string
	// At is when the entry was written.
	At time.Time
	// Error is the failure message, empty on every other state.
	Error string
}

// MigrationLogAttempt is one attempt, read back as the pair of entries that
// describe it. At least one of the two is present.
type MigrationLogAttempt struct {
	// Start is the entry written before the body ran, and is the zero value
	// when the log lost it. See [MigrationLogAttempt.StartLost].
	Start MigrationLogEntry
	// Outcome is the entry written after it, and is the zero value when the
	// attempt has none. See [MigrationLogAttempt.Undetermined].
	Outcome MigrationLogEntry
}

// Undetermined reports whether the attempt has no outcome.
//
// A run that was killed, lost its connection or had its container stopped
// leaves exactly this: a start with nothing after it. The answer is that
// nobody knows, and inferring success from a missing failure — or failure from
// a missing success — would be a claim about a process that stopped where no
// claim is available.
func (a MigrationLogAttempt) Undetermined() bool {
	return a.Outcome.State == ""
}

// StartLost reports whether the attempt has an outcome and no start.
//
// A write to the log that fails does not stop the migration, so a start the
// database refused leaves exactly this. The outcome names the same run,
// operation, version, actor and checksum, and what is gone is when the attempt
// began. [Migrator.MigrationLogReport] tells the run that lost it.
func (a MigrationLogAttempt) StartLost() bool {
	return a.Start.State == ""
}

// MigrationLogReport says what a migrator's attempts to write the log came to.
//
// The log is written beside the work, and a write that fails does not stop the
// migration. So the migrator that tried is the only witness to a lost entry:
// the table cannot say what it was never given, and a rerun finds nothing left
// to do and writes nothing. A table with no rows reads the same whether the run
// had nothing to record or lost everything it tried to record, and this report
// is what tells the two apart.
//
// It counts what the database accepted, which is not always what it stored. A
// MySQL or MariaDB session whose sql_mode is not strict shortens a value too long
// for its column, or replaces a character its charset cannot hold, and answers
// success, so such an entry counts as written.
type MigrationLogReport struct {
	// Written counts the entries the database accepted. Zero with nothing in
	// Lost is a run that had nothing to record.
	Written int `json:"written"`
	// Lost names the entries the database did not accept, in the order they were
	// tried. Each attempt writes two entries, so one migration can appear twice.
	Lost []MigrationLogLoss `json:"lost,omitempty"`
}

// MigrationLogLoss is one entry the log should hold and does not.
//
// It says nothing about the migration: a migration whose entries are lost ran
// as far as the revision table says it ran.
type MigrationLogLoss struct {
	// Operation is the verb the entry belongs to: `up` or `down`.
	Operation string `json:"operation"`
	// Version is the version of the migration the entry is about.
	Version int64 `json:"version"`
	// VersionKey is that migration's exact revision identity, spelled as
	// [RunResult.PlannedKeys] spells it. It is always present, because an empty
	// string is itself an identity a Flyway repeatable migration records.
	VersionKey string `json:"version_key"`
	// State is the outcome the entry would have recorded. It is empty, with
	// Version zero, when a rollback could not name the migrations it undoes, so
	// this one loss stands for all of their entries.
	State MigrationLogState `json:"state,omitempty"`
	// SQLState is the SQLSTATE the server answered with, as in
	// [RunFailure.SQLState].
	SQLState string `json:"sqlstate,omitempty"`
	// ErrorNumber is the server's error number, such as 1406 for a value too long
	// for its column on MySQL, as in [RunFailure.ErrorNumber].
	ErrorNumber int `json:"error_number,omitempty"`
	// Message is the failure written for a person. It can quote what the
	// database said, including part of the value it refused. A caller that must
	// not publish data reads the fields above instead.
	Message string `json:"message"`
}

// migrationLogTally gathers what a migrator's writes came to.
//
// It is a pointer on the Migrator for the reason the sequence counter is one:
// every With* builder copies the Migrator by value, and the migrator a caller
// holds is not the one a run session derives from it.
type migrationLogTally struct {
	mu     sync.Mutex
	report MigrationLogReport
}

func (t *migrationLogTally) wrote() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.report.Written++
}

func (t *migrationLogTally) lost(loss MigrationLogLoss) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.report.Lost = append(t.report.Lost, loss)
}

func (t *migrationLogTally) snapshot() MigrationLogReport {
	t.mu.Lock()
	defer t.mu.Unlock()
	return MigrationLogReport{Written: t.report.Written, Lost: slices.Clone(t.report.Lost)}
}

// MigrationLogReport returns what this migrator's writes to the log have come
// to so far, across every run it and the migrators derived from it have made.
//
// It returns nil when this migrator keeps no log to write to: the log is turned
// off, the revision format is the Atlas-compatible one, or the connection is in
// dry-run mode. A non-nil report with nothing lost means the database accepted
// every entry the runs tried to write.
func (m *Migrator) MigrationLogReport() *MigrationLogReport {
	if m.migrationLogTally == nil || m.conn == nil || !m.migrationLogWritable() || m.conn.Writer().IsDryRun() {
		return nil
	}
	report := m.migrationLogTally.snapshot()
	return &report
}

// reportLostLogEntry records one entry the log did not get. The caller says
// which entry, and cause says why.
func (m *Migrator) reportLostLogEntry(loss MigrationLogLoss, cause error) {
	loss.Message = cause.Error()
	loss.SQLState, loss.ErrorNumber = servercode.Read(cause)
	m.migrationLogTally.lost(loss)
}

// migrationLogWritable reports whether this migrator keeps the log.
//
// Two conditions, in one predicate because the DDL, the writer and the reader
// all have to agree: the caller asked for it, and the revision format is
// Ptah's own. The Atlas-compatible format ships the revision table that
// contract defines and nothing beside it -- a second table Ptah added would
// appear in a database an Atlas user believes only Atlas writes -- so every
// compatibility path is covered here rather than by a line in each of them.
func (m *Migrator) migrationLogWritable() bool {
	return m.migrationLogEnabled && !m.revisionTableFormat.isAtlas()
}

// refuseUnloggedRead says why this migrator has no log to read.
//
// Two conditions switch the log off and they have different remedies, so the
// message names the one that applies. A run told the log was absent because of
// the revision format, when the project simply turned it off, sends its
// operator to change `revision-format` -- a setting that would not bring the
// log back and that changes how every revision is written.
func (m *Migrator) refuseUnloggedRead() error {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Errorf(
			"this migrator keeps no operation log: the log is a native capability and this run uses " +
				"the Atlas-compatible revision format, which defines no such table")
	}
	if !m.migrationLogEnabled {
		return fmt.Errorf(
			"this migrator keeps no operation log: logging is turned off for this run, " +
				"so there is no table to read. Set migration.log to true to record future runs")
	}
	return nil
}

// migrationLogTable is the log's table name, derived from the revision
// table's so an operator who renamed or moved one finds the other beside it.
//
// The suffix goes on the bare name and the result is quoted, not the other way
// round: appending to the quoted form would put the suffix outside the quotes
// and every engine would read two tokens.
func (m *Migrator) migrationLogTable() string {
	return m.qualifiedMetadataTable(m.migrationsTableName() + migrationLogTableSuffix)
}

// migrationLogTableSuffix is what separates the log's name from the revision
// table's. One constant, because the DDL, the writer and the reader all have
// to name the same table, and so do the callers that leave the log out of a
// schema they compare (revisiontable.Configured).
const migrationLogTableSuffix = revisiontable.LogSuffix

// MigrationLogTableIdentifier returns the dialect-quoted name of the operation
// log's table, or an empty string when this migrator keeps no log.
//
// It exists for the callers that have to clear Ptah's own bookkeeping out of a
// database before comparing it against what the migrations describe: a shadow
// replay leaves both tables behind, and one left in place reads as a schema
// difference rather than as metadata.
func (m *Migrator) MigrationLogTableIdentifier() string {
	if !m.migrationLogWritable() {
		return ""
	}
	return m.migrationLogTable()
}

// migrationLogObjectName is the name SQL Server's OBJECT_ID probe takes.
//
// The suffix goes on the bare table name and the schema is applied after, the
// same way [Migrator.migrationLogTable] builds the identifier. Appending to
// the already-qualified name would make the guard probe
// `[dbo].[schema_migrations]_log` while the DDL creates
// `[dbo].[schema_migrations_log]`, so the guard would never find the table it
// created and every later initialization would try to create it again.
func (m *Migrator) migrationLogObjectName() string {
	table := m.quoteIdentifier(m.migrationsTableName() + migrationLogTableSuffix)
	if schema := m.metadataTableSchemaName(); schema != "" {
		return m.quoteIdentifier(schema) + "." + table
	}
	return table
}

// ptahMigrationLogDDL is the log table, in the spelling the engine's revision
// table states.
func ptahMigrationLogDDL(dialect, qualifiedTable, objectLiteral, engine string) string {
	return expandTableStatement(
		revisionTableFor(dialect).CreateLog,
		qualifiedTable,
		objectLiteral,
		revisionEngineClauseFor(dialect, engine),
	)
}

// createMigrationLogTableSQL is the DDL for this migrator's log table.
func (m *Migrator) createMigrationLogTableSQL() string {
	return ptahMigrationLogDDL(
		m.connectionDialect(),
		m.migrationLogTable(),
		sqlStringLiteral(m.migrationLogObjectName()),
		m.migrationsEngine,
	)
}

// logMigrationEvent appends one entry.
//
// It writes on the pool connection rather than inside the migration's
// transaction, which is the whole point: a migration that failed and rolled
// back must still leave the record that it was attempted, and a record that
// vanished with the transaction would describe only the runs that succeeded.
//
// A write that fails does not fail the migration. The log exists to explain
// what happened afterwards; refusing to apply a migration because its note could
// not be written would make an investigation aid into an outage. The failure goes
// to the run's logger for a person and to [Migrator.MigrationLogReport] for a
// caller that reads no log stream, and every value goes to the database as the
// caller gave it. Shortening an actor or replacing a character the column cannot
// hold would write a record that names somebody else, so such an entry is lost
// and says why rather than kept and wrong.
func (m *Migrator) logMigrationEvent(
	ctx context.Context,
	operation string,
	migration *Migration,
	state MigrationLogState,
	failure error,
) {
	if !m.migrationLogWritable() || m.conn.Writer().IsDryRun() {
		return
	}
	// A terminal entry is written with a context of its own, detached from the
	// one the migration ran under. A run that failed because its deadline
	// expired or because somebody interrupted it arrives here with that
	// context already done, and the write would fail immediately -- leaving
	// the start row alone and reporting a timeout as undetermined, which is
	// the one outcome nobody can act on.
	if state != MigrationLogStarted {
		durable, cancel := durableRevisionWriteContext(ctx)
		defer cancel()
		ctx = durable
	}
	lost := MigrationLogLoss{
		Operation:  operation,
		Version:    migration.Version,
		VersionKey: migration.RevisionVersion(),
		State:      state,
	}
	if err := m.ensureMigrationLogTable(ctx); err != nil {
		if !errors.Is(err, errMigrationLogAlreadyRefused) {
			m.logger.Warn("Could not create the migration log table",
				"version", migration.Version, "error", err)
		}
		m.reportLostLogEntry(lost, err)
		return
	}
	entry := MigrationLogEntry{
		RunID:       m.migrationLogRunID,
		Seq:         m.nextMigrationLogSeq(),
		Operation:   operation,
		Version:     migration.Version,
		State:       state,
		Actor:       m.actor,
		ActorSource: m.actorSource,
		Checksum:    migrationRevisionHash(migration),
		At:          time.Now(),
	}
	if failure != nil {
		entry.Error = failure.Error()
	}
	if err := m.writeMigrationLogEntry(ctx, entry); err != nil {
		m.logger.Warn("Could not append to the migration log",
			"version", migration.Version, "operation", operation, "state", string(state), "error", err)
		m.reportLostLogEntry(lost, err)
		return
	}
	m.migrationLogTally.wrote()
}

// migrationLogTableExists reports whether the log table is there.
//
// It asks the same presence query the revision table's own check asks, so the
// two answer through one grammar and a dialect whose catalog needs a special
// spelling needs it written once.
func (m *Migrator) migrationLogTableExists(ctx context.Context) (bool, error) {
	exists, err := m.metadataTableExists(ctx, m.migrationsTableName()+migrationLogTableSuffix)
	if err != nil {
		return false, fmt.Errorf("read the migration log: %w", err)
	}
	return exists, nil
}

// errMigrationLogAlreadyRefused marks a refusal the run's logger has already
// been told about. The caller warns on every other error and skips this one, so
// the warning is not repeated. The refusal itself stays in the chain, because
// the entry is still lost and [Migrator.MigrationLogReport] says why.
var errMigrationLogAlreadyRefused = errors.New("migration log table already refused")

// ensureMigrationLogTable creates the log table if it is not there yet.
//
// The creation lives here rather than in Initialize, and the difference is who
// pays for it. Every read entry point calls Initialize, so a table created
// there would be created by `migrations status`, by the log reader itself, and
// by any command run with an account that may read and not create -- turning a
// question into a write, and a read-only credential into a failure. Only a run
// that has something to record needs the table.
func (m *Migrator) ensureMigrationLogTable(ctx context.Context) error {
	if m.migrationLogReady == nil || m.migrationLogReady.Load() {
		return nil
	}
	store, err := m.conn.MigrationLogStore()
	if err != nil {
		return err
	}
	if store != nil {
		if err := store.Prepare(ctx, m.providerLogTable()); err != nil {
			return err
		}
		m.migrationLogReady.Store(true)
		return nil
	}

	// A name the target would truncate closes the log for this run rather than
	// stopping the migration. See [Migrator.refuseUnaddressableLogTable].
	if err := m.refuseUnaddressableLogTable(); err != nil {
		return m.latchLogTableRefusal(err)
	}
	// The same refusal the revision table gets, and for the same reason: this
	// table is written on every attempt, so adopting one somebody else created
	// hands them a hook on every migration. See
	// [Migrator.refuseForeignMetadataTable].
	//
	// It does not stop the migration, unlike the revision table's. Ptah cannot
	// record what it did without a revision table, so there the refusal has to
	// be terminal; the log is a record beside the work. Failing the run here
	// would hand any role that can create a table in the metadata schema a way
	// to stop every migration by creating this one.
	if err := m.refuseForeignMetadataTable(ctx, m.migrationsTableName()+migrationLogTableSuffix); err != nil {
		// Latched so the refusal is reported once rather than per entry: it
		// names one table and one remedy, and repeating it per attempt buries
		// the rest of the run.
		return m.latchLogTableRefusal(err)
	}
	if _, err := m.conn.ExecContext(ctx, m.createMigrationLogTableSQL()); err != nil {
		return fmt.Errorf("create the migration log table: %w", err)
	}
	// Again after the create, for the reason the revision table is asked
	// twice: the check above and this statement are two round trips, and a
	// role that can create objects in the metadata schema can place its table
	// between them for IF NOT EXISTS to adopt.
	if err := m.refuseForeignMetadataTable(ctx, m.migrationsTableName()+migrationLogTableSuffix); err != nil {
		return m.latchLogTableRefusal(err)
	}
	m.migrationLogReady.Store(true)
	return nil
}

// latchLogTableRefusal returns a refusal of the log table, marked as reported
// from the second time on.
func (m *Migrator) latchLogTableRefusal(err error) error {
	if m.migrationLogRefused != nil && m.migrationLogRefused.Swap(true) {
		return fmt.Errorf("%w: %w", errMigrationLogAlreadyRefused, err)
	}
	return err
}

func (m *Migrator) writeMigrationLogEntry(ctx context.Context, entry MigrationLogEntry) error {
	store, err := m.conn.MigrationLogStore()
	if err != nil {
		return err
	}
	if store != nil {
		return m.appendProviderLog(ctx, store, entry)
	}

	query := sqlutil.Rebind(m.conn.Info().Dialect, fmt.Sprintf(
		`INSERT INTO %s (run_id, seq, operation, version, state, actor, actor_source, checksum, logged_at, error)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, m.migrationLogTable()))
	return executeSQLOn(ctx, m.conn, query,
		entry.RunID, entry.Seq, entry.Operation, entry.Version, string(entry.State),
		m.nullableText(entry.Actor), string(entry.ActorSource), m.nullableText(entry.Checksum),
		entry.At, m.nullableText(entry.Error))
}

// nullableText writes an empty string as NULL, because Oracle reads one back
// that way whatever was sent and a column that means "nothing here" should not
// mean two different things depending on the engine.
//
// The NULL carries its type on YDB, which binds every value with a type and
// refuses an untyped one: measured on 26.2.1.14, a nil argument for a Utf8
// column answers `Failed to convert 'txt': Void to Optional<Utf8>`, and a nil
// *string is accepted.
func (m *Migrator) nullableText(value string) any {
	if value != "" {
		return value
	}
	if m.revisionTable().TypedNulls {
		return (*string)(nil)
	}
	return nil
}

func (m *Migrator) nextMigrationLogSeq() int64 {
	if m.migrationLogSeq == nil {
		return 0
	}
	return m.migrationLogSeq.Add(1)
}

// newMigrationLogRunID returns an identifier for one invocation.
//
// It is the process id and the start time rather than a random value: the two
// together are unique enough to group one run's entries, and they say
// something a reader can use when a log and a process list have to be lined
// up.
func newMigrationLogRunID() string {
	return strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// resolveActor reads the actor a run records, with the provenance that says
// what the name is worth.
//
// A caller that supplied a name is taken at its word and the entry says so. No
// name falls back to the operating system's, which is a different claim and is
// recorded as a different one.
func resolveActor(provided string) (string, ActorSource) {
	if strings.TrimSpace(provided) != "" {
		return strings.TrimSpace(provided), ActorProvided
	}
	for _, key := range []string{"USER", "USERNAME", "LOGNAME"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value, ActorProcessUser
		}
	}
	return "", ActorUnknown
}

// MigrationLog returns the attempts the log recorded, newest first.
//
// The entries of one attempt are paired here rather than by the caller: a
// start with no outcome is the shape that says a run stopped mid-flight, and
// pairing is what makes it visible. An outcome with no start is returned too,
// as [MigrationLogAttempt.StartLost] describes. limit bounds the attempts
// returned; zero returns every one.
func (m *Migrator) MigrationLog(ctx context.Context, limit int) ([]MigrationLogAttempt, error) {
	if err := m.refuseUnloggedRead(); err != nil {
		return nil, err
	}
	// Before the absent-table return: this is a public read path of its own,
	// and a malformed override must not stay dormant because this database
	// happens to have no log.
	if err := m.validateMetadataInputs(); err != nil {
		return nil, err
	}
	// A caller that asked for the log is told why it cannot be addressed,
	// rather than shown the empty list an absent table produces.
	if err := m.refuseUnaddressableLogTable(); err != nil {
		return nil, err
	}
	store, err := m.conn.MigrationLogStore()
	if err != nil {
		return nil, err
	}
	if store != nil {
		return m.readProviderLog(ctx, store, limit)
	}

	// No Initialize: reading is a question, and a question that created a
	// table would change the database it was asked about -- and fail outright
	// for an account allowed to read and not to create. A target with no log
	// has nothing to report, which is what an absent table means.
	present, err := m.migrationLogTableExists(ctx)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	// A read is not safe by being a read: a foreign table can carry a policy
	// or an expression the server evaluates during the SELECT, which runs the
	// squatter's SQL with the reader's privileges. The refusal is terminal
	// here rather than a warning, because a caller that asked for the log gets
	// nothing either way and should be told which table it declined to read.
	if err := m.refuseForeignMetadataTable(ctx, m.migrationsTableName()+migrationLogTableSuffix); err != nil {
		return nil, err
	}
	entries, err := m.readMigrationLogEntries(ctx)
	if err != nil {
		return nil, err
	}
	attempts := pairMigrationLogEntries(entries)
	if limit > 0 && len(attempts) > limit {
		attempts = attempts[:limit]
	}
	return attempts, nil
}

func (m *Migrator) readMigrationLogEntries(ctx context.Context) ([]MigrationLogEntry, error) {
	query := fmt.Sprintf(
		`SELECT run_id, seq, operation, version, state, actor, actor_source, checksum, logged_at, error
FROM %s ORDER BY logged_at DESC, run_id DESC, seq DESC`, m.migrationLogTable())
	rows, err := m.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read the migration log: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var entries []MigrationLogEntry
	for rows.Next() {
		entry, scanErr := m.scanMigrationLogEntry(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the migration log: %w", err)
	}
	return entries, nil
}

// pairMigrationLogEntries joins each start with the outcome that followed it,
// and returns the attempts newest first.
//
// The entries arrive newest first, and the walk takes them oldest first, the
// order they were written in, so a start is open by the time its outcome comes.
// Correlating by run, operation and version is enough: one run applies a version
// once, and a second attempt at the same version is a second run with its own
// identifier.
//
// An outcome that finds no open start is an attempt of its own. The log's writes
// fail one at a time and none of them stops the migration, so a start can be lost
// while its outcome is kept, and that outcome is the only record the attempt has.
func pairMigrationLogEntries(entries []MigrationLogEntry) []MigrationLogAttempt {
	attempts := make([]MigrationLogAttempt, 0, len(entries))
	open := make(map[string]int, len(entries))
	for _, entry := range slices.Backward(entries) {
		key := migrationLogAttemptKey(entry)
		index, started := open[key]
		switch {
		case entry.State == MigrationLogStarted:
			open[key] = len(attempts)
			attempts = append(attempts, MigrationLogAttempt{Start: entry})
		case started:
			attempts[index].Outcome = entry
			delete(open, key)
		default:
			attempts = append(attempts, MigrationLogAttempt{Outcome: entry})
		}
	}
	slices.Reverse(attempts)
	return attempts
}

func migrationLogAttemptKey(entry MigrationLogEntry) string {
	return entry.RunID + "\x00" + entry.Operation + "\x00" + strconv.FormatInt(entry.Version, 10)
}

// scanMigrationLogEntry reads one row, turning the nullable text columns back
// into empty strings.
//
// Oracle stores an empty string as NULL, so a column Ptah wrote empty comes
// back NULL there and non-NULL everywhere else. Reading through a nullable
// type on every engine is what keeps one written value one read value.
func (m *Migrator) scanMigrationLogEntry(rows *sql.Rows) (MigrationLogEntry, error) {
	var (
		entry       MigrationLogEntry
		state       string
		actorSource string
		actor       sql.NullString
		checksum    sql.NullString
		loggedAt    any
		failure     sql.NullString
	)
	if err := rows.Scan(
		&entry.RunID, &entry.Seq, &entry.Operation, &entry.Version, &state,
		&actor, &actorSource, &checksum, &loggedAt, &failure,
	); err != nil {
		return MigrationLogEntry{}, fmt.Errorf("read a migration log row: %w", err)
	}
	at, err := m.parseStoredTime("logged_at", loggedAt)
	if err != nil {
		return MigrationLogEntry{}, fmt.Errorf("read a migration log row: %w", err)
	}
	entry.At = at
	entry.State = MigrationLogState(state)
	entry.ActorSource = ActorSource(actorSource)
	entry.Actor = actor.String
	entry.Checksum = checksum.String
	entry.Error = failure.String
	return entry, nil
}

// LogDerivedRollback opens a log attempt for every migration a rollback is
// about to undo and returns the function that settles them.
//
// It is for a caller that rolls back by running its own statements -- a
// rollback derived from the schema difference rather than from the authored
// down bodies. That path never reaches [Migrator.MigrateDownTo], so nothing
// would record it, while it deletes the same revision rows: the database ends
// up looking like one that was never at those versions, which is the history
// this log exists to keep.
//
// The returned function takes the rollback's own error, records
// [MigrationLogRolledBack] for a nil one and [MigrationLogFailed] otherwise,
// and must be called exactly once. Calling it is the caller's obligation: an
// attempt left unsettled reads as [MigrationLogAttempt.Undetermined], which is
// the right answer for a process that stopped and the wrong one for a rollback
// that finished.
//
// Logging never fails the rollback. A migrator that keeps no log, a dry run,
// and a boundary this migrator cannot resolve each return a function that
// records nothing. The last of these loses the rollback's entries, so
// [Migrator.MigrationLogReport] says so.
func (m *Migrator) LogDerivedRollback(ctx context.Context, targetVersion int64) func(error) {
	noop := func(error) {}
	if !m.migrationLogWritable() || m.conn.Writer().IsDryRun() {
		return noop
	}
	applied, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		m.logger.Warn("Could not read the applied migrations for the log",
			"targetVersion", targetVersion, "error", err)
		m.reportLostLogEntry(MigrationLogLoss{Operation: "down"}, err)
		return noop
	}
	rollingBack, err := migrationsToRollback(
		migrationsByVersion(m.migrationProvider.Migrations()), applied, targetVersion)
	if err != nil {
		m.logger.Warn("Could not name the migrations a derived rollback undoes",
			"targetVersion", targetVersion, "error", err)
		m.reportLostLogEntry(MigrationLogLoss{Operation: "down"}, err)
		return noop
	}
	for _, migration := range rollingBack {
		m.logMigrationEvent(ctx, "down", migration, MigrationLogStarted, nil)
	}
	return func(failure error) {
		state := MigrationLogRolledBack
		if failure != nil {
			state = MigrationLogFailed
		}
		for _, migration := range rollingBack {
			m.logMigrationEvent(ctx, "down", migration, state, failure)
		}
	}
}
