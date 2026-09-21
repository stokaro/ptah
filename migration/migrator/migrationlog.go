package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
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
// describe it.
type MigrationLogAttempt struct {
	// Start is the entry written before the body ran.
	Start MigrationLogEntry
	// Outcome is the entry written after it, and is the zero value when the
	// attempt has none.
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

// migrationLogTable is the log's table name, derived from the revision
// table's so an operator who renamed or moved one finds the other beside it.
//
// The suffix goes on the bare name and the result is quoted, not the other way
// round: appending to the quoted form would put the suffix outside the quotes
// and every engine would read two tokens.
func (m *Migrator) migrationLogTable() string {
	table := m.quoteIdentifier(m.migrationsTableName() + migrationLogTableSuffix)
	schema := m.metadataTableSchemaName()
	if schema == "" {
		return table
	}
	return m.quoteIdentifier(schema) + "." + table
}

// migrationLogTableSuffix is what separates the log's name from the revision
// table's. One constant, because the DDL, the writer and the reader all have
// to name the same table.
const migrationLogTableSuffix = "_log"

// migrationLogObjectName is the unqualified name SQL Server's OBJECT_ID takes.
func (m *Migrator) migrationLogObjectName() string {
	return m.sqlServerObjectName() + migrationLogTableSuffix
}

// ptahMigrationLogDDL is the log table, in the spelling each engine takes.
//
// The primary key is (run_id, seq) rather than a generated identity: every
// engine spells auto-increment differently -- SERIAL, IDENTITY, AUTOINCREMENT,
// GENERATED AS IDENTITY -- and the ordering this table needs is the order the
// migrator wrote its entries in, which the migrator already knows.
func ptahMigrationLogDDL(dialect, qualifiedTable, sqlServerObjectLiteral, engine string) string {
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLServer:
		return fmt.Sprintf(`IF OBJECT_ID(%s, N'U') IS NULL
BEGIN
    CREATE TABLE %s (
        run_id NVARCHAR(64) NOT NULL,
        seq BIGINT NOT NULL,
        operation NVARCHAR(32) NOT NULL,
        version BIGINT NOT NULL,
        state NVARCHAR(32) NOT NULL,
        actor NVARCHAR(256) NULL,
        actor_source NVARCHAR(32) NOT NULL,
        checksum NVARCHAR(64) NULL,
        logged_at DATETIME2 NOT NULL,
        error NVARCHAR(MAX) NULL,
        PRIMARY KEY (run_id, seq)
    )
END`, sqlServerObjectLiteral, qualifiedTable)
	case platform.Oracle:
		// The same three refusals the revision table records: no BIGINT, no
		// TEXT, and DEFAULT before NOT NULL. Every text column Ptah can write
		// empty is nullable, because Oracle stores '' as NULL.
		return oracleCreateTableIfAbsent(fmt.Sprintf(`CREATE TABLE %s (
    run_id VARCHAR2(64) NOT NULL,
    seq NUMBER(19) NOT NULL,
    operation VARCHAR2(32) NOT NULL,
    version NUMBER(19) NOT NULL,
    state VARCHAR2(32) NOT NULL,
    actor VARCHAR2(256) NULL,
    actor_source VARCHAR2(32) NOT NULL,
    checksum VARCHAR2(64) NULL,
    logged_at TIMESTAMP NOT NULL,
    error CLOB NULL,
    PRIMARY KEY (run_id, seq)
)`, qualifiedTable))
	default:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    run_id VARCHAR(64) NOT NULL,
    seq BIGINT NOT NULL,
    operation VARCHAR(32) NOT NULL,
    version BIGINT NOT NULL,
    state VARCHAR(32) NOT NULL,
    actor VARCHAR(256) NULL,
    actor_source VARCHAR(32) NOT NULL,
    checksum VARCHAR(64) NULL,
    logged_at %s NOT NULL,
    error TEXT NULL,
    PRIMARY KEY (run_id, seq)
)%s`, qualifiedTable, revisionTimestampType(dialect),
			revisionEngineClauseFor(dialect, engine))
	}
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
// A write that fails is reported to the run's logger and does not fail the
// migration. The log exists to explain what happened afterwards; refusing to
// apply a migration because its note could not be written would make an
// investigation aid into an outage.
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
	}
}

func (m *Migrator) writeMigrationLogEntry(ctx context.Context, entry MigrationLogEntry) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, fmt.Sprintf(
		`INSERT INTO %s (run_id, seq, operation, version, state, actor, actor_source, checksum, logged_at, error)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, m.migrationLogTable()))
	return executeSQLOn(ctx, m.conn, query,
		entry.RunID, entry.Seq, entry.Operation, entry.Version, string(entry.State),
		nullableText(entry.Actor), string(entry.ActorSource), nullableText(entry.Checksum),
		entry.At, nullableText(entry.Error))
}

// nullableText writes an empty string as NULL, because Oracle reads one back
// that way whatever was sent and a column that means "nothing here" should not
// mean two different things depending on the engine.
func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
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
// pairing is what makes it visible. limit bounds the attempts returned; zero
// returns every one.
func (m *Migrator) MigrationLog(ctx context.Context, limit int) ([]MigrationLogAttempt, error) {
	if !m.migrationLogWritable() {
		return nil, fmt.Errorf(
			"this migrator keeps no operation log: the log is a native capability and this run uses " +
				"the Atlas-compatible revision format, which defines no such table")
	}
	if err := m.Initialize(ctx); err != nil {
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
		entry, scanErr := scanMigrationLogEntry(rows)
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

// pairMigrationLogEntries joins each start with the outcome that followed it.
//
// The entries arrive newest first, so an outcome is seen before the start it
// belongs to. Correlating by run, operation and version is enough: one run
// applies a version once, and a second attempt at the same version is a second
// run with its own identifier.
func pairMigrationLogEntries(entries []MigrationLogEntry) []MigrationLogAttempt {
	outcomes := make(map[string]MigrationLogEntry, len(entries))
	attempts := make([]MigrationLogAttempt, 0, len(entries))
	for _, entry := range entries {
		key := migrationLogAttemptKey(entry)
		if entry.State != MigrationLogStarted {
			outcomes[key] = entry
			continue
		}
		attempts = append(attempts, MigrationLogAttempt{Start: entry, Outcome: outcomes[key]})
	}
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
func scanMigrationLogEntry(rows *sql.Rows) (MigrationLogEntry, error) {
	var (
		entry       MigrationLogEntry
		state       string
		actorSource string
		actor       sql.NullString
		checksum    sql.NullString
		failure     sql.NullString
	)
	if err := rows.Scan(
		&entry.RunID, &entry.Seq, &entry.Operation, &entry.Version, &state,
		&actor, &actorSource, &checksum, &entry.At, &failure,
	); err != nil {
		return MigrationLogEntry{}, fmt.Errorf("read a migration log row: %w", err)
	}
	entry.State = MigrationLogState(state)
	entry.ActorSource = ActorSource(actorSource)
	entry.Actor = actor.String
	entry.Checksum = checksum.String
	entry.Error = failure.String
	return entry, nil
}
