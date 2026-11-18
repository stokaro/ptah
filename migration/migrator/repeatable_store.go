package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/revisiontable"
	"ptah.run/internal/sqlident"
)

// repeatableRow is one row of the repeatable migration record.
type repeatableRow struct {
	identity        string
	checksum        string
	state           string
	attemptChecksum string
	attemptOrder    int
	transactional   bool
	// runID names the run that last attempted the identity. A claim matches
	// it, and writes its own, so a row another run claimed no longer matches.
	runID       string
	attemptedAt time.Time
	appliedAt   time.Time
	errorText   string
	// hasBody reports that the record keeps the content that last applied. The
	// bytes are read only where a replay needs them, by
	// [Migrator.readRepeatableBody].
	hasBody bool
}

// repeatableTableName is the bare name of the record, derived from the
// revision table's as the operation log's is, so an operator who renamed or
// moved one finds the other beside it.
func (m *Migrator) repeatableTableName() string {
	return m.migrationsTableName() + revisiontable.RepeatableSuffix
}

// repeatableTable is the record's quoted, schema-qualified name.
func (m *Migrator) repeatableTable() string {
	return m.qualifiedMetadataTable(m.repeatableTableName())
}

// repeatableUpdate is the statement that sets assignments on the record's row
// of one identity, which it binds first among the condition's values. guard
// adds conditions to the key's, each starting with AND.
func (m *Migrator) repeatableUpdate(assignments, guard string) string {
	template := m.revisionTable().RepeatableUpdate
	if template == "" {
		template = "UPDATE {table} SET {assignments} WHERE {where}"
	}
	return strings.NewReplacer("{table}", m.repeatableTable(), "{assignments}", assignments,
		"{where}", m.repeatableIdentity()+" = ?"+guard).Replace(template)
}

// repeatableIdentity is the record's key column as every statement names it.
// IDENTITY is a reserved word in T-SQL, where the bare name is a syntax error,
// so the name is spelled the way [sqlident.Ident] spells a column the record's
// DDL creates: quoted where the engine needs it, and bare on Oracle, where a
// quoted lower-case name is another column.
func (m *Migrator) repeatableIdentity() string {
	return sqlident.Ident(m.connectionDialect(), "identity")
}

// readRepeatableRows reads the record, keyed by identity. A database that has
// no record yet has no rows: the table is created by the first run that has
// repeatable work, so a status or a dry run never creates it. A record table
// another role owns is refused before it is read, as the revision table is,
// because a SELECT on it can run what its owner attached to it.
func (m *Migrator) readRepeatableRows(ctx context.Context) (map[string]repeatableRow, error) {
	rows := make(map[string]repeatableRow)
	name := m.repeatableTableName()
	exists, err := m.metadataTableExists(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to read the repeatable migration record: %w", err)
	}
	if !exists {
		return rows, nil
	}
	if err := m.refuseForeignMetadataTable(ctx, name); err != nil {
		return nil, err
	}
	// A status reads every row and needs no body, so the query asks only
	// whether one is kept. An empty body is kept too: it is not NULL.
	query := "SELECT " + m.repeatableIdentity() + ", checksum, state, attempt_checksum, attempt_order, transactional, " +
		"run_id, attempted_at, applied_at, error, CASE WHEN body IS NULL THEN 0 ELSE 1 END FROM " + m.repeatableTable()
	read, err := queryMigrationRowsFrom(ctx, m.conn, query, m.scanRepeatableRow,
		"failed to read the repeatable migration record",
		"failed to scan the repeatable migration record",
		"failed to iterate the repeatable migration record")
	if err != nil {
		return nil, err
	}
	for _, row := range read {
		rows[row.identity] = row
	}
	return rows, nil
}

// scanRepeatableRow reads one row of the record.
func (m *Migrator) scanRepeatableRow(scanner rowScanner) (repeatableRow, error) {
	var row repeatableRow
	var transactional int64
	var attemptedAt, appliedAt any
	// checksum is empty until a content applies, which an engine that stores
	// an empty string as NULL reads back as NULL.
	var checksum, runID, errorText sql.NullString
	var hasBody int64
	if err := scanner.Scan(
		&row.identity, &checksum, &row.state, &row.attemptChecksum, &row.attemptOrder, &transactional,
		&runID, &attemptedAt, &appliedAt, &errorText, &hasBody,
	); err != nil {
		return repeatableRow{}, err
	}
	row.checksum = checksum.String
	row.runID = runID.String
	row.transactional = transactional != 0
	row.hasBody = hasBody != 0
	row.errorText = errorText.String
	var err error
	if row.attemptedAt, err = m.parseStoredTime("attempted_at", attemptedAt); err != nil {
		return repeatableRow{}, err
	}
	if row.appliedAt, err = m.parseStoredTime("applied_at", appliedAt); err != nil {
		return repeatableRow{}, err
	}
	return row, nil
}

// ensureRepeatableTable creates the record when it is absent. The checks that
// need no write ran when the run was planned: the name the target would
// truncate ([Migrator.refuseUnaddressableRepeatableRecord]) and an existing
// table somebody else owns ([Migrator.readRepeatableRows]). The ownership
// check runs again after the create, which is what catches a table created
// under the name in between.
//
// The template takes {table}, {engine} and {object}, the literal SQL Server's
// existence guard reads.
func (m *Migrator) ensureRepeatableTable(ctx context.Context) error {
	ddl := expandTableStatement(
		m.revisionTable().CreateRepeatables,
		m.repeatableTable(),
		sqlStringLiteral(m.metadataObjectName(m.repeatableTableName())),
		revisionEngineClauseFor(m.connectionDialect(), m.migrationsEngine),
	)
	create, err := m.createsMetadataTable(ctx, m.repeatableTableName())
	if err != nil {
		return fmt.Errorf("failed to create the repeatable migration record: %w", err)
	}
	if create {
		if _, err := m.conn.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("failed to create the repeatable migration record: %w", err)
		}
	}
	return m.refuseForeignMetadataTable(ctx, m.repeatableTableName())
}

// readRepeatableBody reads the content the record keeps for identity, which
// only a replay of a record whose file no longer hashes to it needs. Whether
// a body is kept is the row's hasBody, never these bytes, so an empty body
// reads as kept whatever slice the driver returns for it.
func (m *Migrator) readRepeatableBody(ctx context.Context, identity string) ([]byte, error) {
	query := sqlutil.Rebind(m.connectionDialect(), "SELECT body FROM "+m.repeatableTable()+" WHERE "+m.repeatableIdentity()+" = ?")
	var body []byte
	if err := m.conn.QueryRowContext(ctx, query, identity).Scan(&body); err != nil {
		return nil, fmt.Errorf("failed to read the body of repeatable migration %s: %w", identity, err)
	}
	return body, nil
}

// ErrRepeatableClaimLost is returned when another run claimed a repeatable
// migration's row after this run read the record. The run that loses the
// claim stops before any statement of the body.
var ErrRepeatableClaimLost = errors.New("another run changed the repeatable migration's record after this run read it")

// errRepeatableRowUnmatched is a claim's UPDATE that matched no row: another
// run's claim replaced the run this run read, or the row is gone.
var errRepeatableRowUnmatched = fmt.Errorf("%w: no row names the run this run read", ErrRepeatableClaimLost)

// markRepeatableAttempt claims the identity for this run and records that an
// attempt is about to run, before any statement of its body does. It commits
// on its own, so a run that stops anywhere after it leaves a row that says an
// attempt started and has no outcome. The digest recorded as applied is left
// as it was.
//
// The claim is the write itself, and the run ID decides it: every run has its
// own, and a claim writes it. An UPDATE matches the row only while it still
// names the run this run read, and an INSERT is refused by the key when
// another run inserted first. A write that does not succeed cleanly is
// settled by [Migrator.settleRepeatableClaim]. Either way exactly one of two
// runs that read the same record goes on to run the body; the other refuses
// with [ErrRepeatableClaimLost].
func (m *Migrator) markRepeatableAttempt(ctx context.Context, attempt repeatableAttempt) error {
	err := m.writeRepeatableAttempt(ctx, attempt)
	if err == nil {
		return nil
	}
	return m.settleRepeatableClaim(ctx, attempt.repeatable.identity, err)
}

// writeRepeatableAttempt writes the attempt's row: an UPDATE of the row the
// run read, or an INSERT where it read none. An UPDATE that matched no row
// returns errRepeatableRowUnmatched.
//
// An engine whose UPDATE counts no rows cannot tell a claim it won from one
// it lost, so there the row is updated by its key alone and nothing decides
// between two runs that read the same record.
func (m *Migrator) writeRepeatableAttempt(ctx context.Context, attempt repeatableAttempt) error {
	repeatable := attempt.repeatable
	dialect := m.connectionDialect()
	now := nativeRevisionTimestamp(time.Now())
	if !attempt.recorded {
		query := "INSERT INTO " + m.repeatableTable() + " (" + m.repeatableIdentity() + ", checksum, state, attempt_checksum, attempt_order, " +
			"transactional, run_id, attempted_at, applied_at, error) VALUES (?, '', ?, ?, ?, ?, ?, ?, NULL, NULL)"
		return executeSQLOn(ctx, m.conn, sqlutil.Rebind(dialect, query),
			repeatable.identity, RepeatableStateRunning, repeatable.checksum, attempt.order, attempt.transactionalColumn(),
			m.migrationLogRunID, now)
	}
	assignments := "state = ?, attempt_checksum = ?, attempt_order = ?, transactional = ?, run_id = ?, " +
		"attempted_at = ?, error = NULL"
	args := []any{
		RepeatableStateRunning, repeatable.checksum, attempt.order, attempt.transactionalColumn(),
		m.migrationLogRunID, now, repeatable.identity,
	}
	if m.revisionTable().UncountedUpdates {
		return executeSQLOn(ctx, m.conn, sqlutil.Rebind(dialect, m.repeatableUpdate(assignments, "")), args...)
	}
	query := m.repeatableUpdate(assignments, " AND run_id = ?")
	result, err := m.conn.ExecContext(ctx, sqlutil.Rebind(dialect, query), append(args, attempt.read.runID)...)
	if err != nil {
		return err
	}
	claimed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count the claimed row: %w", err)
	}
	if claimed == 0 {
		return errRepeatableRowUnmatched
	}
	return nil
}

// settleRepeatableClaim decides a claim whose write did not succeed cleanly:
// an UPDATE that matched no row, an INSERT the server refused, or any error
// from either, such as a serialization failure under contention. The row's run
// ID, read back on the run's connection, says who holds the identity. This
// run's ID means its write took effect, a response lost after the INSERT
// committed or a migrator run again over the row its own attempt left, and
// the claim holds. Another run's ID means the claim is lost. When the read
// fails or finds no row, the write's own error stands.
func (m *Migrator) settleRepeatableClaim(ctx context.Context, identity string, claimErr error) error {
	holder, found, err := m.repeatableRowRunID(ctx, identity)
	if err != nil || !found {
		return claimErr
	}
	if holder == m.migrationLogRunID {
		return nil
	}
	if errors.Is(claimErr, errRepeatableRowUnmatched) {
		return fmt.Errorf("%w: run %q holds the row", ErrRepeatableClaimLost, holder)
	}
	return fmt.Errorf("%w: run %q holds the row: %w", ErrRepeatableClaimLost, holder, claimErr)
}

// repeatableRowRunID reads the run ID of the record's row for identity, and
// whether there is one.
func (m *Migrator) repeatableRowRunID(ctx context.Context, identity string) (string, bool, error) {
	query := sqlutil.Rebind(m.connectionDialect(),
		"SELECT run_id FROM "+m.repeatableTable()+" WHERE "+m.repeatableIdentity()+" = ?")
	var runID sql.NullString
	err := m.conn.QueryRowContext(ctx, query, identity).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return runID.String, true, nil
}

// completeRepeatableOn records the attempt's content as applied, its digest
// and its bytes, on conn: the body's own transaction when it ran in one, so the
// record and the work commit together or not at all. A record whose last
// transactional attempt is unfinished therefore describes the content that
// applied before it.
func (m *Migrator) completeRepeatableOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	repeatable *repeatableMigration,
) error {
	query, args := m.repeatableCompletion(repeatable)
	return executeSQLOn(ctx, conn, sqlutil.Rebind(m.connectionDialect(), query), args...)
}

// repeatableCompletion is the statement that records the attempt's content
// as applied, and the values it binds.
func (m *Migrator) repeatableCompletion(repeatable *repeatableMigration) (string, []any) {
	assignments := "state = ?, checksum = ?, body = ?, applied_at = ?, error = NULL"
	args := []any{
		RepeatableStateApplied, repeatable.checksum, repeatable.body, nativeRevisionTimestamp(time.Now()),
		repeatable.identity,
	}
	// An empty file is a kept body. Where a bound empty value is stored as
	// NULL, which reads as no body, the engine's own expression stores it.
	if empty := m.revisionTable().EmptyBinary; empty != "" && len(repeatable.body) == 0 {
		assignments = "state = ?, checksum = ?, body = " + empty + ", applied_at = ?, error = NULL"
		args = slices.Delete(args, 2, 3)
	}
	return m.repeatableUpdate(assignments, ""), args
}

// refuseRepeatablesOverQuerySize refuses, before anything runs, a repeatable
// migration whose body the record cannot store on an engine whose driver
// writes bound values into the statement text. Without the refusal the body
// runs, the server refuses the statement that records it, and the row stays
// running; a retry runs the body again and fails the same way.
//
// The limit is the session's, read from the server, so a limit raised for the
// migration user is the one compared.
func (m *Migrator) refuseRepeatablesOverQuerySize(
	ctx context.Context,
	state *repeatableState,
	work []RepeatableWork,
) error {
	table := m.revisionTable()
	if table.QuerySizeLimit == "" || len(work) == 0 {
		return nil
	}
	var limit int64
	if err := m.conn.QueryRowContext(ctx, table.QuerySizeLimit).Scan(&limit); err != nil {
		return fmt.Errorf("failed to read the server's %s: %w", table.QuerySizeSetting, err)
	}
	for _, item := range work {
		repeatable := state.repeatable(item.Identity)
		query, args := m.repeatableCompletion(repeatable)
		if err := refuseStatementOverLimit(repeatable.path, query, args, limit, table.QuerySizeSetting); err != nil {
			return err
		}
	}
	return nil
}

// refuseStatementOverLimit returns a [RepeatableStatementTooLongError] when
// query, with args written into its text, is longer than limit, the value of
// setting. A statement exactly limit bytes long is accepted.
func refuseStatementOverLimit(path, query string, args []any, limit int64, setting string) error {
	length := inlinedStatementLength(query, args)
	if int64(length) <= limit {
		return nil
	}
	return &RepeatableStatementTooLongError{Path: path, Length: length, Limit: limit, Setting: setting}
}

// inlinedStatementLength is the length in bytes, at least, of query once a
// driver has written each bound value into its text the way clickhouse-go
// does: a string or byte slice as a quoted literal whose backslashes, quotes
// and NUL bytes are escaped with a backslash, NULL for a nil byte slice, and a
// timestamp as a call no longer than 64 bytes.
func inlinedStatementLength(query string, args []any) int {
	length := len(query)
	for _, arg := range args {
		switch value := arg.(type) {
		case []byte:
			length += inlinedLiteralLength(string(value))
		case string:
			length += inlinedLiteralLength(value)
		case time.Time:
			length += 64
		default:
			length += len(fmt.Sprint(value))
		}
	}
	return length
}

// inlinedLiteralLength is the length of value as a quoted, escaped literal.
func inlinedLiteralLength(value string) int {
	return len(value) + 2 + strings.Count(value, `\`) + strings.Count(value, "'") + strings.Count(value, "\x00")
}

// failRepeatable records that the attempt's body failed. It runs on a context
// of its own, detached from the one the body ran under, because a body stopped
// by a deadline or an interrupt arrives here with that context already done.
func (m *Migrator) failRepeatable(ctx context.Context, repeatable *repeatableMigration, failure error) error {
	durable, cancel := durableRevisionWriteContext(ctx)
	defer cancel()
	query := m.repeatableUpdate("state = ?, error = ?", "")
	return executeSQLOn(durable, m.conn, sqlutil.Rebind(m.connectionDialect(), query),
		RepeatableStateFailed, failure.Error(), repeatable.identity)
}
