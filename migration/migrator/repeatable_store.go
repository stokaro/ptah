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
	// runID names the run that last attempted the identity. A claim compares
	// it, so a row another run claimed and left as it found it still differs.
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
	if _, err := m.conn.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("failed to create the repeatable migration record: %w", err)
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

// ErrRepeatableClaimLost is returned when another run changed a repeatable
// migration's row after this run read the record and before it claimed the
// row. The run that loses the claim stops before any statement of the body.
var ErrRepeatableClaimLost = errors.New("another run changed the repeatable migration's record after this run read it")

// markRepeatableAttempt claims the identity for this run and records that an
// attempt is about to run, before any statement of its body does. It commits
// on its own, so a run that stops anywhere after it leaves a row that says an
// attempt started and has no outcome. The digest recorded as applied is left
// as it was.
//
// The claim is the write itself. An UPDATE matches only the row as this run
// read it, its state, applied digest and last run, and an INSERT is refused by
// the key when another run inserted first. Either way exactly one of two runs
// that read the same record goes on to run the body; the other refuses with
// [ErrRepeatableClaimLost].
func (m *Migrator) markRepeatableAttempt(ctx context.Context, attempt repeatableAttempt) error {
	if attempt.recorded {
		return m.claimRecordedRepeatable(ctx, attempt)
	}
	repeatable := attempt.repeatable
	query := "INSERT INTO " + m.repeatableTable() + " (" + m.repeatableIdentity() + ", checksum, state, attempt_checksum, attempt_order, " +
		"transactional, run_id, attempted_at, applied_at, error) VALUES (?, '', ?, ?, ?, ?, ?, ?, NULL, NULL)"
	err := executeSQLOn(ctx, m.conn, sqlutil.Rebind(m.connectionDialect(), query),
		repeatable.identity, RepeatableStateRunning, repeatable.checksum, attempt.order, attempt.transactionalColumn(),
		m.migrationLogRunID, nativeRevisionTimestamp(time.Now()))
	if err == nil {
		return nil
	}
	// The key is what refuses the second insert, and each engine words that
	// refusal its own way. A row that exists now, where this run read none,
	// is the other run's claim.
	if exists, countErr := m.repeatableRowExists(ctx, repeatable.identity); countErr == nil && exists {
		return fmt.Errorf("%w: %w", ErrRepeatableClaimLost, err)
	}
	return err
}

// claimRecordedRepeatable updates the row this run read into the running
// state, matching it only while it is still as read.
//
// An engine whose UPDATE counts no rows cannot tell a claim it won from one
// it lost, so there the row is updated by its key alone and nothing decides
// between two runs that read the same record.
func (m *Migrator) claimRecordedRepeatable(ctx context.Context, attempt repeatableAttempt) error {
	repeatable := attempt.repeatable
	read := attempt.read
	assignments := "state = ?, attempt_checksum = ?, attempt_order = ?, transactional = ?, run_id = ?, " +
		"attempted_at = ?, error = NULL"
	args := []any{
		RepeatableStateRunning, repeatable.checksum, attempt.order, attempt.transactionalColumn(),
		m.migrationLogRunID, nativeRevisionTimestamp(time.Now()), repeatable.identity,
	}
	if m.revisionTable().UncountedUpdates {
		return executeSQLOn(ctx, m.conn, sqlutil.Rebind(m.connectionDialect(), m.repeatableUpdate(assignments, "")),
			args...)
	}
	checksum, checksumArgs := repeatableColumnIs("checksum", read.checksum)
	runID, runIDArgs := repeatableColumnIs("run_id", read.runID)
	query := m.repeatableUpdate(assignments, " AND state = ? AND "+checksum+" AND "+runID)
	args = append(append(append(args, read.state), checksumArgs...), runIDArgs...)
	result, err := m.conn.ExecContext(ctx, sqlutil.Rebind(m.connectionDialect(), query), args...)
	if err != nil {
		return err
	}
	claimed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count the claimed row: %w", err)
	}
	if claimed == 0 {
		return fmt.Errorf("%w (read as %s, %s)", ErrRepeatableClaimLost, read.state, displayRepeatableChecksum(read.checksum))
	}
	return nil
}

// repeatableColumnIs is the condition that column still holds value, the way
// the run read it. An empty value matches NULL too, because an engine that
// stores an empty string as NULL reads it back as empty.
func repeatableColumnIs(column, value string) (string, []any) {
	if value == "" {
		return "(" + column + " IS NULL OR " + column + " = '')", nil
	}
	return column + " = ?", []any{value}
}

// displayRepeatableChecksum names an applied digest in a message; a row whose
// content never applied has none.
func displayRepeatableChecksum(checksum string) string {
	if checksum == "" {
		return "no applied digest"
	}
	return "applied digest " + checksum
}

// repeatableRowExists reports whether the record holds a row for identity.
func (m *Migrator) repeatableRowExists(ctx context.Context, identity string) (bool, error) {
	query := sqlutil.Rebind(m.connectionDialect(),
		"SELECT count(*) FROM "+m.repeatableTable()+" WHERE "+m.repeatableIdentity()+" = ?")
	var count int64
	if err := m.conn.QueryRowContext(ctx, query, identity).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
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
	return executeSQLOn(ctx, conn, sqlutil.Rebind(m.connectionDialect(), m.repeatableUpdate(assignments, "")), args...)
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
