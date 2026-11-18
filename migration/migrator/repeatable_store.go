package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/revisiontable"
)

// repeatableRow is one row of the repeatable migration record.
type repeatableRow struct {
	identity        string
	checksum        string
	state           string
	attemptChecksum string
	attemptOrder    int
	transactional   bool
	attemptedAt     time.Time
	appliedAt       time.Time
	errorText       string
	// body is the content that last applied, nil where the record keeps none.
	body []byte
}

// repeatableBodyColumn is the record's column holding the content that last
// applied, and repeatableBodyDefinition the definition it is added with to a
// record created without it, where the engine's descriptor states none.
const (
	repeatableBodyColumn     = "body"
	repeatableBodyDefinition = "BYTEA NULL"
)

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
	// A record created without the body column reads as one that keeps no
	// body. Reading never adds it, so a status or a dry run changes nothing.
	withBody, err := m.metadataColumnExists(ctx, name, repeatableBodyColumn)
	if err != nil {
		return nil, fmt.Errorf("failed to read the repeatable migration record: %w", err)
	}
	body := "NULL"
	if withBody {
		body = repeatableBodyColumn
	}
	query := "SELECT identity, checksum, state, attempt_checksum, attempt_order, transactional, " +
		"attempted_at, applied_at, error, " + body + " FROM " + m.repeatableTable()
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
	var errorText sql.NullString
	if err := scanner.Scan(
		&row.identity, &row.checksum, &row.state, &row.attemptChecksum, &row.attemptOrder, &transactional,
		&attemptedAt, &appliedAt, &errorText, &row.body,
	); err != nil {
		return repeatableRow{}, err
	}
	row.transactional = transactional != 0
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
// The template takes {table} and {engine}. No engine that declares one names
// {object}, so the migrator fills in nothing for it.
func (m *Migrator) ensureRepeatableTable(ctx context.Context) error {
	ddl := expandTableStatement(
		m.revisionTable().CreateRepeatables,
		m.repeatableTable(),
		"",
		revisionEngineClauseFor(m.connectionDialect(), m.migrationsEngine),
	)
	if _, err := m.conn.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("failed to create the repeatable migration record: %w", err)
	}
	if err := m.refuseForeignMetadataTable(ctx, m.repeatableTableName()); err != nil {
		return err
	}
	err := m.ensureMetadataColumn(ctx, m.repeatableTableName(), repeatableBodyColumn, repeatableBodyDefinition)
	if err != nil {
		return fmt.Errorf("failed to add column %s to the repeatable migration record: %w", repeatableBodyColumn, err)
	}
	return nil
}

// markRepeatableAttempt records that an attempt is about to run, before any
// statement of its body does. It commits on its own, so a run that stops
// anywhere after it leaves a row that says an attempt started and has no
// outcome. The digest recorded as applied is left as it was.
func (m *Migrator) markRepeatableAttempt(ctx context.Context, attempt repeatableAttempt) error {
	dialect := m.connectionDialect()
	now := nativeRevisionTimestamp(time.Now())
	repeatable := attempt.repeatable
	if attempt.recorded {
		query := "UPDATE " + m.repeatableTable() + " SET state = ?, attempt_checksum = ?, attempt_order = ?, " +
			"transactional = ?, run_id = ?, attempted_at = ?, error = NULL WHERE identity = ?"
		return executeSQLOn(ctx, m.conn, sqlutil.Rebind(dialect, query),
			RepeatableStateRunning, repeatable.checksum, attempt.order, attempt.transactionalColumn(),
			m.migrationLogRunID, now, repeatable.identity)
	}
	query := "INSERT INTO " + m.repeatableTable() + " (identity, checksum, state, attempt_checksum, attempt_order, " +
		"transactional, run_id, attempted_at, applied_at, error) VALUES (?, '', ?, ?, ?, ?, ?, ?, NULL, NULL)"
	return executeSQLOn(ctx, m.conn, sqlutil.Rebind(dialect, query),
		repeatable.identity, RepeatableStateRunning, repeatable.checksum, attempt.order, attempt.transactionalColumn(),
		m.migrationLogRunID, now)
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
	query := "UPDATE " + m.repeatableTable() + " SET state = ?, checksum = ?, " + repeatableBodyColumn +
		" = ?, applied_at = ?, error = NULL WHERE identity = ?"
	return executeSQLOn(ctx, conn, sqlutil.Rebind(m.connectionDialect(), query),
		RepeatableStateApplied, repeatable.checksum, repeatable.body, nativeRevisionTimestamp(time.Now()),
		repeatable.identity)
}

// failRepeatable records that the attempt's body failed. It runs on a context
// of its own, detached from the one the body ran under, because a body stopped
// by a deadline or an interrupt arrives here with that context already done.
func (m *Migrator) failRepeatable(ctx context.Context, repeatable *repeatableMigration, failure error) error {
	durable, cancel := durableRevisionWriteContext(ctx)
	defer cancel()
	query := "UPDATE " + m.repeatableTable() + " SET state = ?, error = ? WHERE identity = ?"
	return executeSQLOn(durable, m.conn, sqlutil.Rebind(m.connectionDialect(), query),
		RepeatableStateFailed, failure.Error(), repeatable.identity)
}
