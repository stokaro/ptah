package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
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
	attemptedAt     time.Time
	appliedAt       time.Time
	errorText       string
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

// repeatableObjectName is the record's name as SQL Server's OBJECT_ID probe
// takes it, built as [Migrator.migrationLogObjectName] builds the log's.
func (m *Migrator) repeatableObjectName() string {
	table := m.quoteIdentifier(m.repeatableTableName())
	if schema := m.metadataTableSchemaName(); schema != "" {
		return m.quoteIdentifier(schema) + "." + table
	}
	return table
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
		"attempted_at, applied_at, error, CASE WHEN body IS NULL THEN 0 ELSE 1 END FROM " + m.repeatableTable()
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
	var checksum, errorText sql.NullString
	var hasBody int64
	if err := scanner.Scan(
		&row.identity, &checksum, &row.state, &row.attemptChecksum, &row.attemptOrder, &transactional,
		&attemptedAt, &appliedAt, &errorText, &hasBody,
	); err != nil {
		return repeatableRow{}, err
	}
	row.checksum = checksum.String
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
		sqlStringLiteral(m.repeatableObjectName()),
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
			"transactional = ?, run_id = ?, attempted_at = ?, error = NULL WHERE " + m.repeatableIdentity() + " = ?"
		return executeSQLOn(ctx, m.conn, sqlutil.Rebind(dialect, query),
			RepeatableStateRunning, repeatable.checksum, attempt.order, attempt.transactionalColumn(),
			m.migrationLogRunID, now, repeatable.identity)
	}
	query := "INSERT INTO " + m.repeatableTable() + " (" + m.repeatableIdentity() + ", checksum, state, attempt_checksum, attempt_order, " +
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
	set := "SET state = ?, checksum = ?, body = ?, applied_at = ?, error = NULL"
	args := []any{
		RepeatableStateApplied, repeatable.checksum, repeatable.body, nativeRevisionTimestamp(time.Now()),
		repeatable.identity,
	}
	// An empty file is a kept body. Where a bound empty value is stored as
	// NULL, which reads as no body, the engine's own expression stores it.
	if empty := m.revisionTable().EmptyBinary; empty != "" && len(repeatable.body) == 0 {
		set = "SET state = ?, checksum = ?, body = " + empty + ", applied_at = ?, error = NULL"
		args = slices.Delete(args, 2, 3)
	}
	query := "UPDATE " + m.repeatableTable() + " " + set + " WHERE " + m.repeatableIdentity() + " = ?"
	return executeSQLOn(ctx, conn, sqlutil.Rebind(m.connectionDialect(), query), args...)
}

// failRepeatable records that the attempt's body failed. It runs on a context
// of its own, detached from the one the body ran under, because a body stopped
// by a deadline or an interrupt arrives here with that context already done.
func (m *Migrator) failRepeatable(ctx context.Context, repeatable *repeatableMigration, failure error) error {
	durable, cancel := durableRevisionWriteContext(ctx)
	defer cancel()
	query := "UPDATE " + m.repeatableTable() + " SET state = ?, error = ? WHERE " + m.repeatableIdentity() + " = ?"
	return executeSQLOn(durable, m.conn, sqlutil.Rebind(m.connectionDialect(), query),
		RepeatableStateFailed, failure.Error(), repeatable.identity)
}
