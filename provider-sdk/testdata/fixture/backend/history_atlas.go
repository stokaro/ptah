package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

type atlasColumn struct {
	name       string
	types      []string
	required   bool
	key        bool
	defaultSQL string
}

// The fixture owns its storage codec. This is the public Atlas column layout;
// no host imports or migration-state decisions are needed to read it.
var atlasColumns = []atlasColumn{
	{"version", []string{"VARCHAR(255)", "VARCHAR", "TEXT"}, false, true, ""},
	{"description", []string{"TEXT", "VARCHAR(255)", "VARCHAR"}, true, false, ""},
	{"type", []string{"BIGINT", "INTEGER", "INT"}, true, false, "2"},
	{"applied", []string{"BIGINT", "INTEGER", "INT"}, true, false, "0"},
	{"total", []string{"BIGINT", "INTEGER", "INT"}, true, false, "0"},
	{"executed_at", []string{"TIMESTAMP", "DATETIME"}, true, false, ""},
	{"execution_time", []string{"BIGINT", "INTEGER", "INT"}, true, false, ""},
	{"error", []string{"TEXT", "VARCHAR"}, false, false, ""},
	{"error_stmt", []string{"TEXT", "VARCHAR"}, false, false, ""},
	{"hash", []string{"VARCHAR(255)", "VARCHAR", "TEXT"}, true, false, ""},
	{"partial_hashes", []string{"JSON", "TEXT"}, false, false, ""},
	{"operator_version", []string{"VARCHAR(255)", "VARCHAR", "TEXT"}, true, false, ""},
}

func atlasHistoryDDL(table string) string {
	columns := make([]string, 0, len(atlasColumns))
	for _, column := range atlasColumns {
		definition := quote(column.name) + " " + column.types[0]
		if column.required {
			definition += " NOT NULL"
		}
		if column.key {
			definition += " PRIMARY KEY"
		}
		if column.defaultSQL != "" {
			definition += " DEFAULT " + column.defaultSQL
		}
		columns = append(columns, definition)
	}
	return "CREATE TABLE " + table + " (" + strings.Join(columns, ", ") + ")"
}

func atlasColumnNames() []string {
	result := make([]string, 0, len(atlasColumns))
	for _, column := range atlasColumns {
		result = append(result, quote(column.name))
	}
	return result
}

func checkAtlasHistoryColumns(ctx context.Context, runner historyRunner, table string) error {
	rows, err := runner.QueryContext(ctx, "PRAGMA main.table_xinfo("+table+")")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	columns := make(map[string]atlasColumn, len(atlasColumns))
	for _, column := range atlasColumns {
		columns[column.name] = column
	}
	count := 0
	for rows.Next() {
		var position, required, key, hidden int
		var name, kind string
		var defaultValue sql.NullString
		if err := rows.Scan(&position, &name, &kind, &required, &defaultValue, &key, &hidden); err != nil {
			return err
		}
		column, exists := columns[name]
		if !exists || hidden != 0 || !slices.Contains(column.types, strings.ToUpper(kind)) || (key == 1) != column.key || key > 1 {
			return fmt.Errorf("fixture Atlas history has an incompatible column")
		}
		// SQLite permits either explicit NOT NULL or its ordinary text primary
		// key spelling. Both preserve the same non-null identities we write.
		if !column.key && (required != 0) != column.required {
			return fmt.Errorf("fixture Atlas history has incompatible nullability")
		}
		delete(columns, name)
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != len(atlasColumns) {
		return fmt.Errorf("fixture Atlas history has missing columns")
	}
	return nil
}

func checkAtlasHistoryIndexes(ctx context.Context, runner historyRunner, table string) error {
	rows, err := runner.QueryContext(ctx, "PRAGMA main.index_list("+table+")")
	if err != nil {
		return err
	}
	var primary string
	for rows.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err := rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			_ = rows.Close()
			return err
		}
		if unique != 0 {
			if origin != "pk" || partial != 0 {
				_ = rows.Close()
				return fmt.Errorf("fixture Atlas history has an unexpected unique index")
			}
			primary = name
		}
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	if primary == "" {
		return fmt.Errorf("fixture Atlas history has no identity index")
	}
	keys, err := runner.QueryContext(ctx, "PRAGMA main.index_xinfo("+quote(primary)+")")
	if err != nil {
		return err
	}
	defer func() { _ = keys.Close() }()
	count := 0
	for keys.Next() {
		var sequence, column, descending, key int
		var name, collation sql.NullString
		if err := keys.Scan(&sequence, &column, &name, &descending, &collation, &key); err != nil {
			return err
		}
		if key == 0 {
			continue
		}
		if name.String != "version" || collation.String != "BINARY" {
			return fmt.Errorf("fixture Atlas identity index is not exact")
		}
		count++
	}
	if err := keys.Err(); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("fixture Atlas identity index has unexpected keys")
	}
	foreign, err := runner.QueryContext(ctx, "PRAGMA main.foreign_key_list("+table+")")
	if err != nil {
		return err
	}
	defer func() { _ = foreign.Close() }()
	if foreign.Next() {
		return fmt.Errorf("fixture Atlas history has foreign keys")
	}
	return foreign.Err()
}

func readAtlasHistory(ctx context.Context, runner historyRunner, table string, limit uint32, version *string) ([]*protocol.HistoryRevision, error) {
	query := "SELECT " + strings.Join(atlasColumnNames(), ", ") + " FROM main." + table
	var args []any
	if version != nil {
		query += " WHERE version COLLATE BINARY = ?"
		args = append(args, *version)
	}
	query += " ORDER BY version COLLATE BINARY LIMIT ?"
	args = append(args, limit+1)
	rows, err := runner.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []*protocol.HistoryRevision
	for rows.Next() {
		row, err := scanAtlasHistory(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func scanAtlasHistory(rows *sql.Rows) (*protocol.HistoryRevision, error) {
	row := &protocol.HistoryRevision{}
	var executedAt any
	var failure, statement, hashes sql.NullString
	if err := rows.Scan(&row.Version, &row.Description, &row.AtlasType, &row.Applied, &row.Total, &executedAt,
		&row.ExecutionNanoseconds, &failure, &statement, &row.Checksum, &hashes, &row.OperatorVersion); err != nil {
		return nil, err
	}
	row.Error, row.ErrorStatement = failure.String, statement.String
	if hashes.Valid {
		if err := json.Unmarshal([]byte(hashes.String), &row.PartialHashes); err != nil {
			return nil, fmt.Errorf("fixture Atlas partial hashes are invalid: %w", err)
		}
	}
	at, err := atlasHistoryTime(executedAt)
	if err != nil {
		return nil, err
	}
	nanos := at.Nanosecond()
	if nanos < 0 || nanos >= 1_000_000_000 {
		return nil, fmt.Errorf("fixture Atlas timestamp has invalid nanoseconds")
	}
	row.AppliedAtSeconds, row.AppliedAtNanos = at.Unix(), int32(nanos)
	if err := provider.ValidateHistoryRevision(protocol.HistoryScope_ATLAS, row); err != nil {
		return nil, err
	}
	return row, nil
}

func atlasHistoryTime(value any) (time.Time, error) {
	switch at := value.(type) {
	case time.Time:
		return at.UTC(), nil
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05.999999999-07:00"} {
			parsed, err := time.Parse(layout, at)
			if err == nil {
				return parsed.UTC(), nil
			}
		}
	}
	return time.Time{}, fmt.Errorf("fixture Atlas timestamp is invalid")
}

func atlasHistoryValues(row *protocol.HistoryRevision) ([]any, error) {
	hashes, err := json.Marshal(row.GetPartialHashes())
	if err != nil {
		return nil, err
	}
	at := time.Unix(row.GetAppliedAtSeconds(), int64(row.GetAppliedAtNanos())).UTC()
	return []any{row.GetVersion(), row.GetDescription(), row.GetAtlasType(), row.GetApplied(), row.GetTotal(), at,
		row.GetExecutionNanoseconds(), row.GetError(), row.GetErrorStatement(), row.GetChecksum(), string(hashes), row.GetOperatorVersion()}, nil
}

// A private transaction makes the complete record comparison atomic with the
// write. An existing caller transaction keeps ownership of its commit. There
// are no retries, including after a serialization failure or lost commit.
func (s *session) mutateAtlasHistory(ctx context.Context, request *protocol.MutateHistoryRequest, table string) (protocol.HistoryResult_Outcome, error) {
	runner, err := s.historyRunner(request.GetScope().GetTransactionId())
	if err != nil {
		return protocol.HistoryResult_UNKNOWN, err
	}
	var owned *sql.Tx
	if request.GetScope().GetTransactionId() == "" {
		owned, err = s.beginDatabaseTransaction(ctx, protocol.TransactionIsolation_SERIALIZABLE)
		if err != nil {
			return protocol.HistoryResult_UNKNOWN, err
		}
		defer func() { _ = owned.Rollback() }()
		runner = owned
	}
	exists, err := checkHistoryTable(ctx, runner, table, request.GetScope().GetTable(), protocol.HistoryScope_ATLAS)
	if err != nil {
		return protocol.HistoryResult_UNKNOWN, err
	}
	if !exists {
		return protocol.HistoryResult_REFUSED, nil
	}
	version := request.GetReplacement().GetVersion()
	if request.GetExpected() != nil {
		version = request.GetExpected().GetVersion()
	}
	rows, err := readAtlasHistory(ctx, runner, table, 1, &version)
	if err != nil {
		return protocol.HistoryResult_UNKNOWN, err
	}
	var current *protocol.HistoryRevision
	if len(rows) > 1 {
		return protocol.HistoryResult_UNKNOWN, fmt.Errorf("fixture Atlas history has duplicate identities")
	}
	if len(rows) == 1 {
		current = rows[0]
	}
	if !proto.Equal(current, request.GetExpected()) {
		return protocol.HistoryResult_CONFLICT, nil
	}
	result, err := writeAtlasHistory(ctx, runner, table, request)
	if err != nil {
		return protocol.HistoryResult_UNKNOWN, err
	}
	if owned != nil {
		if err := owned.Commit(); err != nil {
			return protocol.HistoryResult_UNKNOWN, err
		}
	}
	return result, nil
}

func writeAtlasHistory(ctx context.Context, runner historyRunner, table string, request *protocol.MutateHistoryRequest) (protocol.HistoryResult_Outcome, error) {
	query := "DELETE FROM main." + table + " WHERE version COLLATE BINARY = ?"
	args := []any{request.GetExpected().GetVersion()}
	if request.GetReplacement() != nil {
		var err error
		args, err = atlasHistoryValues(request.GetReplacement())
		if err != nil {
			return protocol.HistoryResult_UNKNOWN, err
		}
		columns := atlasColumnNames()
		query = "INSERT INTO main." + table + " (" + strings.Join(columns, ", ") + ") VALUES (" + strings.TrimSuffix(strings.Repeat("?, ", len(args)), ", ") + ")"
		if request.GetExpected() != nil {
			assignments := make([]string, 0, len(columns)-1)
			for _, column := range columns[1:] {
				assignments = append(assignments, column+" = ?")
			}
			query = "UPDATE main." + table + " SET " + strings.Join(assignments, ", ") + " WHERE version COLLATE BINARY = ?"
			args = append(args[1:], request.GetExpected().GetVersion())
		}
	}
	result, err := runner.ExecContext(ctx, query, args...)
	if err != nil {
		return protocol.HistoryResult_UNKNOWN, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return protocol.HistoryResult_UNKNOWN, err
	}
	if changed != 1 {
		return protocol.HistoryResult_CONFLICT, nil
	}
	return protocol.HistoryResult_APPLIED, nil
}
