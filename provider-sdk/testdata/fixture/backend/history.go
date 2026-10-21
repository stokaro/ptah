package backend

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

const historyEncoding = "ptah-history/revision16\x00"

type historyRunner interface {
	executor
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func historyTable(scope *protocol.HistoryScope) (string, error) {
	if scope.GetSchema() != "" && scope.GetSchema() != "main" {
		return "", fmt.Errorf("fixture history supports storage in main only")
	}
	return quote(scope.GetTable()), nil
}

func historyDDL(table string, format protocol.HistoryScope_Format) string {
	if format == protocol.HistoryScope_ATLAS {
		return atlasHistoryDDL(table)
	}
	return "CREATE TABLE " + table + ` ("version" TEXT NOT NULL PRIMARY KEY, "revision" BLOB NOT NULL)`
}

func checkHistoryTable(ctx context.Context, runner historyRunner, table, name string, format protocol.HistoryScope_Format) (bool, error) {
	var kind, definition string
	err := runner.QueryRowContext(ctx, "SELECT type, sql FROM main.sqlite_schema WHERE name = ? COLLATE NOCASE", name).Scan(&kind, &definition)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if kind != "table" {
		return false, fmt.Errorf("fixture history storage is not a table")
	}
	if format == protocol.HistoryScope_ATLAS {
		if err := checkAtlasHistoryColumns(ctx, runner, table); err != nil {
			return false, err
		}
		if err := checkAtlasHistoryIndexes(ctx, runner, table); err != nil {
			return false, err
		}
	} else if definition != historyDDL(table, format) {
		return false, fmt.Errorf("fixture history storage has an incompatible layout")
	}
	var triggers int
	query := "SELECT COUNT(*) FROM (SELECT type, tbl_name FROM main.sqlite_schema UNION ALL SELECT type, tbl_name FROM temp.sqlite_schema) " +
		"WHERE type = 'trigger' AND tbl_name = ? COLLATE NOCASE"
	if err := runner.QueryRowContext(ctx, query, name).Scan(&triggers); err != nil {
		return false, err
	}
	if triggers != 0 {
		return false, fmt.Errorf("fixture history storage has triggers")
	}
	return true, nil
}

func (s *session) historyRunner(id string) (historyRunner, error) {
	if s.closed {
		return nil, fmt.Errorf("fixture session is closed")
	}
	if _, err := s.executor(id); err != nil {
		return nil, err
	}
	if id == "" {
		return s.db, nil
	}
	return s.transactions[id].tx, nil
}

func historyResult(scope *protocol.HistoryScope, id string, outcome protocol.HistoryResult_Outcome) *protocol.HistoryResult {
	return &protocol.HistoryResult{Scope: proto.CloneOf(scope), RequestId: id, Outcome: outcome}
}

// PrepareHistory creates native or Atlas storage. Existing tables must satisfy
// the selected layout and have no triggers; an earlier layout is never upgraded.
func (p *Provider) PrepareHistory(ctx context.Context, request *protocol.PrepareHistoryRequest) (*protocol.HistoryResult, error) {
	if err := provider.ValidatePrepareHistoryRequest(request); err != nil {
		return nil, err
	}
	table, err := historyTable(request.GetScope())
	if err != nil {
		return nil, err
	}
	s, err := p.lookup(request.GetScope().GetSessionId())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.guardLocks(ctx); err != nil {
		return nil, err
	}
	runner, err := s.historyRunner("")
	if err != nil {
		return nil, err
	}
	exists, err := checkHistoryTable(ctx, runner, table, request.GetScope().GetTable(), request.GetScope().GetFormat())
	if err != nil {
		return nil, err
	}
	if !exists {
		ddl := strings.Replace(historyDDL(table, request.GetScope().GetFormat()), "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS main.", 1)
		if _, err := runner.ExecContext(ctx, ddl); err != nil {
			return nil, err
		}
	}
	exists, err = checkHistoryTable(ctx, runner, table, request.GetScope().GetTable(), request.GetScope().GetFormat())
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("fixture history store disappeared during preparation")
	}
	return historyResult(request.GetScope(), request.GetRequestId(), protocol.HistoryResult_APPLIED), nil
}

// ReadHistory reads all stored revisions in one query and refuses oversized or
// malformed histories. A missing store remains distinguishable from an empty one.
func (p *Provider) ReadHistory(ctx context.Context, request *protocol.ReadHistoryRequest) (*protocol.HistorySnapshot, error) {
	if err := provider.ValidateReadHistoryRequest(request); err != nil {
		return nil, err
	}
	table, err := historyTable(request.GetScope())
	if err != nil {
		return nil, err
	}
	s, err := p.lookup(request.GetScope().GetSessionId())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	runner, err := s.historyRunner(request.GetScope().GetTransactionId())
	if err != nil {
		return nil, err
	}
	exists, err := checkHistoryTable(ctx, runner, table, request.GetScope().GetTable(), request.GetScope().GetFormat())
	if err != nil {
		return nil, err
	}
	result := &protocol.HistorySnapshot{Scope: proto.CloneOf(request.GetScope()), Exists: exists, Complete: true}
	if !exists {
		return result, nil
	}
	if request.GetScope().GetFormat() == protocol.HistoryScope_ATLAS {
		result.Revisions, err = readAtlasHistory(ctx, runner, table, request.GetMaxRevisions(), nil)
	} else {
		result.Revisions, err = readNativeHistory(ctx, runner, table, request.GetMaxRevisions())
	}
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateHistorySnapshot(request, result); err != nil {
		return nil, err
	}
	return result, nil
}

// MutateHistory compares complete deterministic records in the database. The
// single statement makes stale writes conflict across independent sessions too.
// When a transaction is named, schema execution and history share its handle.
func (p *Provider) MutateHistory(ctx context.Context, request *protocol.MutateHistoryRequest) (*protocol.HistoryResult, error) {
	if err := provider.ValidateMutateHistoryRequest(request); err != nil {
		return nil, err
	}
	table, err := historyTable(request.GetScope())
	if err != nil {
		return nil, err
	}
	s, err := p.lookup(request.GetScope().GetSessionId())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.guardLocks(ctx); err != nil {
		return nil, err
	}
	if s.rejectsChecksumReconciliation(request) {
		return historyResult(request.GetScope(), request.GetRequestId(), protocol.HistoryResult_REFUSED), nil
	}
	var outcome protocol.HistoryResult_Outcome
	if request.GetScope().GetFormat() == protocol.HistoryScope_ATLAS {
		outcome, err = s.mutateAtlasHistory(ctx, request, table)
	} else {
		outcome, err = s.mutateNativeHistory(ctx, request, table)
	}
	if err != nil {
		return nil, err
	}
	if s.fault == "lost-history" || s.fault == "lost-history-last" && request.GetReplacement().GetVersion() == "10" {
		return nil, fmt.Errorf("fixture lost history acknowledgment")
	}
	return historyResult(request.GetScope(), request.GetRequestId(), outcome), nil
}

// rejectsChecksumReconciliation isolates the injected late refusal from the
// storage implementation. Inserts and normal completion updates still succeed.
func (s *session) rejectsChecksumReconciliation(request *protocol.MutateHistoryRequest) bool {
	if s.fault != "reject-checksum-last" || request.GetExpected() == nil || request.GetReplacement() == nil || request.GetReplacement().GetVersion() != "4" || request.GetExpected().GetChecksum() == request.GetReplacement().GetChecksum() {
		return false
	}
	expected := proto.CloneOf(request.GetExpected())
	expected.Checksum = request.GetReplacement().GetChecksum()
	return proto.Equal(expected, request.GetReplacement())
}

func historyMutation(table string, request *protocol.MutateHistoryRequest) (string, []any, error) {
	codec := proto.MarshalOptions{Deterministic: true}
	expected, err := codec.Marshal(request.GetExpected())
	if err != nil {
		return "", nil, err
	}
	replacement, err := codec.Marshal(request.GetReplacement())
	if err != nil {
		return "", nil, err
	}
	expected = append([]byte(historyEncoding), expected...)
	replacement = append([]byte(historyEncoding), replacement...)
	table = "main." + table
	switch {
	case request.GetExpected() == nil:
		return "INSERT INTO " + table + " (version, revision) VALUES (?, ?) ON CONFLICT(version) DO NOTHING", []any{request.GetReplacement().GetVersion(), replacement}, nil
	case request.GetReplacement() == nil:
		return "DELETE FROM " + table + " WHERE version = ? AND revision = ?", []any{request.GetExpected().GetVersion(), expected}, nil
	default:
		return "UPDATE " + table + " SET revision = ? WHERE version = ? AND revision = ?", []any{replacement, request.GetExpected().GetVersion(), expected}, nil
	}
}

func readNativeHistory(ctx context.Context, runner historyRunner, table string, limit uint32) ([]*protocol.HistoryRevision, error) {
	var result []*protocol.HistoryRevision
	rows, err := runner.QueryContext(ctx, "SELECT version, revision FROM main."+table+" ORDER BY version LIMIT ?", limit+1)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var key string
		var payload []byte
		if err := rows.Scan(&key, &payload); err != nil {
			return nil, err
		}
		row := &protocol.HistoryRevision{}
		if !bytes.HasPrefix(payload, []byte(historyEncoding)) {
			return nil, fmt.Errorf("fixture history has an incompatible encoding")
		}
		if err := proto.Unmarshal(payload[len(historyEncoding):], row); err != nil {
			return nil, err
		}
		if row.GetVersion() != key {
			return nil, fmt.Errorf("fixture history row changed its identity")
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *session) mutateNativeHistory(ctx context.Context, request *protocol.MutateHistoryRequest, table string) (protocol.HistoryResult_Outcome, error) {
	runner, err := s.historyRunner(request.GetScope().GetTransactionId())
	if err != nil {
		return protocol.HistoryResult_UNKNOWN, err
	}
	exists, err := checkHistoryTable(ctx, runner, table, request.GetScope().GetTable(), request.GetScope().GetFormat())
	if err != nil {
		return protocol.HistoryResult_UNKNOWN, err
	}
	if !exists {
		return protocol.HistoryResult_REFUSED, nil
	}
	query, args, err := historyMutation(table, request)
	if err != nil {
		return protocol.HistoryResult_UNKNOWN, err
	}
	result, err := runner.ExecContext(ctx, query, args...)
	if err != nil {
		return protocol.HistoryResult_UNKNOWN, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return protocol.HistoryResult_UNKNOWN, err
	}
	outcome := protocol.HistoryResult_CONFLICT
	if changed == 1 {
		outcome = protocol.HistoryResult_APPLIED
	}
	if changed < 0 || changed > 1 {
		return protocol.HistoryResult_UNKNOWN, fmt.Errorf("fixture history mutation changed an unexpected number of rows")
	}
	return outcome, nil
}
