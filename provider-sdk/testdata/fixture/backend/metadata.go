package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// The fixture stores ordinary columns, so independent SQL readers can verify
// persisted facts. It accepts only this layout and refuses triggers.
func metadataDDL(table string, kind protocol.MetadataScope_Kind) string {
	if kind == protocol.MetadataScope_TAGS {
		return "CREATE TABLE " + table + " (tag VARCHAR(255) NOT NULL PRIMARY KEY, version BIGINT NOT NULL, recorded_at TIMESTAMP NOT NULL)"
	}
	return "CREATE TABLE " + table + " (run_id VARCHAR(64) NOT NULL, seq BIGINT NOT NULL, operation VARCHAR(32) NOT NULL, version BIGINT NOT NULL, state VARCHAR(32) NOT NULL, " +
		"actor VARCHAR(256) NULL, actor_source VARCHAR(32) NOT NULL, checksum VARCHAR(64) NULL, logged_at TIMESTAMP NOT NULL, error TEXT NULL, PRIMARY KEY (run_id, seq))"
}
func metadataTable(scope *protocol.MetadataScope) (string, error) {
	if scope.GetSchema() != "" && scope.GetSchema() != "main" {
		return "", fmt.Errorf("fixture metadata supports main only")
	}
	return quote(scope.GetTable()), nil
}
func checkMetadataTable(ctx context.Context, runner historyRunner, scope *protocol.MetadataScope) (bool, error) {
	table, err := metadataTable(scope)
	if err != nil {
		return false, err
	}
	var kind, definition string
	err = runner.QueryRowContext(ctx, "SELECT type, sql FROM main.sqlite_schema WHERE name = ? COLLATE NOCASE", scope.GetTable()).Scan(&kind, &definition)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if kind != "table" || definition != metadataDDL(table, scope.GetKind()) {
		return false, fmt.Errorf("fixture metadata storage has an incompatible layout")
	}
	var unsafe int
	query := "SELECT COUNT(*) FROM (SELECT type,tbl_name,sql FROM main.sqlite_schema UNION ALL SELECT type,tbl_name,sql FROM temp.sqlite_schema) WHERE tbl_name = ? COLLATE NOCASE AND (type = 'trigger' OR (type = 'index' AND sql IS NOT NULL))"
	if err := runner.QueryRowContext(ctx, query, scope.GetTable()).Scan(&unsafe); err != nil {
		return false, err
	}
	if unsafe != 0 {
		return false, fmt.Errorf("fixture metadata storage has unsupported triggers or indexes")
	}
	return true, nil
}

// metadataTransaction keeps layout validation and the operation in one data
// transaction. A separate session-owned pool lets durable log writes run outside
// execution transactions; SQLite may refuse concurrent writes, never reroute them.
func (s *session) metadataTransaction(ctx context.Context, scope *protocol.MetadataScope) (historyRunner, *sql.Tx, error) {
	if s.closed {
		return nil, nil, fmt.Errorf("fixture session is closed")
	}
	if scope.GetTransactionId() != "" {
		runner, err := s.historyRunner(scope.GetTransactionId())
		return runner, nil, err
	}
	db := s.db
	if scope.GetKind() == protocol.MetadataScope_LOG {
		if s.logDB == nil {
			var err error
			s.logDB, err = sql.Open("sqlite", s.path)
			if err != nil {
				return nil, nil, err
			}
			s.logDB.SetMaxOpenConns(1)
		}
		db = s.logDB
	} else if s.active != "" {
		return nil, nil, fmt.Errorf("fixture tags require the active transaction")
	}
	tx, err := db.BeginTx(ctx, nil)
	return tx, tx, err
}
func metadataResult(scope *protocol.MetadataScope, id string, outcome protocol.MetadataResult_Outcome) *protocol.MetadataResult {
	return &protocol.MetadataResult{Scope: proto.CloneOf(scope), RequestId: id, Outcome: outcome}
}
func finishMetadata(tx *sql.Tx) error {
	if tx != nil {
		return tx.Commit()
	}
	return nil
}
func rollbackMetadata(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}

// PrepareMetadata creates missing storage outside transactions or validates the
// exact existing store inside a selected execution transaction.
func (p *Provider) PrepareMetadata(ctx context.Context, request *protocol.PrepareMetadataRequest) (*protocol.MetadataResult, error) {
	if err := provider.ValidatePrepareMetadataRequest(request); err != nil {
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
	runner, tx, err := s.metadataTransaction(ctx, request.Scope)
	if err != nil {
		return nil, err
	}
	defer rollbackMetadata(tx)
	exists, err := checkMetadataTable(ctx, runner, request.Scope)
	if err != nil {
		return nil, err
	}
	if !exists {
		if request.Scope.TransactionId != "" {
			return metadataResult(request.Scope, request.RequestId, protocol.MetadataResult_REJECTED), nil
		}
		table, err := metadataTable(request.Scope)
		if err != nil {
			return nil, err
		}
		ddl := strings.Replace(metadataDDL(table, request.Scope.Kind), "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS main.", 1)
		if _, err := runner.ExecContext(ctx, ddl); err != nil {
			return nil, err
		}
	}
	exists, err = checkMetadataTable(ctx, runner, request.Scope)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("metadata store disappeared")
	}
	if err := finishMetadata(tx); err != nil {
		return nil, err
	}
	return metadataResult(request.Scope, request.RequestId, protocol.MetadataResult_APPLIED), nil
}

// MutateMetadata persists an exact log event or atomically moves/removes a tag.
// A lost acknowledgment preserves the mutation and does not trigger a replay.
func (p *Provider) MutateMetadata(ctx context.Context, request *protocol.MutateMetadataRequest) (*protocol.MetadataResult, error) {
	if err := provider.ValidateMutateMetadataRequest(request); err != nil {
		return nil, err
	}
	s, err := p.lookup(request.Scope.SessionId)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.guardLocks(ctx); err != nil {
		return nil, err
	}
	runner, tx, err := s.metadataTransaction(ctx, request.Scope)
	if err != nil {
		return nil, err
	}
	defer rollbackMetadata(tx)
	exists, err := checkMetadataTable(ctx, runner, request.Scope)
	if err != nil {
		return nil, err
	}
	result := metadataResult(request.Scope, request.RequestId, protocol.MetadataResult_APPLIED)
	if !exists {
		if request.GetDeleteTag() == "" {
			result.Outcome = protocol.MetadataResult_REJECTED
		}
		return result, nil
	}
	table, err := metadataTable(request.Scope)
	if err != nil {
		return nil, err
	}
	result.Deleted, err = mutateMetadata(ctx, runner, "main."+table, request)
	if err != nil {
		return nil, err
	}
	if err := finishMetadata(tx); err != nil {
		return nil, err
	}
	if s.fault == "lost-metadata-write" {
		return nil, fmt.Errorf("fixture lost metadata mutation reply")
	}
	return result, nil
}
func mutateMetadata(ctx context.Context, runner historyRunner, table string, request *protocol.MutateMetadataRequest) (bool, error) {
	switch mutation := request.Mutation.(type) {
	case *protocol.MutateMetadataRequest_AppendLog:
		row := mutation.AppendLog
		_, err := runner.ExecContext(ctx, "INSERT INTO "+table+" (run_id,seq,operation,version,state,actor,actor_source,checksum,logged_at,error) VALUES (?,?,?,?,?,?,?,?,?,?)",
			row.RunId, row.Sequence, row.Operation, row.Version, row.State, row.Actor, row.ActorSource, row.Checksum, time.Unix(row.AtSeconds, int64(row.AtNanos)).UTC().Format(time.RFC3339Nano), row.Error)
		return false, err
	case *protocol.MutateMetadataRequest_PutTag:
		row := mutation.PutTag
		_, err := runner.ExecContext(ctx, "INSERT INTO "+table+" (tag,version,recorded_at) VALUES (?,?,?) ON CONFLICT(tag) DO UPDATE SET version=excluded.version,recorded_at=excluded.recorded_at",
			row.Name, row.Version, time.Unix(row.RecordedAtSeconds, int64(row.RecordedAtNanos)).UTC().Format(time.RFC3339Nano))
		return false, err
	case *protocol.MutateMetadataRequest_DeleteTag:
		result, err := runner.ExecContext(ctx, "DELETE FROM "+table+" WHERE tag = ?", mutation.DeleteTag)
		if err != nil {
			return false, err
		}
		count, err := result.RowsAffected()
		return count > 0, err
	default:
		return false, fmt.Errorf("invalid metadata mutation")
	}
}

// LookupMetadataTag reads one label without creating or preparing storage.
func (p *Provider) LookupMetadataTag(ctx context.Context, request *protocol.LookupMetadataTagRequest) (*protocol.MetadataTagResult, error) {
	if err := provider.ValidateLookupMetadataTagRequest(request); err != nil {
		return nil, err
	}
	s, err := p.lookup(request.Scope.SessionId)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	runner, tx, err := s.metadataTransaction(ctx, request.Scope)
	if err != nil {
		return nil, err
	}
	defer rollbackMetadata(tx)
	exists, err := checkMetadataTable(ctx, runner, request.Scope)
	if err != nil {
		return nil, err
	}
	result := &protocol.MetadataTagResult{Scope: proto.CloneOf(request.Scope), RequestId: request.RequestId}
	if !exists {
		return result, nil
	}
	table, err := metadataTable(request.Scope)
	if err != nil {
		return nil, err
	}
	record := &protocol.MigrationTagRecord{}
	var at time.Time
	err = runner.QueryRowContext(ctx, "SELECT tag,version,recorded_at FROM main."+table+" WHERE tag = ?", request.Name).Scan(&record.Name, &record.Version, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	nanos, err := metadataNanoseconds(at)
	if err != nil {
		return nil, err
	}
	record.RecordedAtSeconds, record.RecordedAtNanos = at.Unix(), nanos
	result.Found, result.Record = true, record
	return result, provider.ValidateMetadataTagResult(request, result)
}

func metadataNanoseconds(at time.Time) (int32, error) {
	nanos := at.Nanosecond()
	if nanos < 0 || nanos >= 1_000_000_000 {
		return 0, fmt.Errorf("invalid metadata timestamp nanoseconds")
	}
	return int32(nanos), nil
}
