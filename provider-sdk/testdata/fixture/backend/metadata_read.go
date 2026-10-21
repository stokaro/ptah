package backend

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

type metadataSnapshot struct {
	scope                    *protocol.MetadataScope
	exists, closed, complete bool
	page                     uint64
	offset                   int
	logs                     []*protocol.MigrationLogRecord
	tags                     []*protocol.MigrationTagRecord
}

func (m *metadataSnapshot) close() { m.closed = true; m.logs = nil; m.tags = nil }
func metadataReadResult(request *protocol.MetadataReadRequest) *protocol.MetadataReadResult {
	return &protocol.MetadataReadResult{Scope: proto.CloneOf(request.Scope), ReaderId: request.ReaderId, RequestId: request.RequestId}
}
func (s *session) metadataReservation(request *protocol.MetadataReadRequest) (*metadataSnapshot, error) {
	if s.closed {
		return nil, fmt.Errorf("fixture session is closed")
	}
	if s.metadataReaders == nil {
		s.metadataReaders = make(map[string]*metadataSnapshot)
	}
	record := s.metadataReaders[request.ReaderId]
	if record != nil && !proto.Equal(record.scope, request.Scope) {
		return nil, fmt.Errorf("metadata reader scope changed")
	}
	return record, nil
}

// OpenMetadataRead materializes a consistent snapshot in one transaction. This
// fixture caps each materialized snapshot at 64 MiB and refuses larger stores;
// RPC pages remain bounded independently. Production owners may stream snapshots.
func (p *Provider) OpenMetadataRead(ctx context.Context, request *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error) {
	if err := provider.ValidateMetadataReadRequest(request); err != nil {
		return nil, err
	}
	s, err := p.lookup(request.Scope.SessionId)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, err := s.metadataReservation(request)
	if err != nil {
		return nil, err
	}
	if previous != nil {
		return nil, fmt.Errorf("metadata reader handle was already reserved")
	}
	record := &metadataSnapshot{scope: proto.CloneOf(request.Scope), closed: true}
	s.metadataReaders[request.ReaderId] = record
	runner, tx, err := s.metadataTransaction(ctx, request.Scope)
	if err != nil {
		return nil, err
	}
	defer rollbackMetadata(tx)
	record.exists, err = checkMetadataTable(ctx, runner, request.Scope)
	if err != nil {
		return nil, err
	}
	if record.exists {
		if err := record.load(ctx, runner); err != nil {
			record.close()
			return nil, err
		}
	}
	record.closed = false
	if s.fault == "lost-metadata-open" {
		return nil, fmt.Errorf("fixture lost metadata open receipt")
	}
	return metadataReadResult(request), nil
}
func (m *metadataSnapshot) load(ctx context.Context, runner historyRunner) error {
	table, err := metadataTable(m.scope)
	if err != nil {
		return err
	}
	query := "SELECT tag,version,recorded_at FROM main." + table + " ORDER BY tag"
	if m.scope.Kind == protocol.MetadataScope_LOG {
		query = "SELECT run_id,seq,operation,version,state,actor,actor_source,checksum,logged_at,error FROM main." + table + " ORDER BY run_id,seq"
	}
	rows, err := runner.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	size := 0
	for rows.Next() {
		if m.scope.Kind == protocol.MetadataScope_LOG {
			row, err := scanMetadataLog(rows)
			if err != nil {
				return err
			}
			m.logs = append(m.logs, row)
			size += proto.Size(row)
		} else {
			row := &protocol.MigrationTagRecord{}
			var at time.Time
			if err := rows.Scan(&row.Name, &row.Version, &at); err != nil {
				return err
			}
			nanos, err := metadataNanoseconds(at)
			if err != nil {
				return err
			}
			row.RecordedAtSeconds, row.RecordedAtNanos = at.Unix(), nanos
			if err := provider.ValidateMigrationTagRecord(row); err != nil {
				return err
			}
			m.tags = append(m.tags, row)
			size += proto.Size(row)
		}
		if size > 64<<20 {
			return fmt.Errorf("fixture metadata snapshot exceeds its memory bound")
		}
	}
	return rows.Err()
}
func scanMetadataLog(rows *sql.Rows) (*protocol.MigrationLogRecord, error) {
	row := &protocol.MigrationLogRecord{}
	var actor, checksum, failure sql.NullString
	var at time.Time
	if err := rows.Scan(&row.RunId, &row.Sequence, &row.Operation, &row.Version, &row.State, &actor, &row.ActorSource, &checksum, &at, &failure); err != nil {
		return nil, err
	}
	row.Actor, row.Checksum, row.Error = actor.String, checksum.String, failure.String
	nanos, err := metadataNanoseconds(at)
	if err != nil {
		return nil, err
	}
	row.AtSeconds, row.AtNanos = at.Unix(), nanos
	return row, provider.ValidateMigrationLogRecord(row)
}

// ReadMetadataPage advances the exact cursor once and refuses replayed indices.
func (p *Provider) ReadMetadataPage(ctx context.Context, request *protocol.ReadMetadataPageRequest) (*protocol.MetadataPage, error) {
	if err := provider.ValidateReadMetadataPageRequest(request); err != nil {
		return nil, err
	}
	s, err := p.lookup(request.Reader.Scope.SessionId)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	record, err := s.metadataReservation(request.Reader)
	if err != nil {
		return nil, err
	}
	if record == nil || record.closed || record.complete || request.Page != record.page {
		return nil, fmt.Errorf("metadata reader is not at the requested page")
	}
	if id := record.scope.TransactionId; id != "" {
		if _, err := s.historyRunner(id); err != nil {
			return nil, err
		}
	}
	page := &protocol.MetadataPage{Reader: metadataReadResult(request.Reader), Page: request.Page, Exists: record.exists}
	count := len(record.logs) + len(record.tags)
	end := record.offset
	for end < count && end-record.offset < int(request.MaxRecords) {
		if record.scope.Kind == protocol.MetadataScope_LOG {
			page.Logs = append(page.Logs, proto.CloneOf(record.logs[end]))
		} else {
			page.Tags = append(page.Tags, proto.CloneOf(record.tags[end]))
		}
		if proto.Size(page) > provider.MaxMetadataBytes {
			if end == record.offset {
				return nil, fmt.Errorf("metadata record exceeds the page byte bound")
			}
			if record.scope.Kind == protocol.MetadataScope_LOG {
				page.Logs = page.Logs[:len(page.Logs)-1]
			} else {
				page.Tags = page.Tags[:len(page.Tags)-1]
			}
			break
		}
		end++
	}
	page.Complete = end == count
	s.injectMetadataPageFault(request, record, page)
	if err := provider.ValidateMetadataPage(request, page); err != nil {
		return nil, err
	}
	record.offset, record.page, record.complete = end, record.page+1, page.Complete
	if s.fault == "lost-metadata-page" {
		return nil, fmt.Errorf("fixture lost metadata page reply")
	}
	return page, nil
}

func (s *session) injectMetadataPageFault(request *protocol.ReadMetadataPageRequest, record *metadataSnapshot, page *protocol.MetadataPage) {
	if request.Page > 0 && s.fault == "repeat-metadata-record" && len(page.Tags) != 0 {
		page.Tags[0] = proto.CloneOf(record.tags[0])
	}
	if request.Page > 0 && s.fault == "change-metadata-existence" {
		page.Exists, page.Complete, page.Tags, page.Logs = false, true, nil, nil
	}
}

// CloseMetadataRead releases a snapshot or fences its caller-selected ID before
// an open arrives. A mismatched scope cannot close another reader.
func (p *Provider) CloseMetadataRead(ctx context.Context, request *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error) {
	if err := provider.ValidateMetadataReadRequest(request); err != nil {
		return nil, err
	}
	s, err := p.lookup(request.Scope.SessionId)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	record, err := s.metadataReservation(request)
	if err != nil {
		return nil, err
	}
	if record == nil {
		record = &metadataSnapshot{scope: proto.CloneOf(request.Scope)}
		s.metadataReaders[request.ReaderId] = record
	}
	record.close()
	if s.fault == "lost-metadata-close" {
		return nil, fmt.Errorf("fixture lost metadata close receipt")
	}
	return metadataReadResult(request), nil
}

func (s *session) closeTransactionMetadata(id string) {
	for _, reader := range s.metadataReaders {
		if reader.scope.GetTransactionId() == id {
			reader.close()
		}
	}
}
