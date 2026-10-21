package providerhost

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"slices"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/dbschema"
)

type historyStore struct {
	session     *session
	transaction *transaction
}

func (s *session) HistoryStore() (dbschema.HistoryStore, error) {
	if !slices.Contains(s.target.engine.GetFeatures(), provider.HistoryFeature) || !slices.Contains(s.target.host.description.GetFeatures(), provider.HistoryFeature) {
		return nil, unsupported("history for the selected engine")
	}
	return historyStore{session: s}, nil
}
func (t *transaction) HistoryStore() (dbschema.HistoryStore, error) {
	if _, err := t.session.HistoryStore(); err != nil {
		return nil, err
	}
	return historyStore{session: t.session, transaction: t}, nil
}
func (h historyStore) scope(table dbschema.HistoryTable) (*protocol.HistoryScope, error) {
	format := protocol.HistoryScope_FORMAT_UNSPECIFIED
	switch table.Format {
	case dbschema.NativeHistory:
		format = protocol.HistoryScope_NATIVE
	case dbschema.AtlasHistory:
		format = protocol.HistoryScope_ATLAS
	}
	result := &protocol.HistoryScope{SessionId: h.session.remote.GetId(), Schema: table.Schema, Table: table.Name, Format: format}
	if h.transaction != nil {
		result.TransactionId = h.transaction.id
	}
	return result, provider.ValidateHistoryScope(result)
}

// Holding the transaction mutex keeps a history operation ordered with SQL and
// finish. A dispatched history failure poisons commit just like failed execution;
// silently committing schema after losing its revision write is not permitted.
func (h historyStore) use(ctx context.Context, use func() error) error {
	if h.transaction != nil {
		h.transaction.mu.Lock()
		defer h.transaction.mu.Unlock()
		if h.transaction.commitTried || h.transaction.rollbackTried {
			return fmt.Errorf("provider transaction has finished execution")
		}
		if h.transaction.executionErr != nil {
			return h.transaction.executionErr
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if h.session.closed.Load() {
		return fmt.Errorf("provider session is closed")
	}
	err := nativeOutcomeError(use())
	if h.transaction != nil && err != nil {
		h.transaction.executionErr = err
	}
	return err
}

func (h historyStore) Read(ctx context.Context, table dbschema.HistoryTable) (dbschema.HistorySnapshot, error) {
	scope, err := h.scope(table)
	if err != nil {
		return dbschema.HistorySnapshot{}, err
	}
	var snapshot dbschema.HistorySnapshot
	err = h.use(ctx, func() error {
		result, err := h.session.target.host.client.ReadHistory(ctx, &protocol.ReadHistoryRequest{Scope: scope, MaxRevisions: provider.MaxHistoryRevisions})
		if err != nil {
			return err
		}
		snapshot.Exists = result.GetExists()
		snapshot.Records = make([]dbschema.HistoryRecord, 0, len(result.GetRevisions()))
		for _, row := range result.GetRevisions() {
			record, err := historyRecord(row)
			if err != nil {
				return err
			}
			snapshot.Records = append(snapshot.Records, record)
		}
		return nil
	})
	if err != nil {
		return dbschema.HistorySnapshot{}, err
	}
	return snapshot, nil
}
func (h historyStore) Prepare(ctx context.Context, table dbschema.HistoryTable) error {
	if h.session.IsDryRun() {
		return unsupported("history preparation during a dry run")
	}
	scope, err := h.scope(table)
	if err != nil {
		return err
	}
	request := &protocol.PrepareHistoryRequest{Scope: scope, RequestId: rand.Text()}
	if err := provider.ValidatePrepareHistoryRequest(request); err != nil {
		return err
	}
	return h.use(ctx, func() error {
		_, err := h.session.target.host.client.PrepareHistory(ctx, request)
		return err
	})
}
func (h historyStore) CompareAndSwap(ctx context.Context, table dbschema.HistoryTable, expected, replacement *dbschema.HistoryRecord) error {
	if h.session.IsDryRun() {
		return unsupported("history writes during a dry run")
	}
	scope, err := h.scope(table)
	if err != nil {
		return err
	}
	before, err := historyWireRecord(table.Format, expected)
	if err != nil {
		return err
	}
	after, err := historyWireRecord(table.Format, replacement)
	if err != nil {
		return err
	}
	request := &protocol.MutateHistoryRequest{Scope: scope, RequestId: rand.Text(), Expected: before, Replacement: after}
	if err := provider.ValidateMutateHistoryRequest(request); err != nil {
		return err
	}
	return h.use(ctx, func() error {
		_, err := h.session.target.host.client.MutateHistory(ctx, request)
		return err
	})
}
func historyRecord(row *protocol.HistoryRevision) (dbschema.HistoryRecord, error) {
	applied, total := row.GetApplied(), row.GetTotal()
	if applied > math.MaxInt || total > math.MaxInt {
		return dbschema.HistoryRecord{}, fmt.Errorf("provider history progress exceeds the host integer range")
	}
	states := map[protocol.HistoryRevision_State]string{protocol.HistoryRevision_PENDING: "pending", protocol.HistoryRevision_APPLIED: "applied", protocol.HistoryRevision_FAILED: "failed"}
	directions := map[protocol.HistoryRevision_Direction]string{protocol.HistoryRevision_UP: "up", protocol.HistoryRevision_DOWN: "down"}
	return dbschema.HistoryRecord{
		Version: row.GetVersion(), Description: row.GetDescription(), State: states[row.GetState()], Direction: directions[row.GetDirection()],
		Applied: int(applied), Total: int(total), Error: row.GetError(), ErrorStatement: row.GetErrorStatement(), ExecutionTime: time.Duration(row.GetExecutionNanoseconds()),
		Checksum: row.GetChecksum(), AppliedAt: time.Unix(row.GetAppliedAtSeconds(), int64(row.GetAppliedAtNanos())).UTC(),
		OperatorVersion: row.GetOperatorVersion(), PartialHashes: slices.Clone(row.GetPartialHashes()), AtlasType: row.GetAtlasType(),
	}, nil
}
func historyWireRecord(format dbschema.HistoryFormat, row *dbschema.HistoryRecord) (*protocol.HistoryRevision, error) {
	if row == nil {
		return nil, nil
	}
	if row.Applied < 0 || row.Total < 0 {
		return nil, fmt.Errorf("invalid negative history progress")
	}
	nanos := row.AppliedAt.Nanosecond()
	if nanos < 0 || nanos >= 1e9 {
		return nil, fmt.Errorf("invalid history timestamp nanoseconds")
	}
	states := map[string]protocol.HistoryRevision_State{"pending": protocol.HistoryRevision_PENDING, "applied": protocol.HistoryRevision_APPLIED, "failed": protocol.HistoryRevision_FAILED}
	directions := map[string]protocol.HistoryRevision_Direction{"up": protocol.HistoryRevision_UP, "down": protocol.HistoryRevision_DOWN}
	result := &protocol.HistoryRevision{Version: row.Version, Description: row.Description, State: states[row.State], Direction: directions[row.Direction], Applied: uint64(row.Applied), Total: uint64(row.Total),
		Error: row.Error, ErrorStatement: row.ErrorStatement, ExecutionNanoseconds: int64(row.ExecutionTime), Checksum: row.Checksum, AppliedAtSeconds: row.AppliedAt.Unix(), AppliedAtNanos: int32(nanos),
		OperatorVersion: row.OperatorVersion, PartialHashes: slices.Clone(row.PartialHashes), AtlasType: row.AtlasType}
	wireFormat := protocol.HistoryScope_NATIVE
	if format == dbschema.AtlasHistory {
		wireFormat = protocol.HistoryScope_ATLAS
	}
	if err := provider.ValidateHistoryRevision(wireFormat, result); err != nil {
		return nil, err
	}
	return result, nil
}
