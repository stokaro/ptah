package providerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/renderer"
	"ptah.run/dbschema"
)

var (
	_ dbschema.UnitExecutor = (*session)(nil)
	_ dbschema.UnitExecutor = (*transaction)(nil)
	_ dbschema.UnitExecutor = dryTransaction{}
)

func (s *session) ValidateUnits(ctx context.Context, units []renderer.Fragment) error {
	_, err := s.prepareUnits(ctx, units)
	return err
}

func (s *session) ExecuteUnits(ctx context.Context, units []renderer.Fragment) error {
	prepared, err := s.prepareUnits(ctx, units)
	if err != nil {
		return err
	}
	if s.IsDryRun() || len(prepared) == 0 {
		return nil
	}
	return s.dispatchUnits(ctx, "", prepared)
}

// prepareUnits resolves every local operation before the first execution RPC.
// Model signatures include executable and semantic identity. No SQL prefix is
// sent when a later native value belongs to a different frozen selection.
func (s *session) prepareUnits(ctx context.Context, units []renderer.Fragment) ([]*protocol.Unit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, fmt.Errorf("provider session is closed")
	}
	if err := s.lockError(); err != nil {
		return nil, err
	}
	prepared := make([]*protocol.Unit, len(units))
	for i, unit := range units {
		fragment, err := unit.Clone()
		if err != nil {
			return nil, err
		}
		wire := &protocol.Unit{ChangeId: strconv.Itoa(i)}
		if fragment.Native == nil {
			wire.Body = &protocol.Unit_Sql{Sql: fragment.SQL}
		} else {
			value, err := s.target.operationValue(fragment.Native)
			if err != nil {
				return nil, err
			}
			wire.Body = &protocol.Unit_Native{Native: proto.CloneOf(value.normalized.GetValue())}
		}
		if err := provider.ValidateUnit(wire); err != nil {
			return nil, err
		}
		prepared[i] = wire
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return prepared, nil
}

func (s *session) dispatchUnits(ctx context.Context, transactionID string, units []*protocol.Unit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed.Load() {
		return fmt.Errorf("provider session is closed")
	}
	if err := s.lockError(); err != nil {
		return err
	}
	_, err := s.target.host.client.Execute(ctx, &protocol.ExecuteRequest{
		SessionId: s.remote.GetId(), RequestId: rand.Text(), TransactionId: transactionID, Units: units,
	})
	if err == nil {
		return nil
	}
	if outcome, ok := errors.AsType[*provider.ExecutionError](err); ok {
		return &dbschema.UnitExecutionError{Err: nativeOutcomeError(err), Applied: int(outcome.Applied), Total: len(units)}
	}
	return nativeOutcomeError(err)
}

func (t dryTransaction) ValidateUnits(ctx context.Context, units []renderer.Fragment) error {
	return t.session.ValidateUnits(ctx, units)
}
func (t dryTransaction) ExecuteUnits(ctx context.Context, units []renderer.Fragment) error {
	return t.ValidateUnits(ctx, units)
}
