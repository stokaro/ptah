package providerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/statementtimeout"
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
	request := &protocol.ExecuteRequest{SessionId: s.remote.GetId(), RequestId: rand.Text(), TransactionId: transactionID, Units: units}
	// A migration's statement timeout reaches the provider with the units,
	// and the provider stops a unit that runs longer; see BoundsStatements.
	if timeout, bounded := statementtimeout.From(ctx); bounded {
		request.StatementTimeoutNanoseconds = timeout.Nanoseconds()
	}
	_, err := s.target.host.client.Execute(ctx, request)
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

// BoundsStatements reports whether the provider stops a unit at the statement
// timeout the host sends with it, so the migrator hands it the migration's
// statement timeout instead of a setting to run first.
func (s *session) BoundsStatements() bool { return s.target.boundsStatements() }

// boundsStatements reports whether the provider offers statement timeouts.
// The negotiated description keeps the feature only when the host lists it
// too.
func (t *target) boundsStatements() bool {
	return slices.Contains(t.host.description.GetFeatures(), provider.StatementTimeoutFeature)
}
