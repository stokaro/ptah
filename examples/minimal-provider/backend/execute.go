package backend

import (
	"context"
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/grpc/codes"
)

// Execute runs the units of one request in order and reports how many of them
// the database applied. It never runs a request again because of its ID: when
// the reply is lost, Ptah reads the database to learn what happened.
//
// Each unit is one SQLite statement in autocommit mode, so a statement the
// database refuses applied nothing, and the units before it stay applied. A
// canceled call may have stopped inside a statement, so its outcome is
// UNKNOWN.
func (p *Provider) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	current, err := p.lookup(request.GetSessionId())
	if err != nil {
		return nil, err
	}
	if request.GetTransactionId() != "" {
		return nil, runtime.ErrorWithReason(codes.InvalidArgument, "the tutorial engine offers no transactions")
	}
	current.mu.Lock()
	defer current.mu.Unlock()
	units := request.GetUnits()
	var applied uint32
	for _, unit := range units {
		statement, ok := unit.GetBody().(*protocol.Unit_Sql)
		if !ok {
			return nil, runtime.ErrorWithReason(codes.InvalidArgument, "the tutorial engine runs SQL units only")
		}
		if !current.guard(ctx) {
			return stoppedAt(applied, fmt.Sprintf("the session lost its lock before unit %d of %d", applied+1, len(units))), nil
		}
		if _, err := current.db.ExecContext(ctx, statement.Sql); err != nil {
			return failedAt(ctx, applied, len(units)), nil
		}
		applied++
	}
	return &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: applied}, nil
}

// failedAt reports a statement that failed after applied units of total. A
// canceled call may have stopped inside the statement, so nobody knows
// whether it applied.
func failedAt(ctx context.Context, applied uint32, total int) *protocol.Execution {
	if ctx.Err() != nil {
		return &protocol.Execution{
			Outcome:      protocol.Execution_UNKNOWN,
			AppliedCount: applied,
			Diagnostic:   &protocol.ProviderDiagnostic{Reason: fmt.Sprintf("the call ended while unit %d of %d was running", applied+1, total)},
		}
	}
	return stoppedAt(applied, fmt.Sprintf("the database refused unit %d of %d", applied+1, total))
}

// stoppedAt reports a batch that ran applied units and then stopped before
// the next one took effect: NOT_APPLIED when it applied none, PARTIAL when
// it applied some. The reason names the unit, never its text.
func stoppedAt(applied uint32, reason string) *protocol.Execution {
	outcome := protocol.Execution_PARTIAL
	if applied == 0 {
		outcome = protocol.Execution_NOT_APPLIED
	}
	return &protocol.Execution{
		Outcome:      outcome,
		AppliedCount: applied,
		Diagnostic:   &protocol.ProviderDiagnostic{Reason: reason},
	}
}
