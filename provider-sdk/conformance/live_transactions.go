package conformance

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// begin begins a transaction and checks the reply as the SDK's client does.
func (r *run) begin(ctx context.Context, c *conn, session *protocol.Session, id string, isolation protocol.TransactionIsolation) error {
	request := &protocol.BeginTransactionRequest{SessionId: session.GetId(), TransactionId: id, Isolation: isolation}
	transaction, err := invoke(ctx, r, c, "BeginTransaction", c.client.BeginTransaction, request)
	if err != nil {
		return err
	}
	if err := provider.ValidateTransaction(request, transaction); err != nil {
		return fmt.Errorf("the reply fails the SDK's validator: %w", err)
	}
	return nil
}

// finish finishes a transaction and returns its result, observed for its
// reason and message.
func (r *run) finish(ctx context.Context, c *conn, session *protocol.Session, id string, action protocol.FinishTransactionRequest_Action) (*protocol.TransactionResult, error) {
	result, err := invoke(ctx, r, c, "FinishTransaction", c.client.FinishTransaction,
		&protocol.FinishTransactionRequest{SessionId: session.GetId(), TransactionId: id, Action: action})
	if err != nil {
		return nil, err
	}
	r.observeResultReason(c, "FinishTransaction", result.GetDiagnostic())
	r.observeText("the message of a transaction result", result.GetMessage())
	return result, nil
}

// liveTransactions begins and finishes transactions on one session.
func (r *run) liveTransactions(ctx context.Context) {
	if !provider.EngineOffers(r.full, r.engine, provider.TransactionsFeature) {
		r.add(checkTransactions, NotApplicable, "the engine does not offer sessions.transactions")
		return
	}
	c := r.all
	session, err := r.openSession(ctx, c, r.config.Live.Address)
	if err != nil {
		r.add(checkTransactions, Failed, err.Error())
		return
	}
	failures := r.transactionSequence(ctx, c, session)
	failures = append(failures, r.closedTransaction(ctx, c, session)...)
	r.result(checkTransactions, failures)
}

func (r *run) transactionSequence(ctx context.Context, c *conn, session *protocol.Session) []string {
	var failures []string
	rollback, commit := protocol.FinishTransactionRequest_ROLLBACK, protocol.FinishTransactionRequest_COMMIT
	if err := r.begin(ctx, c, session, "t1", protocol.TransactionIsolation_DEFAULT); err != nil {
		return []string{"a transaction of the default isolation did not begin: " + errorText(err)}
	}
	first, err := r.finish(ctx, c, session, "t1", rollback)
	switch {
	case err != nil:
		failures = append(failures, "rolling back a transaction failed: "+errorText(err))
	case provider.CheckTransactionResult(rollback, first) != nil:
		failures = append(failures, "rolling back a transaction answered "+first.GetOutcome().String())
	default:
		again, err := r.finish(ctx, c, session, "t1", rollback)
		if err != nil || !proto.Equal(again, first) {
			failures = append(failures, "a repeated rollback did not return the stored result")
		}
	}
	if err := r.begin(ctx, c, session, "t2", protocol.TransactionIsolation_DEFAULT); err != nil {
		failures = append(failures, "a second transaction did not begin: "+errorText(err))
	} else if committed, err := r.finish(ctx, c, session, "t2", commit); err != nil || provider.CheckTransactionResult(commit, committed) != nil {
		failures = append(failures, "committing an empty transaction did not answer COMMITTED")
	}
	fenced, err := r.finish(ctx, c, session, "t3", rollback)
	switch {
	case err != nil:
		failures = append(failures, "rolling back an unused transaction ID failed: "+errorText(err))
	case provider.CheckTransactionResult(rollback, fenced) != nil:
		failures = append(failures, "rolling back an unused transaction ID answered "+fenced.GetOutcome().String())
	case r.begin(ctx, c, session, "t3", protocol.TransactionIsolation_DEFAULT) == nil:
		failures = append(failures, "a transaction began with an ID a rollback fenced")
	}
	if r.begin(ctx, c, session, "t4", protocol.TransactionIsolation_SERIALIZABLE) == nil {
		if result, err := r.finish(ctx, c, session, "t4", rollback); err != nil || provider.CheckTransactionResult(rollback, result) != nil {
			failures = append(failures, "rolling back a serializable transaction failed")
		}
	}
	return failures
}

// closedTransaction closes a session with a transaction open. The close
// rolls the transaction back, and the session begins no other.
func (r *run) closedTransaction(ctx context.Context, c *conn, session *protocol.Session) []string {
	if err := r.begin(ctx, c, session, "t5", protocol.TransactionIsolation_DEFAULT); err != nil {
		_ = r.closeSession(ctx, c, session.GetId())
		return []string{"a transaction did not begin before the session closed: " + errorText(err)}
	}
	var failures []string
	if err := r.closeSession(ctx, c, session.GetId()); err != nil {
		failures = append(failures, "closing a session with an open transaction failed: "+errorText(err))
	}
	if r.begin(ctx, c, session, "t6", protocol.TransactionIsolation_DEFAULT) == nil {
		failures = append(failures, "a closed session began a transaction")
	}
	if result, err := r.finish(ctx, c, session, "t5", protocol.FinishTransactionRequest_COMMIT); err == nil && result.GetOutcome() == protocol.TransactionResult_COMMITTED {
		failures = append(failures, "a transaction of a closed session committed")
	}
	return failures
}
