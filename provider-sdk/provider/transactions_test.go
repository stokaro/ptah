package provider_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func TestTransactionResultRequiresConfirmedRequestedEffect(t *testing.T) {
	for _, test := range []struct {
		name   string
		action protocol.FinishTransactionRequest_Action
		result *protocol.TransactionResult
		want   error
	}{
		{"commit", protocol.FinishTransactionRequest_COMMIT, &protocol.TransactionResult{Outcome: protocol.TransactionResult_COMMITTED}, nil},
		{"rollback", protocol.FinishTransactionRequest_ROLLBACK, &protocol.TransactionResult{Outcome: protocol.TransactionResult_ROLLED_BACK}, nil},
		{"fenced rollback", protocol.FinishTransactionRequest_ROLLBACK, &protocol.TransactionResult{Outcome: protocol.TransactionResult_NOT_STARTED}, nil},
		{"commit refused", protocol.FinishTransactionRequest_COMMIT, &protocol.TransactionResult{Outcome: protocol.TransactionResult_NOT_STARTED}, provider.ErrExecution},
		{"commit rolled back", protocol.FinishTransactionRequest_COMMIT, &protocol.TransactionResult{Outcome: protocol.TransactionResult_ROLLED_BACK}, provider.ErrExecution},
		{"rollback committed", protocol.FinishTransactionRequest_ROLLBACK, &protocol.TransactionResult{Outcome: protocol.TransactionResult_COMMITTED}, provider.ErrExecution},
		{"unknown", protocol.FinishTransactionRequest_COMMIT, &protocol.TransactionResult{}, provider.ErrOutcomeUnknown},
		{"missing", protocol.FinishTransactionRequest_ROLLBACK, nil, provider.ErrOutcomeUnknown},
		{"invalid outcome", protocol.FinishTransactionRequest_COMMIT, &protocol.TransactionResult{Outcome: 99}, provider.ErrOutcomeUnknown},
		{"invalid action", 99, &protocol.TransactionResult{Outcome: protocol.TransactionResult_COMMITTED}, provider.ErrProtocol},
		{"missing action", protocol.FinishTransactionRequest_ACTION_UNSPECIFIED, &protocol.TransactionResult{Outcome: protocol.TransactionResult_COMMITTED}, provider.ErrProtocol},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.CheckTransactionResult(test.action, test.result), qt.ErrorIs, test.want)
		})
	}
}

func TestTransactionHandleBindsRequestedSessionAndIdentity(t *testing.T) {
	request := &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: "session", TransactionId: "transaction"}
	for _, test := range []struct {
		name    string
		request *protocol.BeginTransactionRequest
		result  *protocol.Transaction
		want    error
	}{
		{"exact", request, &protocol.Transaction{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: "session", Id: "transaction"}, nil},
		{"wrong session", request, &protocol.Transaction{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: "other", Id: "transaction"}, provider.ErrProtocol},
		{"wrong handle", request, &protocol.Transaction{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: "session", Id: "other"}, provider.ErrProtocol},
		{"missing result", request, nil, provider.ErrProtocol},
		{"missing request", nil, nil, provider.ErrProtocol},
		{"missing session", &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, TransactionId: "transaction"}, nil, provider.ErrProtocol},
		{"missing handle", &protocol.BeginTransactionRequest{Isolation: protocol.TransactionIsolation_DEFAULT, SessionId: "session"}, nil, provider.ErrProtocol},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateTransaction(test.request, test.result), qt.ErrorIs, test.want)
		})
	}
}

func TestTransactionErrorRetainsStateWithoutProviderMessage(t *testing.T) {
	c := qt.New(t)
	err := provider.CheckTransactionResult(protocol.FinishTransactionRequest_ROLLBACK, &protocol.TransactionResult{Outcome: protocol.TransactionResult_COMMITTED, Message: "password=secret"})
	terminal, ok := errors.AsType[*provider.TransactionError](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(terminal.Outcome, qt.Equals, protocol.TransactionResult_COMMITTED)
	c.Assert(err.Error(), qt.Equals, "provider transaction outcome is COMMITTED")
	malformed := provider.CheckTransactionResult(protocol.FinishTransactionRequest_COMMIT, &protocol.TransactionResult{Outcome: 99})
	c.Assert(malformed, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(malformed, qt.ErrorIs, provider.ErrOutcomeUnknown)
}

func TestTransactionAdvertisementRequiresExecutionAndEngineBinding(t *testing.T) {
	c := qt.New(t)
	desc := description()
	desc.Engines[0].Features = []string{provider.TransactionsFeature}
	c.Assert(provider.ValidateDescription(desc, nil), qt.ErrorIs, provider.ErrProtocol)
	desc.Features = append(desc.Features, provider.TransactionsFeature)
	c.Assert(provider.ValidateDescription(desc, nil), qt.ErrorIs, provider.ErrProtocol)
	desc.Features = append(desc.Features, provider.ExecuteFeature)
	c.Assert(provider.ValidateDescription(desc, []string{provider.TransactionsFeature}), qt.IsNil)
}

func TestTransactionIsolationIsExplicitAndBoundToReply(t *testing.T) {
	for _, test := range []struct {
		name      string
		requested protocol.TransactionIsolation
		returned  protocol.TransactionIsolation
		want      error
	}{
		{"default", protocol.TransactionIsolation_DEFAULT, protocol.TransactionIsolation_DEFAULT, nil},
		{"serializable", protocol.TransactionIsolation_SERIALIZABLE, protocol.TransactionIsolation_SERIALIZABLE, nil},
		{"missing request", 0, protocol.TransactionIsolation_DEFAULT, provider.ErrProtocol},
		{"unknown request", 99, 99, provider.ErrProtocol},
		{"missing reply", protocol.TransactionIsolation_DEFAULT, 0, provider.ErrProtocol},
		{"unknown reply", protocol.TransactionIsolation_DEFAULT, 99, provider.ErrProtocol},
		{"downgrade", protocol.TransactionIsolation_SERIALIZABLE, protocol.TransactionIsolation_DEFAULT, provider.ErrProtocol},
		{"substitution", protocol.TransactionIsolation_DEFAULT, protocol.TransactionIsolation_SERIALIZABLE, provider.ErrProtocol},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := &protocol.BeginTransactionRequest{SessionId: "session", TransactionId: "transaction", Isolation: test.requested}
			result := &protocol.Transaction{SessionId: "session", Id: "transaction", Isolation: test.returned}
			c.Assert(provider.ValidateTransaction(request, result), qt.ErrorIs, test.want)
		})
	}
}
