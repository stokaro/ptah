package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func historyScope() *protocol.HistoryScope {
	return &protocol.HistoryScope{SessionId: "session", Schema: "main", Table: "history", Format: protocol.HistoryScope_NATIVE}
}
func historyRevision() *protocol.HistoryRevision {
	return &protocol.HistoryRevision{Version: "1", State: protocol.HistoryRevision_PENDING, Direction: protocol.HistoryRevision_UP, Total: 2}
}
func TestHistorySnapshotRequiresCompleteExactScope(t *testing.T) {
	request := &protocol.ReadHistoryRequest{Scope: historyScope(), MaxRevisions: 1}
	other := historyScope()
	other.TransactionId = "other"
	for _, test := range []struct {
		name   string
		result *protocol.HistorySnapshot
		want   error
	}{
		{"missing", nil, provider.ErrProtocol},
		{"transaction changed", &protocol.HistorySnapshot{Scope: other, Exists: true, Complete: true}, provider.ErrProtocol},
		{"incomplete", &protocol.HistorySnapshot{Scope: historyScope(), Exists: true}, provider.ErrIncomplete},
		{"truncated limit", &protocol.HistorySnapshot{Scope: historyScope(), Exists: true, Complete: true, Revisions: []*protocol.HistoryRevision{historyRevision(), historyRevision()}}, provider.ErrIncomplete},
		{"missing store with rows", &protocol.HistorySnapshot{Scope: historyScope(), Complete: true, Revisions: []*protocol.HistoryRevision{historyRevision()}}, provider.ErrProtocol},
		{"nil row", &protocol.HistorySnapshot{Scope: historyScope(), Exists: true, Complete: true, Revisions: []*protocol.HistoryRevision{nil}}, provider.ErrProtocol},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateHistorySnapshot(request, test.result), qt.ErrorIs, test.want)
		})
	}
}
func TestHistorySnapshotPreservesEmptyAndExactAtlasIdentities(t *testing.T) {
	c := qt.New(t)
	scope := historyScope()
	scope.Format = protocol.HistoryScope_ATLAS
	empty := &protocol.HistoryRevision{Version: ""}
	padded := &protocol.HistoryRevision{Version: "001"}
	plain := &protocol.HistoryRevision{Version: "1"}
	request := &protocol.ReadHistoryRequest{Scope: scope, MaxRevisions: 10}
	result := &protocol.HistorySnapshot{Scope: proto.CloneOf(scope), Exists: true, Complete: true, Revisions: []*protocol.HistoryRevision{empty, padded, plain}}
	c.Assert(provider.ValidateHistorySnapshot(request, result), qt.IsNil)
	result.Revisions = append(result.Revisions, proto.CloneOf(empty))
	c.Assert(provider.ValidateHistorySnapshot(request, result), qt.ErrorIs, provider.ErrProtocol)
}
func TestHistoryMutationRefusesMissingIdentityAndRenames(t *testing.T) {
	renamed := historyRevision()
	renamed.Version = "2"
	noncanonical := historyRevision()
	noncanonical.Version = "01"
	for _, test := range []struct {
		name    string
		request *protocol.MutateHistoryRequest
	}{
		{"missing", nil},
		{"no row", &protocol.MutateHistoryRequest{Scope: historyScope(), RequestId: "write"}},
		{"no request identity", &protocol.MutateHistoryRequest{Scope: historyScope(), Replacement: historyRevision()}},
		{"rename", &protocol.MutateHistoryRequest{Scope: historyScope(), RequestId: "write", Expected: historyRevision(), Replacement: renamed}},
		{"numeric spelling", &protocol.MutateHistoryRequest{Scope: historyScope(), RequestId: "write", Replacement: noncanonical}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateMutateHistoryRequest(test.request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
func TestHistoryMutationAcceptsInsertReplaceDelete(t *testing.T) {
	changed := historyRevision()
	changed.Applied = 1
	for _, test := range []struct {
		name                  string
		expected, replacement *protocol.HistoryRevision
	}{
		{"insert", nil, historyRevision()}, {"replace", historyRevision(), changed}, {"delete", historyRevision(), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateMutateHistoryRequest(&protocol.MutateHistoryRequest{Scope: historyScope(), RequestId: "write", Expected: test.expected, Replacement: test.replacement}), qt.IsNil)
		})
	}
}
func TestHistoryWriteReceiptRequiresExactBinding(t *testing.T) {
	for _, test := range []struct {
		name    string
		scope   *protocol.HistoryScope
		id      string
		outcome protocol.HistoryResult_Outcome
		want    error
	}{
		{"missing scope", nil, "write", protocol.HistoryResult_APPLIED, provider.ErrOutcomeUnknown},
		{"different request", historyScope(), "other", protocol.HistoryResult_APPLIED, provider.ErrOutcomeUnknown},
		{"unknown", historyScope(), "write", protocol.HistoryResult_UNKNOWN, provider.ErrOutcomeUnknown},
		{"unknown enum", historyScope(), "write", 99, provider.ErrOutcomeUnknown},
		{"conflict", historyScope(), "write", protocol.HistoryResult_CONFLICT, provider.ErrExecution},
		{"refused", historyScope(), "write", protocol.HistoryResult_REFUSED, provider.ErrExecution},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := &protocol.HistoryResult{Scope: test.scope, RequestId: test.id, Outcome: test.outcome, Message: "password=secret"}
			err := provider.CheckHistoryResult(historyScope(), "write", result)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(err.Error(), qt.Not(qt.Contains), "secret")
		})
	}
}
func TestHistoryRequiresExplicitBoundsAndPreparationOutsideTransactions(t *testing.T) {
	c := qt.New(t)
	for _, limit := range []uint32{0, provider.MaxHistoryRevisions + 1} {
		c.Assert(provider.ValidateReadHistoryRequest(&protocol.ReadHistoryRequest{Scope: historyScope(), MaxRevisions: limit}), qt.ErrorIs, provider.ErrProtocol)
	}
	scope := historyScope()
	scope.TransactionId = "transaction"
	c.Assert(provider.ValidatePrepareHistoryRequest(&protocol.PrepareHistoryRequest{Scope: scope, RequestId: "prepare"}), qt.ErrorIs, provider.ErrProtocol)
}
func TestHistoryAdvertisementRequiresGlobalService(t *testing.T) {
	c := qt.New(t)
	desc := description()
	desc.Engines[0].Features = append(desc.Engines[0].Features, provider.HistoryFeature)
	c.Assert(provider.ValidateDescription(desc, nil), qt.ErrorIs, provider.ErrProtocol)
	desc.Features = append(desc.Features, provider.HistoryFeature)
	c.Assert(provider.ValidateDescription(desc, []string{provider.HistoryFeature}), qt.IsNil)
}

func TestHistoryStateBelongsToTheStoredRepresentation(t *testing.T) {
	for _, test := range []struct {
		name   string
		format protocol.HistoryScope_Format
		row    *protocol.HistoryRevision
		want   error
	}{
		{"native requires state", protocol.HistoryScope_NATIVE, &protocol.HistoryRevision{Version: "1", Direction: protocol.HistoryRevision_UP}, provider.ErrProtocol},
		{"native requires direction", protocol.HistoryScope_NATIVE, &protocol.HistoryRevision{Version: "1", State: protocol.HistoryRevision_APPLIED}, provider.ErrProtocol},
		{"native retains stored state", protocol.HistoryScope_NATIVE, historyRevision(), nil},
		{"Atlas carries raw facts", protocol.HistoryScope_ATLAS, &protocol.HistoryRevision{Version: "", Applied: 1, Total: 2, Error: "partial", OperatorVersion: "owner", AtlasType: 2}, nil},
		{"Atlas refuses derived state", protocol.HistoryScope_ATLAS, &protocol.HistoryRevision{State: protocol.HistoryRevision_FAILED}, provider.ErrProtocol},
		{"Atlas refuses derived direction", protocol.HistoryScope_ATLAS, &protocol.HistoryRevision{Direction: protocol.HistoryRevision_DOWN}, provider.ErrProtocol},
		{"Atlas refuses unknown state", protocol.HistoryScope_ATLAS, &protocol.HistoryRevision{State: 99}, provider.ErrProtocol},
		{"Atlas refuses invalid progress", protocol.HistoryScope_ATLAS, &protocol.HistoryRevision{Applied: 2, Total: 1}, provider.ErrProtocol},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateHistoryRevision(test.format, test.row), qt.ErrorIs, test.want)
		})
	}
}
