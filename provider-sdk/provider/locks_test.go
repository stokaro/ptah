package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func lockRequest() *protocol.LockRequest {
	return &protocol.LockRequest{Scope: &protocol.LockScope{SessionId: "session", LockId: "attempt", Name: "migration"}, RequestId: "request"}
}

func TestLockRequestRequiresCompleteExactIdentity(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*protocol.LockRequest)
	}{
		{"missing scope", func(r *protocol.LockRequest) { r.Scope = nil }},
		{"missing session", func(r *protocol.LockRequest) { r.Scope.SessionId = "" }},
		{"missing handle", func(r *protocol.LockRequest) { r.Scope.LockId = "" }},
		{"missing name", func(r *protocol.LockRequest) { r.Scope.Name = "" }},
		{"missing request", func(r *protocol.LockRequest) { r.RequestId = "" }},
		{"surrounding whitespace", func(r *protocol.LockRequest) { r.Scope.Name = " migration" }},
		{"NUL", func(r *protocol.LockRequest) { r.Scope.Name = "migration\x00" }},
		{"invalid UTF-8", func(r *protocol.LockRequest) { r.Scope.Name = string([]byte{0xff}) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := lockRequest()
			c.Assert(provider.ValidateLockRequest(request), qt.IsNil)
			test.edit(request)
			c.Assert(provider.ValidateLockRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	c.Assert(provider.ValidateLockRequest(nil), qt.ErrorIs, provider.ErrProtocol)
}

func TestLockReceiptBindsEveryIdentity(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*protocol.LockResult)
	}{
		{"session", func(r *protocol.LockResult) { r.Scope.SessionId = "other" }},
		{"handle", func(r *protocol.LockResult) { r.Scope.LockId = "other" }},
		{"name", func(r *protocol.LockResult) { r.Scope.Name = "Migration" }},
		{"request", func(r *protocol.LockResult) { r.RequestId = "other" }},
		{"scope", func(r *protocol.LockResult) { r.Scope = nil }},
		{"enum", func(r *protocol.LockResult) { r.Outcome = 99 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := lockRequest()
			result := &protocol.LockResult{Scope: proto.CloneOf(request.Scope), RequestId: request.RequestId, Outcome: protocol.LockResult_ACQUIRED}
			c.Assert(provider.CheckLockResult(provider.LockTry, request, result), qt.IsNil)
			test.edit(result)
			err := provider.CheckLockResult(provider.LockTry, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
		})
	}
}

func TestLockOutcomesRequireTheRequestedEffect(t *testing.T) {
	for _, test := range []struct {
		name    string
		op      provider.LockOperation
		outcome protocol.LockResult_Outcome
		cause   error
	}{
		{"acquired", provider.LockTry, protocol.LockResult_ACQUIRED, nil},
		{"busy", provider.LockTry, protocol.LockResult_BUSY, provider.ErrLockBusy},
		{"fenced late try", provider.LockTry, protocol.LockResult_NOT_ACQUIRED, provider.ErrExecution},
		{"released attempt", provider.LockTry, protocol.LockResult_RELEASED, provider.ErrExecution},
		{"unknown acquisition", provider.LockTry, protocol.LockResult_UNKNOWN, provider.ErrOutcomeUnknown},
		{"held", provider.LockCheck, protocol.LockResult_ACQUIRED, nil},
		{"lost", provider.LockCheck, protocol.LockResult_LOST, provider.ErrLockLost},
		{"released is not held", provider.LockCheck, protocol.LockResult_RELEASED, provider.ErrLockLost},
		{"absent is not held", provider.LockCheck, protocol.LockResult_NOT_ACQUIRED, provider.ErrLockLost},
		{"busy is not a check", provider.LockCheck, protocol.LockResult_BUSY, provider.ErrProtocol},
		{"released", provider.LockRelease, protocol.LockResult_RELEASED, nil},
		{"release fences absence", provider.LockRelease, protocol.LockResult_NOT_ACQUIRED, nil},
		{"release retains loss", provider.LockRelease, protocol.LockResult_LOST, provider.ErrLockLost},
		{"still held", provider.LockRelease, protocol.LockResult_ACQUIRED, provider.ErrProtocol},
		{"unknown release", provider.LockRelease, protocol.LockResult_UNKNOWN, provider.ErrOutcomeUnknown},
		{"unknown operation", 0, protocol.LockResult_ACQUIRED, provider.ErrProtocol},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := lockRequest()
			result := &protocol.LockResult{Scope: proto.CloneOf(request.Scope), RequestId: request.RequestId, Outcome: test.outcome}
			c.Assert(provider.CheckLockResult(test.op, request, result), qt.ErrorIs, test.cause)
		})
	}
}

func TestLockAdvertisementRequiresTheGlobalService(t *testing.T) {
	c := qt.New(t)
	value := description()
	value.Engines[0].Features = []string{provider.LocksFeature}
	c.Assert(provider.ValidateDescription(value, nil), qt.ErrorIs, provider.ErrProtocol)
	value.Features = append(value.Features, provider.LocksFeature)
	c.Assert(provider.ValidateDescription(value, nil), qt.IsNil)
}
