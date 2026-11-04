package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func TestValidateDiagnostic_HappyPath(t *testing.T) {
	tests := []struct {
		name, reason string
	}{
		{name: "a sentence", reason: "the streaming query capability was not established"},
		{name: "512 bytes", reason: strings.Repeat("a", 512)},
		{name: "text outside ASCII", reason: "la table « items » n’existe pas"},
		{name: "a quoted name", reason: `table "items" has no column "price"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDiagnostic(&protocol.ProviderDiagnostic{Reason: test.reason}), qt.IsNil)
		})
	}
}

// Each rule refuses a reason a host could not print on one line: a stack
// trace, a dumped driver message or a terminal escape all fail one of them.
func TestValidateDiagnostic_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		diagnostic *protocol.ProviderDiagnostic
	}{
		{name: "no diagnostic", diagnostic: nil},
		{name: "an empty reason", diagnostic: &protocol.ProviderDiagnostic{}},
		{name: "513 bytes", diagnostic: &protocol.ProviderDiagnostic{Reason: strings.Repeat("a", 513)}},
		{name: "invalid UTF-8", diagnostic: &protocol.ProviderDiagnostic{Reason: "table \xff is missing"}},
		{name: "a leading space", diagnostic: &protocol.ProviderDiagnostic{Reason: " table is missing"}},
		{name: "a trailing space", diagnostic: &protocol.ProviderDiagnostic{Reason: "table is missing "}},
		{name: "two lines", diagnostic: &protocol.ProviderDiagnostic{Reason: "table is missing\nat line 2"}},
		{name: "a tab", diagnostic: &protocol.ProviderDiagnostic{Reason: "table\tis missing"}},
		{name: "a terminal escape", diagnostic: &protocol.ProviderDiagnostic{Reason: "table \x1b[31mis missing"}},
		{name: "a NUL", diagnostic: &protocol.ProviderDiagnostic{Reason: "table\x00is missing"}},
		{name: "a C1 control", diagnostic: &protocol.ProviderDiagnostic{Reason: "table\u0085is missing"}},
		{name: "a line separator", diagnostic: &protocol.ProviderDiagnostic{Reason: "table is missing"}},
		{name: "a paragraph separator", diagnostic: &protocol.ProviderDiagnostic{Reason: "table is missing"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.ValidateDiagnostic(test.diagnostic)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*a provider's reason is one line of at most 512 bytes of text`)
		})
	}
}

// The feature is two-way and the provider's alone: negotiation removes it
// from a description unless the host lists it.
func TestErrorReasonsFeatureIsNegotiated(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ErrorReasonsFeature)
	offered := description()
	offered.Features = append(offered.Features, provider.ErrorReasonsFeature)
	c.Assert(provider.ValidateDescription(offered, nil), qt.IsNil)
	c.Assert(provider.NegotiatedDescription(offered, []string{provider.ErrorReasonsFeature}).GetFeatures(), qt.Contains, provider.ErrorReasonsFeature)
	c.Assert(provider.NegotiatedDescription(offered, nil).GetFeatures(), qt.Not(qt.Contains), provider.ErrorReasonsFeature)
}

func reasonedReceipt(outcome protocol.LockResult_Outcome, reason string) (*protocol.LockRequest, *protocol.LockResult) {
	request := lockRequest()
	return request, &protocol.LockResult{Scope: proto.CloneOf(request.Scope), RequestId: request.RequestId, Outcome: outcome,
		Diagnostic: &protocol.ProviderDiagnostic{Reason: reason}}
}

// A receipt whose outcome is a failure of its operation may say why, and the
// reason reaches the LockError beside the outcome.
func TestLockReceiptReason_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		operation provider.LockOperation
		outcome   protocol.LockResult_Outcome
		cause     error
		want      string
	}{
		{name: "a busy acquisition", operation: provider.LockTry, outcome: protocol.LockResult_BUSY, cause: provider.ErrLockBusy,
			want: "provider lock outcome is BUSY: another session holds the lock node"},
		{name: "a refused acquisition", operation: provider.LockTry, outcome: protocol.LockResult_NOT_ACQUIRED, cause: provider.ErrExecution,
			want: "provider lock outcome is NOT_ACQUIRED: another session holds the lock node"},
		{name: "an unknown acquisition", operation: provider.LockTry, outcome: protocol.LockResult_UNKNOWN, cause: provider.ErrOutcomeUnknown,
			want: "provider lock outcome is UNKNOWN: another session holds the lock node"},
		{name: "a lost lock", operation: provider.LockCheck, outcome: protocol.LockResult_LOST, cause: provider.ErrLockLost,
			want: "provider lock outcome is LOST: another session holds the lock node"},
		{name: "a check of an absent lock", operation: provider.LockCheck, outcome: protocol.LockResult_NOT_ACQUIRED, cause: provider.ErrLockLost,
			want: "provider lock ownership was lost\nprovider lock outcome is NOT_ACQUIRED: another session holds the lock node"},
		{name: "an unknown release", operation: provider.LockRelease, outcome: protocol.LockResult_UNKNOWN, cause: provider.ErrOutcomeUnknown,
			want: "provider lock outcome is UNKNOWN: another session holds the lock node"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := reasonedReceipt(test.outcome, "another session holds the lock node")
			c.Assert(provider.ValidateLockResult(test.operation, request, result), qt.IsNil)

			err := provider.CheckLockResult(test.operation, request, result)

			c.Assert(err, qt.ErrorIs, test.cause)
			c.Assert(err, qt.Not(qt.ErrorIs), provider.ErrProtocol)
			c.Assert(err.Error(), qt.Equals, test.want)
			var failure *provider.LockError
			c.Assert(err, qt.ErrorAs, &failure)
			c.Assert(failure.Reason, qt.Equals, "another session holds the lock node")
		})
	}
}

// A reason never accompanies an outcome its operation succeeds with, and a
// malformed reason makes the receipt invalid, which proves no ownership.
func TestLockReceiptReason_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		operation provider.LockOperation
		outcome   protocol.LockResult_Outcome
		reason    string
		wantErr   string
	}{
		{name: "an acquisition", operation: provider.LockTry, outcome: protocol.LockResult_ACQUIRED, reason: "acquired after a wait",
			wantErr: `.*a lock receipt gives a reason only for an outcome its operation does not succeed with`},
		{name: "a held lock", operation: provider.LockCheck, outcome: protocol.LockResult_ACQUIRED, reason: "still held",
			wantErr: `.*a lock receipt gives a reason only for an outcome its operation does not succeed with`},
		{name: "a release", operation: provider.LockRelease, outcome: protocol.LockResult_RELEASED, reason: "released",
			wantErr: `.*a lock receipt gives a reason only for an outcome its operation does not succeed with`},
		{name: "a fenced handle", operation: provider.LockRelease, outcome: protocol.LockResult_NOT_ACQUIRED, reason: "never acquired",
			wantErr: `.*a lock receipt gives a reason only for an outcome its operation does not succeed with`},
		{name: "a reason of two lines", operation: provider.LockTry, outcome: protocol.LockResult_BUSY, reason: "busy\nsince noon",
			wantErr: `.*a provider's reason is one line of at most 512 bytes of text`},
		{name: "an empty reason", operation: provider.LockCheck, outcome: protocol.LockResult_LOST, reason: "",
			wantErr: `.*a provider's reason is one line of at most 512 bytes of text`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := reasonedReceipt(test.outcome, test.reason)

			c.Assert(provider.ValidateLockResult(test.operation, request, result), qt.ErrorMatches, test.wantErr)
			err := provider.CheckLockResult(test.operation, request, result)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
			var failure *provider.LockError
			c.Assert(err, qt.ErrorAs, &failure)
			c.Assert(failure.Reason, qt.Equals, "")
		})
	}
}

// A failure without a diagnostic reads as before the feature existed.
func TestLockErrorWithoutReason(t *testing.T) {
	c := qt.New(t)
	request := lockRequest()
	result := &protocol.LockResult{Scope: proto.CloneOf(request.Scope), RequestId: request.RequestId, Outcome: protocol.LockResult_BUSY}
	err := provider.CheckLockResult(provider.LockTry, request, result)
	c.Assert(err, qt.ErrorMatches, `provider lock outcome is BUSY`)
	var failure *provider.LockError
	c.Assert(err, qt.ErrorAs, &failure)
	c.Assert(failure.Reason, qt.Equals, "")
}
