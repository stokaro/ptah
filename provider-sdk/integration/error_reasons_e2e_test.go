//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

func reasonsOptions(c *qt.C, hostFeatures ...string) transport.Options {
	c.Helper()
	binary := buildSDKFixture(c, "sdk-error-reasons")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		RequiredFeatures: []string{provider.LocksFeature}, HostFeatures: hostFeatures}
}

// An independent provider's reasons reach a host that lists the feature:
// the reason for a refused inspection, and nothing of the status message that
// quotes the database, the reason for a busy lock, and the reasons for an
// execution that applied nothing and a commit that rolled back, and nothing
// of their messages. A host that does not list it reads the code and the
// outcome alone.
func TestIndependentProviderReasons(t *testing.T) {
	tests := []struct {
		name         string
		hostFeatures []string
		listed       bool
		wantInspect  string
		wantLock     string
		wantExecute  string
		wantCommit   string
	}{
		{name: "a host that lists the feature", hostFeatures: []string{provider.ErrorReasonsFeature}, listed: true,
			wantInspect: "provider inspect failed (FailedPrecondition): the fixture refuses to read this database",
			wantLock:    "provider lock outcome is BUSY: another session holds the lock",
			wantExecute: "provider execution: NOT_APPLIED (0 units confirmed applied): the fixture refuses to run this unit",
			wantCommit:  "provider transaction outcome is ROLLED_BACK: the fixture refuses to commit"},
		{name: "a host that does not list it", listed: false,
			wantInspect: "provider inspect failed (FailedPrecondition)",
			wantLock:    "provider lock outcome is BUSY",
			wantExecute: "provider execution: NOT_APPLIED (0 units confirmed applied)",
			wantCommit:  "provider transaction outcome is ROLLED_BACK"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			options := reasonsOptions(c, test.hostFeatures...)
			client, contender := start(c, options), start(c, options)
			c.Assert(slices.Contains(client.Description().GetFeatures(), provider.ErrorReasonsFeature), qt.Equals, test.listed)

			refused := open(c, client, "refuse")
			schema, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: refused.GetId()})
			c.Assert(schema, qt.IsNil)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.wantInspect)

			executed, err := client.Execute(c.Context(), execution(refused.GetId()))
			c.Assert(err, qt.ErrorIs, provider.ErrExecution)
			c.Assert(err.Error(), qt.Equals, test.wantExecute)
			c.Assert(executed.GetOutcome(), qt.Equals, protocol.Execution_NOT_APPLIED)
			_, err = client.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{SessionId: refused.GetId(), TransactionId: "refused",
				Isolation: protocol.TransactionIsolation_DEFAULT})
			c.Assert(err, qt.IsNil)
			finished, err := client.FinishTransaction(c.Context(), &protocol.FinishTransactionRequest{SessionId: refused.GetId(), TransactionId: "refused",
				Action: protocol.FinishTransactionRequest_COMMIT})
			c.Assert(err, qt.ErrorIs, provider.ErrExecution)
			c.Assert(err.Error(), qt.Equals, test.wantCommit)
			c.Assert(finished.GetOutcome(), qt.Equals, protocol.TransactionResult_ROLLED_BACK)

			holder := open(c, client, "")
			_, err = client.TryLock(c.Context(), lockRequest(holder, "holder", "migration"))
			c.Assert(err, qt.IsNil)
			waiter := reopenLockedDatabase(c, contender, holder.GetDatabase(), "")
			result, err := contender.TryLock(c.Context(), lockRequest(waiter, "waiter", "migration"))
			c.Assert(err, qt.ErrorIs, provider.ErrLockBusy)
			c.Assert(err.Error(), qt.Equals, test.wantLock)
			c.Assert(result.GetOutcome(), qt.Equals, protocol.LockResult_BUSY)
		})
	}
}
