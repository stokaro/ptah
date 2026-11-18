package runtime_test

import (
	"context"
	"slices"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	transport "ptah.run/provider-sdk/runtime"
)

// trustDescription declares the replay guard and the further features on the
// provider and its engine: an older provider lists none, and one that honors
// trust lists provider.ReplayTrustFeature.
func trustDescription(further ...string) *protocol.Description {
	features := append([]string{provider.ReplayGuardFeature}, further...)
	return &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1",
		Features: slices.Clone(features),
		Engines: []*protocol.Engine{{Id: "test", Features: slices.Clone(features), Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT,
			ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}}
}

// trustRecorder records the trust each request carried to the provider's
// services, and refuses only VACUUM INTO, and only without trust, as a guard
// that cannot confine a write outside the database does.
type trustRecorder struct {
	mu     sync.Mutex
	guards []bool
	opens  []bool
}

func (r *trustRecorder) GuardReplay(_ context.Context, request *protocol.GuardReplayRequest) (*protocol.GuardedReplay, error) {
	r.mu.Lock()
	r.guards = append(r.guards, request.GetScope().GetTrustedSql())
	r.mu.Unlock()
	result := &protocol.GuardedReplay{}
	for _, statement := range request.GetStatements() {
		verdict := &protocol.ReplayVerdict{}
		if statement == "VACUUM INTO 'copy.db'" && !request.GetScope().GetTrustedSql() {
			verdict.Refused, verdict.Operation = true, "VACUUM INTO writes another database file"
		}
		result.Verdicts = append(result.Verdicts, verdict)
	}
	return result, nil
}

func (r *trustRecorder) Open(_ context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	r.mu.Lock()
	r.opens = append(r.opens, request.GetTrustedSql())
	r.mu.Unlock()
	return &protocol.Session{Id: "session", Engine: request.GetEngine(), Identifiers: trustDescription().GetEngines()[0].GetIdentifiers()}, nil
}

func (*trustRecorder) Close(context.Context, *protocol.CloseRequest) error { return nil }

func (r *trustRecorder) seen() (guards, opens []bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.guards), slices.Clone(r.opens)
}

// serveTrust serves a provider with description, negotiated with a host that
// lists hostFeatures.
func serveTrust(c *qt.C, description *protocol.Description, hostFeatures []string) (protocol.ProviderClient, *trustRecorder) {
	c.Helper()
	recorder := &trustRecorder{}
	client, stop, err := transport.ServeInMemory(transport.Server{Description: description, Sessions: recorder, ReplayGuard: recorder})
	c.Assert(err, qt.IsNil)
	c.Cleanup(stop)
	_, err = client.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision, HostContract: provider.Contract(), HostFeatures: hostFeatures})
	c.Assert(err, qt.IsNil)
	return client, recorder
}

func guardRequest(scope *protocol.ReplayScope) *protocol.GuardReplayRequest {
	return &protocol.GuardReplayRequest{Engine: "test", Scope: scope, Statements: []string{"VACUUM INTO 'copy.db'"}}
}

// A negotiated trust reaches the provider as the host sent it: true and false
// arrive as written, and a request that says nothing about trust arrives as
// false, which is the strict default. The guard lifts its refusal only for the
// trusted request.
func TestReplayTrust_HappyPath(t *testing.T) {
	c := qt.New(t)
	client, recorder := serveTrust(c, trustDescription(provider.ReplayTrustFeature), []string{provider.ReplayTrustFeature})

	trusted, err := client.GuardReplay(c.Context(), guardRequest(&protocol.ReplayScope{Database: "dev", TrustedSql: true}))
	c.Assert(err, qt.IsNil)
	untrusted, err := client.GuardReplay(c.Context(), guardRequest(&protocol.ReplayScope{Database: "dev", TrustedSql: false}))
	c.Assert(err, qt.IsNil)
	omitted, err := client.GuardReplay(c.Context(), guardRequest(&protocol.ReplayScope{Database: "dev"}))
	c.Assert(err, qt.IsNil)
	_, err = client.Open(c.Context(), &protocol.OpenRequest{Engine: "test", Address: "test://dev", TrustedSql: true})
	c.Assert(err, qt.IsNil)
	_, err = client.Open(c.Context(), &protocol.OpenRequest{Engine: "test", Address: "test://dev"})
	c.Assert(err, qt.IsNil)

	guards, opens := recorder.seen()
	c.Assert(guards, qt.DeepEquals, []bool{true, false, false})
	c.Assert(opens, qt.DeepEquals, []bool{true, false})
	c.Assert(trusted.GetVerdicts()[0].GetRefused(), qt.IsFalse)
	c.Assert(untrusted.GetVerdicts()[0].GetRefused(), qt.IsTrue)
	c.Assert(omitted.GetVerdicts()[0].GetRefused(), qt.IsTrue)
}

// An older provider, which does not declare trust, keeps working for a host
// that lists it, as long as the host sends no trust: the strict requests reach
// it unchanged.
func TestReplayTrustOlderProvider_HappyPath(t *testing.T) {
	c := qt.New(t)
	client, recorder := serveTrust(c, trustDescription(), []string{provider.ReplayTrustFeature})

	described, err := client.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision, HostContract: provider.Contract(),
		HostFeatures: []string{provider.ReplayTrustFeature}})
	c.Assert(err, qt.IsNil)
	guarded, err := client.GuardReplay(c.Context(), guardRequest(&protocol.ReplayScope{Database: "dev"}))
	c.Assert(err, qt.IsNil)
	_, err = client.Open(c.Context(), &protocol.OpenRequest{Engine: "test", Address: "test://dev"})
	c.Assert(err, qt.IsNil)

	guards, opens := recorder.seen()
	c.Assert(provider.EngineOffers(described, "test", provider.ReplayTrustFeature), qt.IsFalse)
	c.Assert(guarded.GetVerdicts()[0].GetRefused(), qt.IsTrue)
	c.Assert(guards, qt.DeepEquals, []bool{false})
	c.Assert(opens, qt.DeepEquals, []bool{false})
}

// Trust that was not negotiated never reaches the provider's services: the
// server refuses it, whether the provider does not declare it or the host did
// not list it, so neither side can act on an answer the other cannot honor.
func TestReplayTrust_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name         string
		description  *protocol.Description
		hostFeatures []string
	}{
		{name: "an older provider", description: trustDescription(), hostFeatures: []string{provider.ReplayTrustFeature}},
		{name: "a host that does not list the feature", description: trustDescription(provider.ReplayTrustFeature)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			client, recorder := serveTrust(c, test.description, test.hostFeatures)

			guarded, guardErr := client.GuardReplay(c.Context(), guardRequest(&protocol.ReplayScope{Database: "dev", TrustedSql: true}))
			opened, openErr := client.Open(c.Context(), &protocol.OpenRequest{Engine: "test", Address: "test://dev", TrustedSql: true})

			guards, opens := recorder.seen()
			c.Assert(status.Code(guardErr), qt.Equals, codes.InvalidArgument)
			c.Assert(guardErr, qt.ErrorMatches, `.*trusted replay SQL requires database\.dev-replay-trust for engine test`)
			c.Assert(guarded, qt.IsNil)
			c.Assert(status.Code(openErr), qt.Equals, codes.InvalidArgument)
			c.Assert(openErr, qt.ErrorMatches, `.*a session for trusted dev SQL requires database\.dev-replay-trust for engine test`)
			c.Assert(opened, qt.IsNil)
			c.Assert(guards, qt.HasLen, 0)
			c.Assert(opens, qt.HasLen, 0)
		})
	}
}
