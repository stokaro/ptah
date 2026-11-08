package conformance_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	"ptah.run/provider-sdk/conformance"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

func TestCheck_ConfigFailurePath(t *testing.T) {
	target := conformance.InProcess(newMemoryProvider(memoryDefects{}).server())
	render := &protocol.RenderRequest{Engine: memoryEngine}
	for _, test := range []struct {
		name   string
		config conformance.Config
		want   string
	}{
		{name: "no target", config: conformance.Config{ProviderID: memoryProviderID}, want: `.*a target is required`},
		{name: "no provider identity", config: conformance.Config{Target: target}, want: `.*the expected provider identity is required`},
		{name: "a negative timeout", config: conformance.Config{Target: target, ProviderID: memoryProviderID, CallTimeout: -1}, want: `.*the call timeout is negative`},
		{
			name:   "a call name with a slash",
			config: conformance.Config{Target: target, ProviderID: memoryProviderID, Calls: []conformance.Call{{Name: "render/create", Request: render}}},
			want:   `.*the call name "render/create" is empty, repeated.*`,
		},
		{
			name: "a repeated call name",
			config: conformance.Config{Target: target, ProviderID: memoryProviderID, Calls: []conformance.Call{
				{Name: "render", Request: render}, {Name: "render", Request: render},
			}},
			want: `.*the call name "render" is empty, repeated.*`,
		},
		{
			name:   "a call to a session-bound service",
			config: conformance.Config{Target: target, ProviderID: memoryProviderID, Calls: []conformance.Call{{Name: "inspect", Request: &protocol.InspectRequest{}}}},
			want:   `.*call inspect does not send a request of an offline service`,
		},
		{
			name:   "a live part without an address",
			config: conformance.Config{Target: target, ProviderID: memoryProviderID, Live: &conformance.Live{}},
			want:   `.*the live part needs an address`,
		},
		{
			name:   "a live call that builds nothing",
			config: conformance.Config{Target: target, ProviderID: memoryProviderID, Live: &conformance.Live{Address: "memory:///x", Calls: []conformance.LiveCall{{Name: "rows"}}}},
			want:   `.*live call rows builds no request`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := conformance.Check(c.Context(), test.config)

			c.Assert(err, qt.ErrorIs, conformance.ErrConfig)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(report, qt.IsNil)
		})
	}
}

// A provider that does not start fails the first check, and no other check
// runs. The executable path is absolute on every platform, so that each row
// reaches the check it names; nothing is started.
func TestCheck_NotStarted(t *testing.T) {
	absolute := filepath.Join(os.TempDir(), "ptah-conformance-provider")
	for _, test := range []struct {
		name   string
		target conformance.Target
		want   string
	}{
		{name: "a relative executable", target: conformance.Executable("bin/provider", strings.Repeat("0", 64)), want: "the executable's path is not absolute"},
		{name: "a checksum of another length", target: conformance.Executable(absolute, "00"), want: "the executable's checksum is not a SHA-256 digest"},
		{name: "a reserved environment entry", target: conformance.Executable(absolute, strings.Repeat("0", 64), "PLUGIN_X=1"), want: "invalid or reserved provider environment entry"},
		{name: "a false advertisement", target: conformance.InProcess(runtime.Server{Description: memoryDescription(), Sessions: newMemoryProvider(memoryDefects{})}),
			want: "invalid provider protocol exchange: schema.inspect advertisement differs from implementation"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := conformance.Check(c.Context(), conformance.Config{Target: test.target, ProviderID: memoryProviderID})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Results[0], qt.DeepEquals, conformance.Result{ID: "bootstrap.start", Status: conformance.Failed, Detail: test.want})
			c.Assert(report.Results[1:], qt.HasLen, len(conformance.Checks())-1)
			c.Assert(report.Results[1].Status, qt.Equals, conformance.NotApplicable)
			c.Assert(report.Provider, qt.IsNil)
		})
	}
}

// retention is the observed model of the memory provider's model variant.
const retention = memoryProviderID + "/retention"

// modelServer is the memory provider with one observed model at schema
// version 2, and, when formerDigest is set, the identity it had at version
// 1 named as its own predecessor.
func modelServer(formerDigest string) runtime.Server {
	definition := []byte(`{"type":"retention"}`)
	sum := sha256.Sum256(definition)
	server := newMemoryProvider(memoryDefects{}).server()
	engine := server.Description.Engines[0]
	engine.ValueModels = []*protocol.ValueModel{{Kind: retention, Representation: protocol.ValueInput_OBSERVED, SchemaVersion: 2, Encoding: "json",
		DefinitionSha256: hex.EncodeToString(sum[:]), CanonicalFormat: "json", DefinitionJson: definition}}
	if formerDigest != "" {
		server.Description.Features = append(server.Description.Features, provider.ModelPredecessorsFeature, provider.OwnPredecessorsFeature)
		engine.Features = append(engine.Features, provider.ModelPredecessorsFeature, provider.OwnPredecessorsFeature)
		engine.ModelPredecessors = []*protocol.ModelPredecessor{{Kind: retention, Representation: protocol.ValueInput_OBSERVED,
			FormerOwner: memoryProviderID, FormerKind: retention, SchemaVersion: 1, DefinitionSha256: formerDigest}}
	}
	return server
}

func TestModelIdentities_HappyPath(t *testing.T) {
	c := qt.New(t)
	description := modelServer("").Description

	identities := conformance.ModelIdentities(description)

	c.Assert(identities, qt.DeepEquals, []conformance.ModelIdentity{{Owner: memoryProviderID, Engine: memoryEngine, Kind: retention, Representation: "OBSERVED",
		SchemaVersion: 2, DefinitionSHA256: description.Engines[0].ValueModels[0].GetDefinitionSha256()}})
}

func TestModels_HappyPath(t *testing.T) {
	former := strings.Repeat("a", 64)
	current := modelServer("")
	for _, test := range []struct {
		name   string
		server runtime.Server
		models []conformance.ModelIdentity
	}{
		{name: "the current identity", server: current, models: conformance.ModelIdentities(current.Description)},
		{name: "an identity a predecessor names", server: modelServer(former), models: []conformance.ModelIdentity{
			{Owner: memoryProviderID, Engine: memoryEngine, Kind: retention, Representation: "OBSERVED", SchemaVersion: 1, DefinitionSHA256: former},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			config := conformance.Config{Target: conformance.InProcess(test.server), ProviderID: memoryProviderID, Models: test.models}

			report, err := conformance.Check(c.Context(), config)

			c.Assert(err, qt.IsNil)
			c.Assert(statuses(report)["models.recorded"], qt.Equals, conformance.Passed)
			c.Assert(report.Err(), qt.IsNil)
		})
	}
}

// An identity an earlier release described that the provider neither
// describes nor names as a predecessor no longer reads: a file exported
// under it is refused.
func TestModels_FailurePath(t *testing.T) {
	c := qt.New(t)
	recorded := conformance.ModelIdentity{Owner: memoryProviderID, Engine: memoryEngine, Kind: retention, Representation: "OBSERVED", SchemaVersion: 1,
		DefinitionSHA256: strings.Repeat("a", 64)}
	config := conformance.Config{Target: conformance.InProcess(modelServer("")), ProviderID: memoryProviderID, Models: []conformance.ModelIdentity{recorded}}

	report, err := conformance.Check(c.Context(), config)

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failures(), qt.DeepEquals, []conformance.Result{{ID: "models.recorded", Status: conformance.Failed,
		Detail: recorded.String() + " no longer reads: the provider neither describes it nor names it as a predecessor"}})
}

// A live call names a session-bound read; a request of another service, or
// of one the provider does not offer, fails the call.
func TestLiveCalls_FailurePath(t *testing.T) {
	config := memoryConfig(conformance.InProcess(newMemoryProvider(memoryDefects{}).server()), "calls")
	config.Live.Calls = []conformance.LiveCall{
		{Name: "inspect", Request: func(session *protocol.Session) proto.Message {
			return &protocol.InspectRequest{SessionId: session.GetId()}
		}},
		{Name: "rows", Request: func(session *protocol.Session) proto.Message {
			return &protocol.ReadRowsRequest{SessionId: session.GetId(), Table: &protocol.Reference{Kind: "table", Name: "notes"}}
		}},
	}
	c := qt.New(t)

	report, err := conformance.Check(c.Context(), config)

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failures(), qt.DeepEquals, []conformance.Result{
		{ID: "live.calls.inspect", Status: conformance.Failed, Detail: "the request is not one of the session-bound reads the suite sends"},
		{ID: "live.calls.rows", Status: conformance.Failed, Detail: "the call needs data.read-rows, which the description does not offer"},
	})
}

// Run reports each check as a subtest and logs the ones that do not apply.
func TestRun_HappyPath(t *testing.T) {
	conformance.Run(t, memoryConfig(conformance.InProcess(newMemoryProvider(memoryDefects{}).server()), "run"))
}

func TestReport_FailurePath(t *testing.T) {
	c := qt.New(t)
	report := &conformance.Report{Results: []conformance.Result{
		{ID: "bootstrap.start", Status: conformance.Passed},
		{ID: "live.locks", Status: conformance.Failed, Detail: "a second session took the lock"},
		{ID: "live.calls", Status: conformance.NotApplicable, Detail: "none"},
	}}

	err := report.Err()

	c.Assert(err, qt.ErrorIs, conformance.ErrNonconforming)
	c.Assert(err, qt.ErrorMatches, "the provider does not conform: 1 of 3 checks failed:\nlive.locks: a second session took the lock")
}

func TestChecks_HappyPath(t *testing.T) {
	c := qt.New(t)

	checks := conformance.Checks()

	c.Assert(checks[0].ID, qt.Equals, "bootstrap.start")
	c.Assert(checks[len(checks)-1].ID, qt.Equals, "reasons.redacted")
	c.Assert(conforming(), qt.HasLen, len(checks)+1)
}
