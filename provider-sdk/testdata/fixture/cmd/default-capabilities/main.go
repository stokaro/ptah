// Package main serves the fixture engine as an engine that renders only
// against an established answer for transactional_ddl, as the YDB provider
// renders a table only against established nontransactional DDL. It states
// that answer for offline planning (contract 25.22,
// engine.default-capabilities), false by default and on release line 2.1 and
// true on line 1.4, unless FIXTURE_NO_DEFAULTS is set, which is the control:
// without the stated facts an offline render is refused. A session reports
// the server version FIXTURE_SERVER_VERSION names, when it names one, so a
// command that connects can be measured against the declared lines.
//
// The engine is fixture-defaults, or the control's fixture-no-defaults: the
// host keeps one set of offline facts per engine for the life of a process,
// so the two cases cannot share an engine within one test process.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host explicitly selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	engine, stated := "fixture-defaults", os.Getenv("FIXTURE_NO_DEFAULTS") == ""
	if !stated {
		engine = "fixture-no-defaults"
	}
	owner := &establishedRenderer{Provider: &backend.Provider{Engine: engine}}
	description := backend.Description()
	description.SemanticIdentity = "fixture-default-capabilities-1"
	served := description.Engines[0]
	served.Id = engine
	if stated {
		description.Features = append(description.Features, provider.DefaultCapabilitiesFeature)
		served.Features = append(served.Features, provider.DefaultCapabilitiesFeature)
		served.DefaultCapabilities = map[string]bool{"transactional_ddl": false}
		served.ReleaseLines = []*protocol.ReleaseLine{
			{Line: "2.1", Label: "Fixture 2.1", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "a fixture line", Capabilities: map[string]bool{"transactional_ddl": false}},
			{Line: "1.4", Label: "Fixture 1.4", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "a fixture line", Capabilities: map[string]bool{"transactional_ddl": true}},
		}
	}
	// FIXTURE_CAPABILITY_SOURCE is the line the engine states about where its
	// connection reads refined capabilities (contract 25.35).
	if source := os.Getenv("FIXTURE_CAPABILITY_SOURCE"); source != "" {
		description.Features = append(description.Features, provider.ConnectionCapabilitySourceFeature)
		served.Features = append(served.Features, provider.ConnectionCapabilitySourceFeature)
		served.ConnectionCapabilitySource = source
	}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type establishedRenderer struct{ *backend.Provider }

// Open opens a session of the fixture backend that reports the server version
// FIXTURE_SERVER_VERSION names, or the backend's own when it names none.
func (r *establishedRenderer) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := r.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	if version := os.Getenv("FIXTURE_SERVER_VERSION"); version != "" {
		session.ServerVersion = version
	}
	// FIXTURE_SESSION_TRANSACTIONAL_DDL=true has the connection answer
	// transactional_ddl as supported, which line 2.1 answers unsupported: a
	// capability the connection refines.
	if os.Getenv("FIXTURE_SESSION_TRANSACTIONAL_DDL") == "true" {
		if session.Capabilities == nil {
			session.Capabilities = make(map[string]bool)
		}
		session.Capabilities["transactional_ddl"] = true
	}
	return session, nil
}

// Render refuses a request that establishes no answer for transactional_ddl.
// Otherwise it writes one unit per change that names the change's object and
// the answer it rendered against, which is the evidence a test reads.
func (*establishedRenderer) Render(ctx context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	transactional, established := request.GetCapabilities()["transactional_ddl"]
	if !established {
		return nil, status.Error(codes.FailedPrecondition, "the fixture renders only against an established transactional_ddl")
	}
	result := &protocol.RenderResult{Complete: true}
	for _, change := range request.GetChanges() {
		text := fmt.Sprintf("-- transactional_ddl=%t\nCREATE TABLE %q (id INTEGER);", transactional, change.GetObject().GetName())
		result.Units = append(result.Units, &protocol.Unit{ChangeId: change.GetId(), Body: &protocol.Unit_Sql{Sql: text}})
	}
	return result, nil
}
