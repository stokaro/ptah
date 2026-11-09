// Package main serves the fixture's migration lint with the statement facts of
// contract 25.1 (see package lintfacts), so `ptah sql lint` can read a
// provider's split through an independent process.
//
// FIXTURE_LINT_ENGINE=ydb serves the lint under the identity of the official
// YDB provider, engine ydb, as the ydb-lint fixture does, and states the
// capabilities of two YDB release lines, so a run resolves a server version
// through the lines the provider declares. YDB 25.1 has no parameterized
// DECIMAL and 26.2 has one. Otherwise the process is the fixture provider,
// engine fixture, which declares no release line.
package main

import (
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	"example.com/ptah-fixture-provider/lintfacts"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	owner := &backend.Provider{}
	server := runtime.Server{Description: backend.Description(), Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, Linter: lintfacts.Linter{}}
	description := server.Description
	description.SemanticIdentity += "-lint-facts"
	description.Features = append(description.Features, provider.LintFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.LintFeature, provider.LintStatementFactsFeature)
	engine.LintRules = backend.LintRules()
	if os.Getenv("FIXTURE_LINT_ENGINE") == "ydb" {
		server.Description = &protocol.Description{Revision: description.Revision, ProviderId: "github.com/stokaro/ptah-ydb-plugin",
			Version: description.Version, SemanticIdentity: description.SemanticIdentity,
			Engines: []*protocol.Engine{{Id: "ydb", DefaultSchema: engine.DefaultSchema, Identifiers: engine.Identifiers, LintRules: engine.LintRules,
				Features:            []string{provider.LintFeature, provider.LintStatementFactsFeature, provider.DefaultCapabilitiesFeature},
				DefaultCapabilities: map[string]bool{"parameterized_decimal": true},
				ReleaseLines: []*protocol.ReleaseLine{
					{Line: "25.1", Label: "YDB 25.1", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "a fixture line", Capabilities: map[string]bool{"parameterized_decimal": false}},
					{Line: "26.2", Label: "YDB 26.2", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "a fixture line", Capabilities: map[string]bool{"parameterized_decimal": true}},
				}}},
			Features: []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature, provider.AnalyzeSQLFeature, provider.LintFeature, provider.DefaultCapabilitiesFeature}}
		server.ValueNormalizer, server.SourceDecoder, server.ValueConverter = nil, nil, nil
	}
	if err := runtime.Serve(server); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
