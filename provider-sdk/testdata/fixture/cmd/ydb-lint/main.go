// Package main serves the fixture's migration lint under the identity of the
// official YDB provider: provider github.com/stokaro/ptah-ydb-plugin, engine
// ydb. That is the shape in which the YDB provider serves YQL migration lint,
// so a run that names the ydb dialect reaches this process, with the
// capabilities the host resolved for the server version it names.
//
// PTAH_FIXTURE_LINT_RULES replaces the published rule catalog with the JSON
// array it holds, so a test can hand the process the catalog a provider
// publishes and read it back through the host.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

// publishedRule is one rule of the catalog PTAH_FIXTURE_LINT_RULES holds.
type publishedRule struct {
	Code          string   `json:"code"`
	Title         string   `json:"title"`
	Severity      string   `json:"severity"`
	AppliesToDown bool     `json:"applies_to_down"`
	Subsumes      []string `json:"subsumes"`
}

var severities = map[string]protocol.LintRule_Severity{
	"info": protocol.LintRule_INFO, "warning": protocol.LintRule_WARNING, "error": protocol.LintRule_ERROR,
}

func rules() ([]*protocol.LintRule, error) {
	catalog, found := os.LookupEnv("PTAH_FIXTURE_LINT_RULES")
	if !found {
		return backend.LintRules(), nil
	}
	var published []publishedRule
	if err := json.Unmarshal([]byte(catalog), &published); err != nil {
		return nil, fmt.Errorf("PTAH_FIXTURE_LINT_RULES: %w", err)
	}
	result := make([]*protocol.LintRule, 0, len(published))
	for _, rule := range published {
		result = append(result, &protocol.LintRule{Code: rule.Code, Title: rule.Title, Severity: severities[rule.Severity],
			AppliesToDown: rule.AppliesToDown, Subsumes: rule.Subsumes})
	}
	return result, nil
}

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	published, err := rules()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fixture := backend.Description()
	description := &protocol.Description{Revision: fixture.Revision, ProviderId: "github.com/stokaro/ptah-ydb-plugin", Version: fixture.Version,
		SemanticIdentity: fixture.SemanticIdentity + "-ydb-lint",
		Engines: []*protocol.Engine{{Id: "ydb", DefaultSchema: fixture.Engines[0].DefaultSchema, Features: []string{"tables", "columns", provider.LintFeature},
			Identifiers: fixture.Engines[0].Identifiers, LintRules: published}},
		Features: []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature, provider.AnalyzeSQLFeature, provider.LintFeature}}
	owner := &backend.Provider{}
	err = runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		Linter: backend.Linter{}})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
