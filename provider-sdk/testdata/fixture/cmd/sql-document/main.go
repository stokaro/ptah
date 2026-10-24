// Package main serves the fixture engine in the DOCUMENT mode: the provider
// reads every SQL schema document of the engine whole, in the fixture's narrow
// dialect, and the host does not parse the text. It is transport evidence for
// ReadSQLSchema, not an engine grammar.
package main

import (
	"context"
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, fmt.Appendf(nil, "%d", os.Getpid()), 0o600); err != nil { // #nosec G703 -- the host explicitly selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	owner := &documentProvider{Provider: &backend.Provider{}}
	description := backend.SQLDocumentDescription()
	description.Features = append(description.Features, provider.LocksFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.LocksFeature)
	err := runtime.Serve(runtime.Server{Description: description, Locks: owner, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, SQLSchemaReader: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type documentProvider struct{ *backend.Provider }

// NormalizeValues reads a retention object as the retention value it holds,
// and keeps the object's own model kind.
func (p *documentProvider) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	captured := proto.CloneOf(request)
	for _, input := range captured.GetValues() {
		if input.GetValue().GetKind() == backend.RetentionObjectKind {
			input.Value.Kind = "example.org/fixture/retention"
		}
	}
	result, err := p.Provider.NormalizeValues(ctx, captured)
	if err != nil {
		return nil, err
	}
	for i, value := range result.GetValues() {
		value.Value.Kind = request.GetValues()[i].GetValue().GetKind()
	}
	return result, nil
}

func (p *documentProvider) ConvertValues(ctx context.Context, request *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error) {
	return backend.ConvertValues(ctx, request, p)
}
