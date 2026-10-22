// Package main exercises schema validation through an independent provider.
package main

import (
	"context"
	"fmt"
	"os"
	"slices"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-schema-validation"
	description.Features = append(description.Features, provider.ValidateSchemaFeature)
	engine := description.Engines[0]
	engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == provider.CommonTableIdentityFeature })
	engine.Features = append(engine.Features, provider.ValidateSchemaFeature)
	owner := &validator{Provider: &backend.Provider{}}
	if err := runtime.Serve(runtime.Server{
		Description:     description,
		Sessions:        owner,
		Inspector:       owner,
		Renderer:        owner,
		Executor:        owner,
		SQLAnalyzer:     owner,
		ValueNormalizer: owner,
		SourceDecoder:   owner,
		ValueConverter:  owner,
		SchemaValidator: owner,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type validator struct{ *backend.Provider }

func (*validator) ValidateSchema(ctx context.Context, request *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	result := &protocol.SchemaValidation{Complete: true}
	for _, input := range request.Tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		table := input.Declaration
		switch table.Name {
		case "records":
			if len(table.Columns) != 1 || table.Columns[0].Comment != "retained" ||
				len(request.Namespaces) != 1 || request.Namespaces[0].Comment != "namespace comment" ||
				len(request.NotDescribed) != 1 || request.NotDescribed[0].Kind != "role" ||
				!slices.Equal(request.Dependencies["records"].GetTables(), []string{"parent"}) {
				return nil, fmt.Errorf("incomplete schema capture")
			}
			if request.NoSkipped {
				result.Diagnostics = append(result.Diagnostics, &protocol.SchemaDiagnostic{Code: protocol.SchemaDiagnostic_OMITTED_DECLARATION, Kind: "comment", Object: "records", Message: "Fixture does not store comments"})
			}
		case "invalid":
			result.Diagnostics = append(result.Diagnostics, &protocol.SchemaDiagnostic{Code: protocol.SchemaDiagnostic_UNSUPPORTED_FEATURE, Kind: "table", Object: table.Name, Feature: "fixture", Message: "Fixture refuses this declaration"})
		case "malformed":
			result.Diagnostics = append(result.Diagnostics, nil)
		}
	}
	return result, nil
}
