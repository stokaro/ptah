// Package main exercises owned table preparation through an independent process.
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
	"google.golang.org/protobuf/proto"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-table-preparation"
	description.Features = append(description.Features, provider.PrepareTablesFeature)
	engine := description.Engines[0]
	engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == provider.CommonTableIdentityFeature })
	engine.Features = append(engine.Features, provider.PrepareTablesFeature)
	owner := &preparer{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner,
		Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, TablePreparer: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type preparer struct{ *backend.Provider }

func (*preparer) PrepareTables(ctx context.Context, request *protocol.PrepareTablesRequest) (*protocol.PreparedTables, error) {
	result := &protocol.PreparedTables{Complete: true}
	for _, input := range request.Tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		output := &protocol.PreparedTable{Subject: proto.CloneOf(input.Subject), ColumnPrimaryKeysPrepared: true,
			ColumnPrimaryKeys: slices.Clone(input.Desired.PrimaryKey)}
		for _, original := range input.Desired.Features.Facets.Records {
			if original.Value != nil && original.Subject.Kind == "table" {
				record := proto.CloneOf(original)
				record.Targets = nil
				output.ResolvedFacets = append(output.ResolvedFacets, record)
			}
		}
		if input.Desired.Name == "invalid" {
			output.ColumnPrimaryKeys = []string{"missing"}
		}
		result.Tables = append(result.Tables, output)
	}
	return result, nil
}
