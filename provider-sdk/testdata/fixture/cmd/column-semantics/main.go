// Package main exercises offline common-column ownership through a subprocess.
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
	description.SemanticIdentity += "-column-semantics"
	features := []string{provider.ObservedColumnsFeature, provider.ColumnsFeature, provider.ProjectTableCreationsFeature, provider.PrepareTablesFeature, provider.ValidateSchemaFeature}
	description.Features = append(description.Features, features...)
	engine := description.Engines[0]
	engine.Aliases = append(engine.Aliases, "fixture-columns")
	engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == provider.CommonTableIdentityFeature })
	engine.Features = append(engine.Features, features...)
	owner := &columns{Provider: &backend.Provider{}}
	if err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, SchemaValidator: owner, TablePreparer: owner, TableCreations: owner, Columns: owner}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type columns struct{ *backend.Provider }

func (*columns) ValidateSchema(ctx context.Context, _ *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &protocol.SchemaValidation{Complete: true}, nil
}

func (*columns) PrepareTables(ctx context.Context, request *protocol.PrepareTablesRequest) (*protocol.PreparedTables, error) {
	result := &protocol.PreparedTables{Complete: true}
	for _, table := range request.Tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result.Tables = append(result.Tables, &protocol.PreparedTable{Subject: proto.CloneOf(table.Subject), ColumnPrimaryKeysPrepared: true, ColumnPrimaryKeys: keys(table.Desired)})
	}
	return result, nil
}

func keys(table *protocol.DeclaredTable) []string {
	result := slices.Clone(table.PrimaryKey)
	for _, column := range table.Columns {
		if column.Primary && !slices.Contains(result, column.Name) {
			result = append(result, column.Name)
		}
	}
	return result
}

func effectiveNullable(table *protocol.DeclaredTable, column *protocol.DeclaredColumn) bool {
	// A second table shape proves the host accepts explicit true even when its
	// descriptor would infer NOT NULL from primary-key membership.
	return table.Comment == "nullable keys" || column.Nullable && !slices.Contains(keys(table), column.Name)
}

func canonicalType(value string) string {
	if value == "BIGINT" {
		return "Int64"
	}
	return value
}
