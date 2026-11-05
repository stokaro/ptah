// Package main exercises offline common-column ownership through a subprocess.
package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-column-semantics"
	features := []string{provider.ObservedColumnsFeature, provider.ObservedIndexesFeature, provider.ColumnsFeature, provider.ProjectTableCreationsFeature, provider.PrepareTablesFeature, provider.ValidateSchemaFeature}
	// FIXTURE_INDEX_METHODS=off leaves out the prepared index kinds, so a
	// test can show what a provider without them gets.
	if os.Getenv("FIXTURE_INDEX_METHODS") != "off" {
		features = append(features, provider.IndexMethodsFeature)
	}
	// FIXTURE_INDEX_FACETS=off leaves out schema.observed-index-facets, while
	// the read still reports a facet on the index of table index_facet, so a
	// test can show that the host refuses what it did not negotiate.
	if os.Getenv("FIXTURE_INDEX_FACETS") != "off" {
		features = append(features, provider.ObservedIndexFacetsFeature)
	}
	description.Features = append(description.Features, features...)
	engine := description.Engines[0]
	engine.Aliases = append(engine.Aliases, "fixture-columns")
	engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == provider.CommonTableIdentityFeature })
	engine.Features = append(engine.Features, features...)
	owner := &columns{Provider: &backend.Provider{}, description: description}
	for _, model := range engine.ValueModels {
		if model.GetKind() == "example.org/fixture/retention" && model.GetRepresentation() == protocol.ValueInput_OBSERVED {
			owner.retention = model
		}
	}
	if err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, SchemaValidator: owner, TablePreparer: owner, TableCreations: owner, Columns: owner}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type columns struct {
	*backend.Provider
	// description is the provider's own, which a session negotiates with the
	// host, and retention the observed model of the facet an index carries.
	description *protocol.Description
	retention   *protocol.ValueModel
}

func (*columns) ValidateSchema(ctx context.Context, _ *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &protocol.SchemaValidation{Complete: true}, nil
}

func (*columns) PrepareTables(ctx context.Context, request *protocol.PrepareTablesRequest) (*protocol.PreparedTables, error) {
	methods := slices.Contains(request.TargetFeatures, provider.IndexMethodsFeature)
	result := &protocol.PreparedTables{Complete: true}
	for _, table := range request.Tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		prepared := &protocol.PreparedTable{Subject: proto.CloneOf(table.Subject), ColumnPrimaryKeysPrepared: true, ColumnPrimaryKeys: keys(table.Desired)}
		if methods {
			indexes, err := preparedIndexes(table.Desired)
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
			prepared.Indexes = indexes
		}
		result.Tables = append(result.Tables, prepared)
	}
	return result, nil
}

// preparedIndexes states the kind of each index table declares, in order.
func preparedIndexes(table *protocol.DeclaredTable) ([]*protocol.PreparedIndex, error) {
	result := make([]*protocol.PreparedIndex, 0, len(table.Indexes))
	for _, index := range table.Indexes {
		method, err := indexMethod(index.Type)
		if err != nil {
			return nil, fmt.Errorf("index %q: %w", index.Name, err)
		}
		result = append(result, &protocol.PreparedIndex{Name: index.Name, Method: method})
	}
	return result, nil
}

// indexMethod reads a declared index's type as the kind inspection reports a
// held index's method with: global for a synchronous index, async for an
// asynchronous one, in any case.
func indexMethod(declared string) (string, error) {
	switch strings.ToUpper(declared) {
	case "", "GLOBAL", "SYNC", "GLOBAL SYNC":
		return "global", nil
	case "ASYNC", "GLOBAL ASYNC":
		return "async", nil
	default:
		return "", fmt.Errorf("the fixture has no index kind %q", declared)
	}
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
