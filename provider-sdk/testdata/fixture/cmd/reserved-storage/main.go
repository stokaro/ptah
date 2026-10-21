// Package main exercises reserved storage across the subprocess boundary.
package main

import (
	"context"
	"fmt"
	"os"
	"slices"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-reserved-storage"
	owner := &reservedProvider{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type reservedProvider struct{ *backend.Provider }

func (p *reservedProvider) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := p.Provider.Inspect(ctx, request)
	if err != nil {
		return nil, err
	}
	snapshot.Tables = slices.DeleteFunc(snapshot.Tables, func(table *protocol.Table) bool {
		// A synthetic marker and its complete layout identify fixture-owned storage.
		// A regular table with the same name remains a managed observation.
		if table.GetNormalizedIdentity().GetName() != "service_history" || len(table.GetColumns()) != 1 {
			return false
		}
		column := table.GetColumns()[0]
		if column.GetName() != "ptah_fixture_history_v1" || column.GetType() != "INTEGER" || column.GetNullable() || !column.GetPrimaryKey() || column.DefaultExpression != nil {
			return false
		}
		snapshot.ReservedObjects = append(snapshot.ReservedObjects, &protocol.ReservedObject{Object: proto.CloneOf(table.GetIdentity()), NormalizedObject: proto.CloneOf(table.GetNormalizedIdentity()),
			Purpose: protocol.ReservedObject_HISTORY, ConflictKinds: []string{"table"}})
		return true
	})
	return snapshot, nil
}
