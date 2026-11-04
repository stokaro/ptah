// Package main reads the typed arguments of an operator statement over RPC.
// It runs nothing: each statement returns one row per typed argument, which
// names the argument's scalar, whether it is optional and whether it is a
// list, and carries its value, or for a list its number of elements, so a
// process test sees what crossed the boundary.
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
	description := backend.Description()
	description.SemanticIdentity += "-sdk-typed-arguments"
	description.Features = append(description.Features, provider.StatementsFeature, provider.TypedArgumentsFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.StatementsFeature)
	owner := &echoOwner{Provider: &backend.Provider{}}
	if err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, Statements: owner}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type echoOwner struct {
	*backend.Provider
}

// RunStatement returns the typed arguments it received as rows.
func (*echoOwner) RunStatement(ctx context.Context, request *protocol.RunStatementRequest) (*protocol.StatementResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := &protocol.StatementResult{RequestId: request.GetRequestId(), Outcome: protocol.StatementResult_SUCCEEDED,
		Columns: []*protocol.DataColumn{{Name: "scalar"}, {Name: "optional"}, {Name: "list"}, {Name: "value"}}}
	for _, argument := range request.GetTypedArguments() {
		value := proto.CloneOf(argument.GetValue())
		if argument.GetType().GetList() {
			value = &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: int64(len(argument.GetElements()))}}
		}
		result.Rows = append(result.Rows, &protocol.Row{Values: []*protocol.DataValue{
			{Value: &protocol.DataValue_StringValue{StringValue: argument.GetType().GetScalar().String()}},
			{Value: &protocol.DataValue_BoolValue{BoolValue: argument.GetType().GetOptional()}},
			{Value: &protocol.DataValue_BoolValue{BoolValue: argument.GetType().GetList()}},
			value,
		}})
	}
	return result, nil
}

// ReadStatementRows has no open result to read: every result fits its reply.
func (*echoOwner) ReadStatementRows(context.Context, *protocol.ReadStatementRowsRequest) (*protocol.StatementRows, error) {
	return nil, fmt.Errorf("no open result for the page token")
}

// CloseStatement releases nothing, since no result stays open.
func (*echoOwner) CloseStatement(context.Context, *protocol.CloseStatementRequest) (*protocol.Empty, error) {
	return &protocol.Empty{}, nil
}
