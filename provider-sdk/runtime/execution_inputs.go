package runtime

import (
	"context"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// DescribeExecutionInputs discovers references before plan approval. It never
// resolves input values; invalid or canceled discovery exposes no prefix.
func (c *Client) DescribeExecutionInputs(ctx context.Context, request *protocol.DescribeExecutionInputsRequest) (*protocol.ExecutionInputs, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "describe execution inputs", provider.ValidateExecutionInputsRequest, provider.ValidateExecutionInputs, c.rpc.DescribeExecutionInputs)
}

func (s *serverAdapter) DescribeExecutionInputs(ctx context.Context, request *protocol.DescribeExecutionInputsRequest) (*protocol.ExecutionInputs, error) {
	if nilService(s.server.ExecutionInputs) {
		return nil, unavailable("execution input discovery")
	}
	return serveOffline(ctx, s, request, provider.ValidateExecutionInputsRequest, provider.ValidateExecutionInputs, s.server.ExecutionInputs.DescribeExecutionInputs)
}

func (s *serverAdapter) checkExecutionInputs(ctx context.Context, request *protocol.ExecuteRequest) error {
	if !slices.Contains(s.negotiated().GetFeatures(), provider.SensitiveInputsFeature) {
		return nil
	}
	for _, engine := range s.negotiated().GetEngines() {
		if engine.GetId() == request.GetEngine() && !slices.Contains(engine.GetFeatures(), provider.SensitiveInputsFeature) {
			return nil
		}
	}
	result, err := s.DescribeExecutionInputs(ctx, &protocol.DescribeExecutionInputsRequest{Engine: request.GetEngine(), Units: request.GetUnits()})
	if err != nil {
		return err
	}
	if err := provider.CheckExecutionInputs(request.GetExpectedInputs(), result.GetInputs()); err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return nil
}
