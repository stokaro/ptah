package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// SplitLintStatements splits migration sources offline. Invalid replies and
// cancellation expose no statements.
func (c *Client) SplitLintStatements(ctx context.Context, request *protocol.SplitLintRequest) (*protocol.SplitLint, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "split lint statements", provider.ValidateSplitLintRequest, provider.ValidateSplitLint, c.rpc.SplitLintStatements)
}

// AnnotateLintStatements asks the owner for facts of statements the host split.
func (c *Client) AnnotateLintStatements(ctx context.Context, request *protocol.AnnotateLintRequest) (*protocol.AnnotatedLint, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "annotate lint statements", provider.ValidateAnnotateLintRequest, provider.ValidateAnnotatedLint, c.rpc.AnnotateLintStatements)
}

// CheckLintMigrations returns the findings of the enabled rules, or an error
// and no finding.
func (c *Client) CheckLintMigrations(ctx context.Context, request *protocol.CheckLintRequest) (*protocol.CheckedLint, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "check lint migrations", provider.ValidateCheckLintRequest, provider.ValidateCheckedLint, c.rpc.CheckLintMigrations)
}

func (s *serverAdapter) SplitLintStatements(ctx context.Context, request *protocol.SplitLintRequest) (*protocol.SplitLint, error) {
	if s.server.Linter == nil {
		return nil, status.Error(codes.Unimplemented, "lint is unavailable")
	}
	return serveOffline(ctx, s.server.Description, request, provider.ValidateSplitLintRequest, provider.ValidateSplitLint, s.server.Linter.SplitLintStatements)
}

func (s *serverAdapter) AnnotateLintStatements(ctx context.Context, request *protocol.AnnotateLintRequest) (*protocol.AnnotatedLint, error) {
	if s.server.Linter == nil {
		return nil, status.Error(codes.Unimplemented, "lint is unavailable")
	}
	return serveOffline(ctx, s.server.Description, request, provider.ValidateAnnotateLintRequest, provider.ValidateAnnotatedLint, s.server.Linter.AnnotateLintStatements)
}

func (s *serverAdapter) CheckLintMigrations(ctx context.Context, request *protocol.CheckLintRequest) (*protocol.CheckedLint, error) {
	if s.server.Linter == nil {
		return nil, status.Error(codes.Unimplemented, "lint is unavailable")
	}
	return serveOffline(ctx, s.server.Description, request, provider.ValidateCheckLintRequest, provider.ValidateCheckedLint, s.server.Linter.CheckLintMigrations)
}
