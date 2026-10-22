package runtime

// White-box testing required: malformed peer replies must bypass the SDK server,
// and owner mutation must be checked against the adapter's original capture.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func runtimeLintExchange() (*protocol.Description, *protocol.CheckLintRequest, *protocol.CheckedLint) {
	description, _, _ := normalizationExchange()
	description.Features = append(description.Features, provider.LintFeature)
	description.Engines[0].Features = []string{provider.LintFeature}
	description.Engines[0].LintRules = []*protocol.LintRule{{Code: "XY101", Title: "Drop loses data", Severity: protocol.LintRule_ERROR}}
	request := &protocol.CheckLintRequest{Engine: "test", Codes: []string{"XY101"}, Files: []*protocol.LintFile{{Name: "1.sql", Up: true,
		Statements: []*protocol.LintStatement{{Start: 0, End: 13, Line: 1, Sql: "DROP TABLE a;"}}}}}
	result := &protocol.CheckedLint{Complete: true, Findings: []*protocol.LintFinding{{Code: "XY101", Message: "drop loses data"}}}
	return description, request, result
}

func TestLintClientDiscardsUntrustedReply(t *testing.T) {
	c := qt.New(t)
	description, request, valid := runtimeLintExchange()
	peer := &lintPeer{result: proto.CloneOf(valid)}
	client := &Client{description: description, rpc: peer}
	result, err := client.CheckLintMigrations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.result.Complete = false
	result, err = client.CheckLintMigrations(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.result = proto.CloneOf(valid)
	peer.result.Findings[0].Code = "ZZ999"
	result, err = client.CheckLintMigrations(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	result, err = client.CheckLintMigrations(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `provider check lint migrations failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
	request.Codes = []string{"ZZ999"}
	result, err = client.CheckLintMigrations(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 4)
}

func TestLintServerPreservesEvidenceAndCancellation(t *testing.T) {
	c := qt.New(t)
	description, request, _ := runtimeLintExchange()
	original := proto.CloneOf(request)
	owner := &mutatingLinter{}
	server := &serverAdapter{server: Server{Description: description, Linter: owner}}
	result, err := server.CheckLintMigrations(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = server.CheckLintMigrations(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 1)
}

func TestLintServerIsUnimplementedWithoutAnOwner(t *testing.T) {
	c := qt.New(t)
	description, request, _ := runtimeLintExchange()
	server := &serverAdapter{server: Server{Description: description}}
	_, err := server.CheckLintMigrations(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = server.SplitLintStatements(c.Context(), &protocol.SplitLintRequest{})
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = server.AnnotateLintStatements(c.Context(), &protocol.AnnotateLintRequest{})
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
}

type lintPeer struct {
	protocol.ProviderClient
	result *protocol.CheckedLint
	err    error
	calls  int
}

func (p *lintPeer) CheckLintMigrations(context.Context, *protocol.CheckLintRequest, ...grpc.CallOption) (*protocol.CheckedLint, error) {
	p.calls++
	return p.result, p.err
}

type mutatingLinter struct {
	provider.Linter
	calls int
}

func (l *mutatingLinter) CheckLintMigrations(_ context.Context, request *protocol.CheckLintRequest) (*protocol.CheckedLint, error) {
	l.calls++
	request.Codes[0] = "ZZ999"
	return &protocol.CheckedLint{Complete: true, Findings: []*protocol.LintFinding{{Code: "ZZ999", Message: "forged"}}}, nil
}
