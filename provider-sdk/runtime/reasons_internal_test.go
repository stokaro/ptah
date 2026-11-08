package runtime

// White-box testing required: the reasons check runs as the server's
// interceptor and in the client's private remoteError. A test drives each
// end alone over a real gRPC connection, to show that either one keeps a
// reason the host did not negotiate, or a malformed one, out of the host's
// error, and reads the raw status the server sent.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/anypb"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// failingInspector fails every inspection with err.
type failingInspector struct{ err error }

func (i failingInspector) Inspect(context.Context, *protocol.InspectRequest) (*protocol.Schema, error) {
	return nil, i.err
}

const (
	reasonText = "the streaming query capability was not established"
	secretText = "dial ydb://admin:hunter2@db:2136/local" // #nosec G101 -- a fake credential the host must never show
)

// reasoned is a status whose message quotes a credential and whose details
// carry the given diagnostics. Like regexp.MustCompile, it panics on a
// failure to marshal one, which a ProviderDiagnostic cannot have.
func reasoned(code codes.Code, diagnostics ...*protocol.ProviderDiagnostic) error {
	sent := status.New(code, secretText).Proto()
	for _, diagnostic := range diagnostics {
		detail, err := anypb.New(diagnostic)
		if err != nil {
			panic(err)
		}
		sent.Details = append(sent.Details, detail)
	}
	return status.FromProto(sent).Err()
}

// reasonsServer serves an inspector that fails with err. The provider lists
// the given features and the host lists hostFeatures. sdk selects the
// server Serve builds, with its reasons check, over a bare gRPC server.
type reasonsServer struct {
	features, hostFeatures []string
	err                    error
	sdk                    bool
}

// start returns the SDK client of the server and the raw gRPC client, which
// sees the status as the server sent it.
func (r reasonsServer) start(c *qt.C) (*Client, protocol.ProviderClient) {
	c.Helper()
	description := &protocol.Description{Features: append([]string{provider.InspectFeature}, r.features...)}
	adapter := negotiatedServer(Server{Description: description, Inspector: failingInspector{err: r.err}}, r.hostFeatures...)
	server := grpc.NewServer()
	if r.sdk {
		server = grpcServer(adapter)(nil)
	}
	protocol.RegisterProviderServer(server, adapter)
	listener := bufconn.Listen(1 << 20)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	c.Cleanup(func() {
		server.Stop()
		c.Check(listener.Close(), qt.IsNil)
		c.Check(<-done, qt.IsNil)
	})
	connection, err := grpc.NewClient("passthrough:///reasons", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(connection.Close(), qt.IsNil) })
	rpc := protocol.NewProviderClient(connection)
	return &Client{description: adapter.negotiated(), rpc: rpc}, rpc
}

var reasonsBoth = []string{provider.ErrorReasonsFeature}

// When the provider and the host both list the feature, the host's error
// carries the provider's reason and nothing else of the status, whichever
// server sent it.
func TestErrorReasons_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "a reason the provider wrote",
			err:  ErrorWithReason(codes.FailedPrecondition, reasonText),
			want: "provider inspect failed (FailedPrecondition): " + reasonText},
		{name: "a reason beside a message that quotes a credential",
			err:  reasoned(codes.Unavailable, &protocol.ProviderDiagnostic{Reason: "the database refused the connection"}),
			want: "provider inspect failed (Unavailable): the database refused the connection"},
		{name: "a reason the provider wrapped",
			err:  fmt.Errorf("%s: %w", secretText, ErrorWithReason(codes.InvalidArgument, reasonText)),
			want: "provider inspect failed (InvalidArgument): " + reasonText},
	}
	for _, test := range tests {
		for _, sdk := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s, SDK server %t", test.name, sdk), func(t *testing.T) {
				c := qt.New(t)
				client, _ := reasonsServer{features: reasonsBoth, hostFeatures: reasonsBoth, err: test.err, sdk: sdk}.start(c)

				schema, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: "session"})

				c.Assert(schema, qt.IsNil)
				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Equals, test.want)
			})
		}
	}
}

// Without the feature on both sides, a status's text stays out of the
// host's error, and a reason the host would show must be one valid
// diagnostic: the SDK server and the client each enforce it alone.
func TestErrorReasons_FailurePath(t *testing.T) {
	tests := []struct {
		name                   string
		features, hostFeatures []string
		err                    error
		want                   string
	}{
		{name: "a host that does not list it", features: reasonsBoth,
			err:  ErrorWithReason(codes.FailedPrecondition, reasonText),
			want: `provider inspect failed \(FailedPrecondition\)`},
		{name: "a provider that does not list it", hostFeatures: reasonsBoth,
			err:  ErrorWithReason(codes.FailedPrecondition, reasonText),
			want: `provider inspect failed \(FailedPrecondition\)`},
		{name: "an error that is not a status", features: reasonsBoth, hostFeatures: reasonsBoth,
			err:  errors.New(secretText),
			want: `provider inspect failed \(Unknown\)`},
		{name: "a status without a reason", features: reasonsBoth, hostFeatures: reasonsBoth,
			err:  status.Error(codes.InvalidArgument, secretText),
			want: `provider inspect failed \(InvalidArgument\)`},
		{name: "a reason of two lines", features: reasonsBoth, hostFeatures: reasonsBoth,
			err:  ErrorWithReason(codes.FailedPrecondition, "dial failed:\n"+secretText),
			want: `provider inspect failed \(FailedPrecondition\): the provider's reason is malformed and not shown`},
		{name: "an empty reason", features: reasonsBoth, hostFeatures: reasonsBoth,
			err:  reasoned(codes.Internal, &protocol.ProviderDiagnostic{}),
			want: `provider inspect failed \(Internal\): the provider's reason is malformed and not shown`},
		{name: "two reasons", features: reasonsBoth, hostFeatures: reasonsBoth,
			err:  reasoned(codes.Aborted, &protocol.ProviderDiagnostic{Reason: reasonText}, &protocol.ProviderDiagnostic{Reason: "another"}),
			want: `provider inspect failed \(Aborted\): the provider's reason is malformed and not shown`},
		{name: "a canceled call", features: reasonsBoth, hostFeatures: reasonsBoth,
			err:  ErrorWithReason(codes.Canceled, reasonText),
			want: `context canceled`},
		{name: "a passed deadline", features: reasonsBoth, hostFeatures: reasonsBoth,
			err:  ErrorWithReason(codes.DeadlineExceeded, reasonText),
			want: `context deadline exceeded`},
	}
	for _, test := range tests {
		for _, sdk := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s, SDK server %t", test.name, sdk), func(t *testing.T) {
				c := qt.New(t)
				client, _ := reasonsServer{features: test.features, hostFeatures: test.hostFeatures, err: test.err, sdk: sdk}.start(c)

				schema, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: "session"})

				c.Assert(schema, qt.IsNil)
				c.Assert(err, qt.ErrorMatches, test.want)
			})
		}
	}
}

// emptyDetail names the detail that is not a diagnostic.
const emptyDetail = "ptah.provider.experimental.Empty"

// details returns an Empty detail followed by one diagnostic per reason, in
// the order the server sends them.
func details(c *qt.C, reasons ...string) []*anypb.Any {
	c.Helper()
	other, err := anypb.New(&protocol.Empty{})
	c.Assert(err, qt.IsNil)
	result := []*anypb.Any{other}
	for _, reason := range reasons {
		detail, err := anypb.New(&protocol.ProviderDiagnostic{Reason: reason})
		c.Assert(err, qt.IsNil)
		result = append(result, detail)
	}
	return result
}

// detailTexts names each detail of the status err carries: a diagnostic by
// its reason, anything else by its type. A failed comparison of the names
// prints a few lines, where one of the protobuf values printed through
// quicktest's formatter walks every message's reflection state.
func detailTexts(c *qt.C, err error) []string {
	c.Helper()
	var texts []string
	for _, detail := range status.Convert(err).Proto().GetDetails() {
		diagnostic := &protocol.ProviderDiagnostic{}
		if !detail.MessageIs(diagnostic) {
			texts = append(texts, string(detail.MessageName()))
			continue
		}
		c.Assert(detail.UnmarshalTo(diagnostic), qt.IsNil)
		texts = append(texts, "reason: "+diagnostic.GetReason())
	}
	return texts
}

// The SDK server sends a reason only to a host that negotiated it, and its
// own sentence in place of a malformed one or of several. It keeps the
// code, the message and every other detail.
func TestServerSendsOnlyNegotiatedReasons(t *testing.T) {
	tests := []struct {
		name         string
		hostFeatures []string
		reasons      []string
		want         []string
	}{
		{name: "a negotiated reason", hostFeatures: reasonsBoth, reasons: []string{reasonText}, want: []string{emptyDetail, "reason: " + reasonText}},
		{name: "a reason the host did not list", reasons: []string{reasonText}, want: []string{emptyDetail}},
		{name: "a malformed reason", hostFeatures: reasonsBoth, reasons: []string{reasonText + "\n"}, want: []string{emptyDetail, "reason: " + malformedReason}},
		{name: "two reasons", hostFeatures: reasonsBoth, reasons: []string{reasonText, reasonText}, want: []string{emptyDetail, "reason: " + malformedReason}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sent := status.New(codes.FailedPrecondition, secretText).Proto()
			sent.Details = details(c, test.reasons...)
			_, rpc := reasonsServer{features: reasonsBoth, hostFeatures: test.hostFeatures, err: status.FromProto(sent).Err(), sdk: true}.start(c)

			_, err := rpc.Inspect(c.Context(), &protocol.InspectRequest{SessionId: "session"})

			c.Assert(status.Code(err), qt.Equals, codes.FailedPrecondition)
			c.Assert(status.Convert(err).Message(), qt.Equals, secretText)
			c.Assert(detailTexts(c, err), qt.DeepEquals, test.want)
		})
	}
}

// An error without a diagnostic leaves the server as the owner wrote it.
func TestServerLeavesAnErrorWithoutAReason(t *testing.T) {
	c := qt.New(t)
	_, rpc := reasonsServer{features: reasonsBoth, hostFeatures: reasonsBoth, err: status.Error(codes.NotFound, "no such table"), sdk: true}.start(c)
	_, err := rpc.Inspect(c.Context(), &protocol.InspectRequest{SessionId: "session"})
	c.Assert(status.Code(err), qt.Equals, codes.NotFound)
	c.Assert(status.Convert(err).Message(), qt.Equals, "no such table")
	c.Assert(detailTexts(c, err), qt.HasLen, 0)
}

// ErrorWithReason follows status.Error: codes.OK fails nothing.
func TestErrorWithReasonOfOK(t *testing.T) {
	c := qt.New(t)
	c.Assert(ErrorWithReason(codes.OK, reasonText), qt.IsNil)
	err := ErrorWithReason(codes.Unavailable, reasonText)
	c.Assert(status.Code(err), qt.Equals, codes.Unavailable)
	c.Assert(status.Convert(err).Message(), qt.Equals, reasonText)
}

func reasonedLockReceipt(outcome protocol.LockResult_Outcome, reason string) *protocol.LockResult {
	result := testLockReceipt(outcome)
	result.Diagnostic = &protocol.ProviderDiagnostic{Reason: reason}
	return result
}

// The client reads a lock receipt's reason only under the negotiated
// feature, and returns no diagnostic to its caller otherwise.
func TestLockClientReasons(t *testing.T) {
	tests := []struct {
		name       string
		features   []string
		want       string
		wantReason string
	}{
		{name: "negotiated", features: []string{provider.LocksFeature, provider.ErrorReasonsFeature},
			want: "provider lock outcome is BUSY: another session holds the lock node", wantReason: "another session holds the lock node"},
		{name: "not negotiated", features: []string{provider.LocksFeature},
			want: "provider lock outcome is BUSY", wantReason: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := &lockService{result: reasonedLockReceipt(protocol.LockResult_BUSY, "another session holds the lock node")}
			client := &Client{rpc: &lockPeer{owner: owner}, description: &protocol.Description{Features: test.features}}

			result, err := client.TryLock(c.Context(), testLockRequest())

			c.Assert(err, qt.ErrorIs, provider.ErrLockBusy)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result.GetOutcome(), qt.Equals, protocol.LockResult_BUSY)
			c.Assert(result.GetDiagnostic().GetReason(), qt.Equals, test.wantReason)
			c.Assert(owner.result.GetDiagnostic().GetReason(), qt.Equals, "another session holds the lock node")
		})
	}
}

// The SDK server sends a lock receipt's reason only to a host that
// negotiated it, and refuses a receipt whose reason it would send malformed.
func TestLockServerReasons(t *testing.T) {
	description := &protocol.Description{Features: []string{provider.LocksFeature, provider.ErrorReasonsFeature}}
	t.Run("negotiated", func(t *testing.T) {
		c := qt.New(t)
		server := negotiatedServer(Server{Description: description, Locks: &lockService{result: reasonedLockReceipt(protocol.LockResult_BUSY, reasonText)}}, reasonsBoth...)
		result, err := server.TryLock(c.Context(), testLockRequest())
		c.Assert(err, qt.IsNil)
		c.Assert(result.GetDiagnostic().GetReason(), qt.Equals, reasonText)
	})
	t.Run("not negotiated", func(t *testing.T) {
		c := qt.New(t)
		owner := &lockService{result: reasonedLockReceipt(protocol.LockResult_BUSY, reasonText+"\n")}
		server := negotiatedServer(Server{Description: description, Locks: owner})
		result, err := server.TryLock(c.Context(), testLockRequest())
		c.Assert(err, qt.IsNil)
		c.Assert(result.GetOutcome(), qt.Equals, protocol.LockResult_BUSY)
		c.Assert(result.GetDiagnostic(), qt.IsNil)
		c.Assert(owner.result.GetDiagnostic(), qt.IsNotNil)
	})
	t.Run("malformed", func(t *testing.T) {
		c := qt.New(t)
		owner := &lockService{result: reasonedLockReceipt(protocol.LockResult_LOST, reasonText+"\n"), releaseOutcome: protocol.LockResult_NOT_ACQUIRED}
		server := negotiatedServer(Server{Description: description, Locks: owner}, reasonsBoth...)
		result, err := server.CheckLock(c.Context(), testLockRequest())
		c.Assert(status.Code(err), qt.Equals, codes.Internal)
		c.Assert(status.Convert(err).Message(), qt.Equals, "provider returned an invalid lock receipt")
		c.Assert(result, qt.IsNil)
	})
}
