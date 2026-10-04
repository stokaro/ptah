package ydb

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ticketHeader is the gRPC metadata key a YDB client presents its credential
// under: the token a static user's login returned, an access token as it was
// given, or the token an environment credential produced.
const ticketHeader = "x-ydb-auth-ticket"

// ticketProbe holds the credential the call Ticket makes carried.
type ticketProbe struct {
	ticket string
}

// ticketProbeKey is the context key a ticketProbe travels under.
type ticketProbeKey struct{}

// recordTicket is a unary interceptor on every connection Open makes. It reads
// the credential only from a call whose context carries a ticketProbe, which
// only Ticket's call does, and passes every call on unchanged.
func recordTicket(
	ctx context.Context,
	method string,
	request, reply any,
	conn *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	options ...grpc.CallOption,
) error {
	if probe, ok := ctx.Value(ticketProbeKey{}).(*ticketProbe); ok {
		if outgoing, ok := metadata.FromOutgoingContext(ctx); ok {
			if values := outgoing.Get(ticketHeader); len(values) > 0 {
				probe.ticket = values[len(values)-1]
			}
		}
	}
	return invoker(ctx, method, request, reply, conn, options...)
}

// Ticket returns the credential the connection presents to the server, as the
// server receives it, or "" for a connection the URL gives no credential.
//
// The YDB SDK builds the credential inside the driver and never hands it out:
// a static user's token comes from a login the driver makes, and an
// environment credential from a provider it builds. So Ticket asks the server
// who the connection is, and reads the credential the SDK attached to that
// call. The server's monitoring endpoint reads the same credential from an
// Authorization header, which is what lets Ptah read a cluster's feature
// flags where the cluster enforces authentication.
//
// The ticket is a secret. A caller sends it and never prints it.
func (c *Connection) Ticket(ctx context.Context) (string, error) {
	if !c.authenticated {
		return "", nil
	}
	probe := &ticketProbe{}
	if _, err := c.Driver.Discovery().WhoAmI(context.WithValue(ctx, ticketProbeKey{}, probe)); err != nil {
		return "", fmt.Errorf("ask YDB which user the connection is: %w", WithoutStackFrames(err))
	}
	return probe.ticket, nil
}
