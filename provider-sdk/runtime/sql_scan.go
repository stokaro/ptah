package runtime

import (
	"context"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// ScanSQL returns the tokens of one text of the engine's SQL, offline. The
// provider and the engine must list provider.ScanSQLFeature. The input is
// copied before dispatch; an invalid reply and cancellation expose no token.
func (c *Client) ScanSQL(ctx context.Context, request *protocol.ScanSQLRequest) (*protocol.SQLScan, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "scan SQL",
		provider.ValidateScanSQLRequest, provider.ValidateSQLScanReply, c.rpc.ScanSQL)
}

func (s *serverAdapter) ScanSQL(ctx context.Context, request *protocol.ScanSQLRequest) (*protocol.SQLScan, error) {
	if nilService(s.server.SQLScanner) {
		return nil, unavailable("SQL scanning")
	}
	return serveOffline(ctx, s, request, provider.ValidateScanSQLRequest, provider.ValidateSQLScanReply, s.server.SQLScanner.ScanSQL)
}
