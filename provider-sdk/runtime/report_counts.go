package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// ReportValues counts captured values offline. Both sides must list
// provider.ReportCountsFeature, and each value's model must declare metrics.
// The input is copied before dispatch; an invalid reply and cancellation
// expose no result.
func (c *Client) ReportValues(ctx context.Context, request *protocol.ReportValuesRequest) (*protocol.ValueReports, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "report values",
		provider.ValidateReportValuesRequest, provider.ValidateValueReports, c.rpc.ReportValues)
}

func (s *serverAdapter) ReportValues(ctx context.Context, request *protocol.ReportValuesRequest) (*protocol.ValueReports, error) {
	if nilService(s.server.ValueReporter) {
		return nil, unavailable("value reports")
	}
	return serveOffline(ctx, s, request, provider.ValidateReportValuesRequest, provider.ValidateValueReports, s.server.ValueReporter.ReportValues)
}
