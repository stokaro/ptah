package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a reply from a peer that
// is not the SDK server must reach the client's own checks.

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

// reportingDescription offers report counts on the normalization exchange's
// engine, whose model declares one metric.
func reportingDescription() *protocol.Description {
	d, _, _ := normalizationExchange()
	d.Features = append(d.Features, provider.ReportCountsFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.ReportCountsFeature)
	engine.ValueModels[0].DisplayName = "values"
	engine.ValueModels[0].Metrics = []*protocol.ModelMetric{{Name: "values", Help: "Values"}}
	return d
}

func reportingRequest() *protocol.ReportValuesRequest {
	_, inputs, _ := normalizationExchange()
	return &protocol.ReportValuesRequest{Engine: inputs.Engine, TargetFeatures: []string{provider.ReportCountsFeature}, Values: inputs.Values}
}

// countingOwner counts each value once, or answers with its canned reply. It
// counts its calls.
type countingOwner struct {
	calls int
	reply *protocol.ValueReports
}

func (o *countingOwner) ReportValues(_ context.Context, request *protocol.ReportValuesRequest) (*protocol.ValueReports, error) {
	o.calls++
	if o.reply != nil {
		return o.reply, nil
	}
	result := &protocol.ValueReports{Complete: true}
	for _, input := range request.Values {
		result.Values = append(result.Values, &protocol.ValueReport{Id: input.Id, Counts: []*protocol.MetricCount{{Name: "values", Value: 1}}})
	}
	return result, nil
}

// reportingLoopback forwards ReportValues to a server adapter, or answers with
// a canned reply when one is set, as a peer that is not the SDK server could.
type reportingLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
	canned *protocol.ValueReports
}

func (l *reportingLoopback) ReportValues(ctx context.Context, request *protocol.ReportValuesRequest, _ ...grpc.CallOption) (*protocol.ValueReports, error) {
	l.calls++
	if l.canned != nil {
		return l.canned, nil
	}
	return l.server.ReportValues(ctx, request)
}

func newReportingLoopback(description *protocol.Description, owner *countingOwner, hostFeatures ...string) (*Client, *reportingLoopback) {
	server := negotiatedServer(Server{Description: description, ValueReporter: owner}, hostFeatures...)
	peer := &reportingLoopback{server: server}
	return &Client{description: server.negotiated(), rpc: peer}, peer
}

func TestReportValuesConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &countingOwner{}
	client, peer := newReportingLoopback(reportingDescription(), owner, provider.ReportCountsFeature)
	request := reportingRequest()
	before := proto.CloneOf(request)

	reports, err := client.ReportValues(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(reports.Values, qt.HasLen, 1)
	c.Assert(reports.Values[0].Counts[0].Value, qt.Equals, uint64(1))
	c.Assert(peer.calls, qt.Equals, 1)
	c.Assert(owner.calls, qt.Equals, 1)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
}

// A host that does not list the feature gets a description without it: the
// client sends nothing, and the server refuses the call before its owner sees
// it.
func TestReportValuesNeedsTheHostsFeature(t *testing.T) {
	c := qt.New(t)
	owner := &countingOwner{}
	client, peer := newReportingLoopback(reportingDescription(), owner)

	reports, err := client.ReportValues(c.Context(), reportingRequest())
	c.Assert(err, qt.ErrorMatches, `.*report counts: the feature was not negotiated`)
	c.Assert(reports, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 0)

	reports, err = peer.server.ReportValues(c.Context(), reportingRequest())
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(reports, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 0)
}

// An invalid reply reaches no caller: the server refuses its owner's, and the
// client refuses one from a peer that is not the SDK server.
func TestReportValuesConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	invalid := &protocol.ValueReports{Complete: true, Values: []*protocol.ValueReport{{Id: "first"}}}
	owner := &countingOwner{reply: invalid}
	client, peer := newReportingLoopback(reportingDescription(), owner, provider.ReportCountsFeature)

	reports, err := peer.server.ReportValues(c.Context(), reportingRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(reports, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 1)

	peer.canned = invalid
	reports, err = client.ReportValues(c.Context(), reportingRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*report counts: a report leaves a metric out or counts one twice`)
	c.Assert(reports, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 1)
}

// A server that offers the feature needs the service, and one with the
// service lists the feature.
func TestReportValuesServerAdvertisement(t *testing.T) {
	c := qt.New(t)
	description := reportingDescription()
	description.Revision, description.Version, description.SemanticIdentity = provider.Revision, "1", "report-1"
	description.Engines[0].Identifiers = &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
		IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}
	c.Assert(Server{Description: description, Sessions: &sessionOwner{}, ValueNormalizer: &mutatingNormalizer{}}.Validate(), qt.ErrorMatches,
		`.*models\.report-counts advertisement differs from implementation`)
	c.Assert(Server{Description: description, Sessions: &sessionOwner{}, ValueNormalizer: &mutatingNormalizer{}, ValueReporter: &countingOwner{}}.Validate(), qt.IsNil)
}
