package runtime

// White-box testing required: shared coverage lives in the client's
// interceptor and the server's, which a test reaches only over a real gRPC
// connection, and only an interceptor between the SDK's and the connection
// sees the message as it travels; a server that answers without the SDK's
// checks is the only way to send the client a malformed reply.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// sharedCoveragePlanning is runtimePlanningExchange with a second table, and
// both tables' facet coverages naming the engine's models by one kind message
// for each representation, as a host builds them. The provider lists the
// feature.
func sharedCoveragePlanning() (*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures) {
	d, request, reply := runtimePlanningExchange()
	d.Features = append(d.Features, provider.SharedCoverageFeature)
	complete := []*protocol.ComparisonKnowledgeSource{{Outside: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}
	desired := &protocol.ComparisonKindCoverage{Model: proto.CloneOf(d.Engines[0].ValueModels[0]), Sources: complete}
	observed := &protocol.ComparisonKindCoverage{Model: proto.CloneOf(d.Engines[0].ValueModels[1]), Sources: complete}
	second := proto.CloneOf(request.Tables[0])
	second.Subject.Name = &protocol.ComparisonPart{Source: "kept", Normalized: "kept"}
	second.Desired.Name, second.Current.Name = "kept", "kept"
	request.Tables = append(request.Tables, second)
	for _, table := range request.Tables {
		table.Desired.Features.Facets.Coverage.Kinds = []*protocol.ComparisonKindCoverage{desired}
		table.Current.Features.Facets.Coverage.Kinds = []*protocol.ComparisonKindCoverage{observed}
	}
	receipt := proto.CloneOf(reply.Parents[0])
	receipt.Receipt.Subject = proto.CloneOf(second.Subject)
	reply.Parents = append(reply.Parents, receipt)
	return d, request, reply
}

// recordingPlanner answers every plan with reply and keeps each request it is
// given.
type recordingPlanner struct {
	reply    *protocol.PlannedFeatures
	requests []*protocol.PlanFeaturesRequest
}

func (p *recordingPlanner) PlanFeatures(_ context.Context, request *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error) {
	p.requests = append(p.requests, request)
	return proto.CloneOf(p.reply), nil
}

// desiredKinds returns the kinds of each table's desired facet coverage.
func desiredKinds(request *protocol.PlanFeaturesRequest) [][]*protocol.ComparisonKindCoverage {
	var kinds [][]*protocol.ComparisonKindCoverage
	for _, table := range request.GetTables() {
		kinds = append(kinds, table.GetDesired().GetFeatures().GetFacets().GetCoverage().GetKinds())
	}
	return kinds
}

func TestSharedCoverage_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name         string
		hostFeatures []string
		// shared reports whether the second table's kinds travel as the
		// first table's key, and the planner reads one kind message for
		// both.
		shared bool
	}{
		{name: "both list the feature", hostFeatures: []string{provider.SharedCoverageFeature}, shared: true},
		{name: "the host does not list it", shared: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, reply := sharedCoveragePlanning()
			planner := &recordingPlanner{reply: reply}
			server := negotiatedServer(Server{Description: d, FeaturePlanner: planner}, test.hostFeatures...)
			client, wire := transportPeers(c, d, server, test.hostFeatures...)
			before := proto.CloneOf(request)

			result, err := client.PlanFeatures(c.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(proto.Equal(result, reply), qt.IsTrue)
			c.Assert(proto.Equal(request, before), qt.IsTrue)
			sent := desiredKinds(wire.requests[0].(*protocol.PlanFeaturesRequest))
			c.Assert(len(sent[1]) == 0, qt.Equals, test.shared)
			c.Assert(planner.requests, qt.HasLen, 1)
			c.Assert(proto.Equal(planner.requests[0], request), qt.IsTrue)
			read := desiredKinds(planner.requests[0])
			c.Assert(read[1][0] == read[0][0], qt.Equals, test.shared)
		})
	}
}

// A server refuses a shared list it cannot resolve before any service reads
// it: a key no coverage of the request defines, and any key from a host that
// did not negotiate the feature.
func TestServerSharedCoverage_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name         string
		hostFeatures []string
	}{
		{name: "a key the request does not define", hostFeatures: []string{provider.SharedCoverageFeature}},
		{name: "a key without the feature"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, reply := sharedCoveragePlanning()
			planner := &recordingPlanner{reply: reply}
			client, _ := transportPeers(c, d, negotiatedServer(Server{Description: d, FeaturePlanner: planner}, test.hostFeatures...))
			coverage := request.Tables[1].Desired.Features.Facets.Coverage
			coverage.Kinds, coverage.KindsOf = nil, 9

			_, err := client.rpc.PlanFeatures(c.Context(), request)

			c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
			c.Assert(planner.requests, qt.HasLen, 0)
		})
	}
}

// comparingProvider answers CompareModels with reply, without the SDK's
// checks: a peer that does not keep the contract.
type comparingProvider struct {
	protocol.UnimplementedProviderServer
	reply *protocol.ComparedModels
}

func (p *comparingProvider) CompareModels(context.Context, *protocol.CompareModelsRequest) (*protocol.ComparedModels, error) {
	return proto.CloneOf(p.reply), nil
}

// A client refuses a reply whose shared list names no coverage of the reply.
func TestClientSharedCoverage_FailurePath(t *testing.T) {
	c := qt.New(t)
	d, request, reply := runtimeComparisonExchange()
	d.Features = append(d.Features, provider.SharedCoverageFeature)
	reply.Desired.Coverage = &protocol.ComparisonCoverage{KindsOf: 9}
	client, _ := transportPeers(c, d, &comparingProvider{reply: reply}, provider.SharedCoverageFeature)

	result, err := client.CompareModels(c.Context(), request)

	c.Assert(err, qt.ErrorMatches, `provider compare models failed \(Internal\)`)
	c.Assert(result, qt.IsNil)
}
