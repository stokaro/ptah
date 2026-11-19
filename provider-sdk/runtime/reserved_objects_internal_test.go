package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a snapshot from a peer
// that is not the SDK server must reach the client's own checks.

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// reservedExchange is the inspection exchange of an engine that lists the
// reservation feature and its prerequisites, whose snapshot reserves one
// table for purpose.
func reservedExchange(feature string, prerequisites []string, purpose protocol.ReservedObject_Purpose) (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	description, request, snapshot := inspectionExchange()
	features := append(append([]string(nil), prerequisites...), feature)
	description.Features = append(description.Features, features...)
	description.Engines[0].Features = append(description.Engines[0].Features, features...)
	reference := &protocol.Reference{Engine: "test", Database: "db", Kind: "table", Name: "reserved"}
	snapshot.ReservedObjects = []*protocol.ReservedObject{{Object: reference, NormalizedObject: proto.CloneOf(reference), Purpose: purpose, ConflictKinds: []string{"table"}}}
	snapshot.TargetFeatures = []string{feature}
	return description, request, snapshot
}

// reservations are the reserved purposes a host receives only when it lists
// their feature, each with the features its engine lists beside it.
var reservations = []struct {
	name          string
	feature       string
	prerequisites []string
	purpose       protocol.ReservedObject_Purpose
	refusal       string
}{
	{"the seed ledger", provider.ReservedSeedsFeature, []string{provider.SeedsFeature}, protocol.ReservedObject_SEEDS,
		`.*a reserved seed ledger needs schema\.reserved-seeds`},
	{"the repeatable migration record", provider.ReservedRepeatableRecordFeature, []string{provider.HistoryFeature, provider.RepeatableRecordFeature},
		protocol.ReservedObject_REPEATABLE, `.*a reserved repeatable migration record needs schema\.reserved-repeatable-record`},
}

func TestInspectReservedObjectsConformance_HappyPath(t *testing.T) {
	for _, test := range reservations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, snapshot := reservedExchange(test.feature, test.prerequisites, test.purpose)
			server := negotiatedServer(Server{Description: description, Inspector: &unreadInspector{snapshot: snapshot}}, provider.TwoWayFeatures()...)
			client := &Client{description: server.negotiated(), rpc: &unreadLoopback{server: server}}

			result, err := client.Inspect(c.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(proto.Equal(result, snapshot), qt.IsTrue)
		})
	}
}

// A reservation passes only when both sides list its feature: the server
// refuses its owner's snapshot for a host that did not list it, and a client
// whose negotiated description lacks it refuses the same snapshot from a peer
// that is not the SDK server. The snapshot carries the target features of a
// session opened without the feature.
func TestInspectReservedObjectsConformance_FailurePath(t *testing.T) {
	for _, test := range reservations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, snapshot := reservedExchange(test.feature, test.prerequisites, test.purpose)
			snapshot.TargetFeatures = nil
			listed := withoutFeature(provider.TwoWayFeatures(), test.feature)
			server := negotiatedServer(Server{Description: description, Inspector: &unreadInspector{snapshot: snapshot}}, listed...)
			peer := &unreadLoopback{server: server}
			client := &Client{description: server.negotiated(), rpc: peer}

			served, serverErr := server.Inspect(c.Context(), request)
			peer.canned = snapshot
			received, clientErr := client.Inspect(c.Context(), request)

			c.Assert(server.negotiated().GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), test.feature)
			c.Assert(status.Code(serverErr), qt.Equals, codes.FailedPrecondition)
			c.Assert(status.Convert(serverErr).Message(), qt.Matches, test.refusal)
			c.Assert(served, qt.IsNil)
			c.Assert(clientErr, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(clientErr, qt.ErrorMatches, test.refusal)
			c.Assert(received, qt.IsNil)
		})
	}
}

// withoutFeature is features without feature.
func withoutFeature(features []string, feature string) []string {
	return slices.DeleteFunc(slices.Clone(features), func(listed string) bool { return listed == feature })
}
