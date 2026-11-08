package conformance

// White-box testing required: the probe table must name every RPC of the
// generated service, and a report does not say which RPCs the suite probed,
// so a service added to the protocol without a probe would pass every
// black-box test while the suite never asks a provider about it.

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

func TestProbesCoverTheService(t *testing.T) {
	c := qt.New(t)
	var service []string
	for _, method := range protocol.Provider_ServiceDesc.Methods {
		service = append(service, method.MethodName)
	}
	service = slices.DeleteFunc(service, func(name string) bool { return name == "Describe" })
	var probed []string
	for _, entry := range probes() {
		probed = append(probed, entry.method)
	}
	slices.Sort(service)
	slices.Sort(probed)

	c.Assert(probed, qt.DeepEquals, service)
}

// An offline call and a live call each name the RPC of their request type
// once, and only offline services take a Call.
func TestRPCTablesNameEachRequestOnce(t *testing.T) {
	c := qt.New(t)
	var names []string
	for _, entry := range append(offlineRPCs(), liveRPCs()...) {
		names = append(names, string(entry.request))
	}
	sorted := slices.Clone(names)
	slices.Sort(sorted)

	c.Assert(slices.Compact(slices.Clone(sorted)), qt.HasLen, len(names))
	c.Assert(offlineRPCFor(&protocol.InspectRequest{}), qt.IsNil)
	c.Assert(offlineRPCFor(&protocol.RenderRequest{}).method, qt.Equals, "Render")
	c.Assert(liveRPCFor(&protocol.ReadRowsRequest{}).method, qt.Equals, "ReadRows")
}
