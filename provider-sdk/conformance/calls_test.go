package conformance_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/conformance"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// callName is the offline call the tests in this file add.
const callName = "prepare-common-objects"

// withNegotiatedCall is memoryConfig with an offline call whose request needs
// two-way features: PrepareCommonObjects, which a host that does not list
// schema.common-objects and schema.common-namespaces has no wire for. The
// SDK's server refuses the request for such a host before the provider sees
// it.
func withNegotiatedCall(target conformance.Target, name string) conformance.Config {
	config := memoryConfig(target, name)
	config.Calls = append(config.Calls, conformance.Call{Name: callName, Request: &protocol.PrepareCommonObjectsRequest{
		Engine: memoryEngine, Identifiers: &protocol.ComparisonIdentifiers{Rules: memoryRules()},
	}})
	return config
}

// A call of a negotiated service passes: under the hosts that did not list
// its features the refusal is the SDK server's, which the suite does not
// hold against the provider.
func TestCheck_NegotiatedCall_HappyPath(t *testing.T) {
	t.Run("in process", func(t *testing.T) {
		c := qt.New(t)
		target := conformance.InProcess(newMemoryProvider(memoryDefects{}).server())

		report, err := conformance.Check(c.Context(), withNegotiatedCall(target, "negotiated"))

		c.Assert(err, qt.IsNil)
		c.Assert(report.Err(), qt.IsNil)
		c.Assert(statuses(report)["calls."+callName], qt.Equals, conformance.Passed)
	})
	t.Run("a wire service", func(t *testing.T) {
		c := qt.New(t)
		target := conformance.Wire(wireTarget(c, newMemoryProvider(memoryDefects{}), wireDefects{}))

		report, err := conformance.Check(c.Context(), withNegotiatedCall(target, "negotiated"))

		c.Assert(err, qt.IsNil)
		c.Assert(report.Err(), qt.IsNil)
		c.Assert(statuses(report)["calls."+callName], qt.Equals, conformance.Passed)
	})
}

// A refusal or a reply the provider wrote still fails the call. An
// incomplete reply is the provider's, and the SDK's server refuses it; a
// protocol refusal in the provider's own words looks like the server's but
// is not its text.
func TestCheck_NegotiatedCall_FailurePath(t *testing.T) {
	t.Run("an incomplete reply", func(t *testing.T) {
		c := qt.New(t)
		target := conformance.InProcess(newMemoryProvider(memoryDefects{prepareIncomplete: true}).server())

		report, err := conformance.Check(c.Context(), withNegotiatedCall(target, "broken"))

		c.Assert(err, qt.IsNil)
		c.Assert(failed(report), qt.DeepEquals, []string{"calls." + callName}, qt.Commentf("%v", report.Err()))
	})
	t.Run("a protocol refusal of the provider's own", func(t *testing.T) {
		c := qt.New(t)
		target := conformance.Wire(wireTarget(c, newMemoryProvider(memoryDefects{}), wireDefects{ownProtocolRefusal: true}))

		report, err := conformance.Check(c.Context(), withNegotiatedCall(target, "broken"))

		c.Assert(err, qt.IsNil)
		c.Assert(failed(report), qt.DeepEquals, []string{"calls." + callName}, qt.Commentf("%v", report.Err()))
		c.Assert(report.Err(), qt.ErrorMatches, `(?s).*PrepareCommonObjects failed under .*the wire provider does not prepare common objects for this host.*`)
	})
}
