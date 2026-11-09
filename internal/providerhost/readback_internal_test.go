package providerhost

// White-box testing required: the read-back service is part of the target
// registration the host assembles from the negotiated description, below any
// command, and the commands that read it compare against a live engine whose
// read reports index methods.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// An engine that spells a declared index's kind as it reports a held one
// reads an index declared with a kind back with that kind as its method; an
// engine that does not registers no read-back service.
func TestRegistrationReadsAnIndexKindBackAsItsMethod(t *testing.T) {
	c := qt.New(t)
	features := []string{provider.ObservedIndexesFeature, provider.IndexMethodsFeature}
	host := &Host{description: &protocol.Description{Features: features}}
	methods := (&target{host: host, engine: &protocol.Engine{Id: "fixture", Features: features}}).registration()
	c.Assert(methods.ReadBack, qt.IsNotNil)
	method, read := methods.ReadBack.IndexMethod("async")
	c.Assert(read, qt.IsTrue)
	c.Assert(method, qt.Equals, "async")
	_, read = methods.ReadBack.IndexMethod("")
	c.Assert(read, qt.IsFalse)
	_, retyped := methods.ReadBack.ColumnType("Int64")
	c.Assert(retyped, qt.IsFalse)

	plain := (&target{host: &Host{description: &protocol.Description{}}, engine: &protocol.Engine{Id: "fixture"}}).registration()
	c.Assert(plain.ReadBack, qt.IsNil)
}
