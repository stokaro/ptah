package providerhost

// White-box testing required: target is private, and the subprocess tests in
// integration/providercli reach this contract only with the engine's own facts,
// so a request that carries other facts cannot be built from outside.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaprojection"
)

// TestCommonTableCreations_TakeTheEnginesOwnFacts_HappyPath holds the facts the
// conversion of a source sends: what capability.ForDialect answers for the
// engine, which is nothing until a provider or its first session states some.
func TestCommonTableCreations_TakeTheEnginesOwnFacts_HappyPath(t *testing.T) {
	c := qt.New(t)
	const stated = "creation-facts-stated"
	c.Assert(capability.RegisterDefaults(capability.Postgres18(), stated), qt.IsNil)

	for _, engine := range []string{"creation-facts-none", stated} {
		t.Run(engine, func(t *testing.T) {
			c := qt.New(t)
			selected := &target{engine: &protocol.Engine{Id: engine}}

			got, err := selected.ProjectTableCreations(t.Context(), schemaprojection.TableCreationRequest{
				Target: engine, Capabilities: capability.ForDialect(engine),
			})

			c.Assert(err, qt.IsNil)
			c.Assert(got.Complete, qt.IsTrue)
		})
	}
}

// TestCommonTableCreations_RefuseFactsOtherThanTheEnginesOwn_FailurePath holds
// the refusal the common contract owes a request it could not honor: it answers
// from the declaration alone, so another engine's facts would be ignored.
func TestCommonTableCreations_RefuseFactsOtherThanTheEnginesOwn_FailurePath(t *testing.T) {
	const stated = "creation-facts-other"
	qt.New(t).Assert(capability.RegisterDefaults(capability.Postgres18(), stated), qt.IsNil)

	tests := []struct {
		name    string
		engine  string
		request schemaprojection.TableCreationRequest
	}{
		{
			name:    "facts on an engine that has none",
			engine:  "creation-facts-absent",
			request: schemaprojection.TableCreationRequest{Target: "creation-facts-absent", Capabilities: capability.Postgres18()},
		},
		{
			name:    "facts other than the ones the engine states",
			engine:  stated,
			request: schemaprojection.TableCreationRequest{Target: stated, Capabilities: capability.SQLite3()},
		},
		{
			name:    "another target",
			engine:  stated,
			request: schemaprojection.TableCreationRequest{Target: "creation-facts-elsewhere", Capabilities: capability.Postgres18()},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selected := &target{engine: &protocol.Engine{Id: test.engine}}

			got, err := selected.ProjectTableCreations(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(got.Complete, qt.IsFalse)
		})
	}
}
