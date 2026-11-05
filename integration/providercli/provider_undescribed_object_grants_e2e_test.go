//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

// queueCoverage is a desired source's claim about the scope-selections
// fixture's queues: the kind enrolled with each knowledge given, and no claim
// at all without one.
func queueCoverage(c *qt.C, runtime *engine.Runtime, knowledge ...schemaext.Knowledge) schemaext.Coverage {
	c.Helper()
	model, found := runtime.Codecs().Identity(selectedQueue, schemaext.Desired)
	c.Assert(found, qt.IsTrue)
	kinds := make([]schemaext.KindCoverage, 0, len(knowledge))
	for _, claim := range knowledge {
		kinds = append(kinds, schemaext.KindCoverage{Model: model, Knowledge: schemaext.GlobalKnowledge(claim)})
	}
	coverage, err := schemaext.NewCoverage(schemaext.Desired, kinds, nil)
	c.Assert(err, qt.IsNil)
	return coverage
}

// mailReader is the read's entry for the role reader on the queue main.mail,
// which the fixture's scope selection names by a TOPIC grant.
var mailReader = catalog.Grant{Role: "reader", Privilege: "SELECT", ObjectType: "TOPIC", Schema: "main", ObjectName: "mail"}

// A grant on a provider's object rides what the desired source says about
// the object. The source manages the role reader and declares no queue. Where
// it does not describe queues, by a not-described claim or by claiming
// nothing, the read's grant on a queue stays as the database holds it. Where
// it describes them, the queue is not declared, and the grant is a removal.
func TestProviderKeepsTheGrantsOfAnUndescribedObject(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "scope-selections", nil)
	for _, test := range []struct {
		name      string
		knowledge []schemaext.Knowledge
		removed   []difftypes.GrantRef
	}{
		{name: "no claim about queues"},
		{name: "queues not described", knowledge: []schemaext.Knowledge{{State: schemaext.Uninspected, Reason: "the description does not describe queue"}}},
		{name: "queues described", knowledge: []schemaext.Knowledge{{State: schemaext.Complete}},
			removed: []difftypes.GrantRef{{Role: "reader", Privilege: "SELECT", ObjectType: "TOPIC", ObjectName: "main.mail"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader"}}, FeatureCoverage: queueCoverage(c, runtime, test.knowledge...)}
			current := &catalog.Database{Roles: []catalog.Role{{Name: "reader"}}, Grants: []catalog.Grant{mailReader}}

			diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, "fixture", runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.GrantsRemoved, qt.DeepEquals, test.removed)
			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
		})
	}
}
