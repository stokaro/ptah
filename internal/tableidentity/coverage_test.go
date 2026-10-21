package tableidentity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/tableidentity"
)

func TestBindCoverageRefusesUnboundNamespaceLimits(t *testing.T) {
	c := qt.New(t)
	domain, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Complete}, schemaext.NamespaceKnowledge{
		Scope:     schemaext.NamespaceScope{Schema: new(objectidentity.Part)},
		Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "relative schema was not inspected"},
	})
	c.Assert(err, qt.IsNil)
	model := schemaext.CodecIdentity{Owner: "example.org/owner", Kind: "example.org/model", Representation: schemaext.Observed, Version: 1, Definition: "model definition"}
	source, err := schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: model, Knowledge: domain}}, nil)
	c.Assert(err, qt.IsNil)
	relative := objectidentity.ID{Kind: objectidentity.KindTable, Name: objectidentity.Part{Source: "orders", Normalized: "orders"}}
	c.Assert(source.Lookup(model.Kind, relative).State, qt.Equals, schemaext.Uninspected)
	semantics := identifier.ForDialect("postgres")
	bound := tableidentity.Subject("", "orders", "postgres", semantics)
	c.Assert(source.Lookup(model.Kind, bound).State, qt.Equals, schemaext.Complete)
	selected, err := schemaext.NewTargetSelection("postgres")
	c.Assert(err, qt.IsNil)
	result, err := tableidentity.BindCoverage(source, selected, semantics)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result.IsZero(), qt.IsTrue)
}
