package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestKnowledgeSourcesRetainIndependentAuthorityAndStorage(t *testing.T) {
	c := qt.New(t)
	first := scopedKnowledge(c, schemaext.NamespaceScope{Targets: []string{"ydb"}, Schema: new(namespacePart("first"))})
	second := scopedKnowledge(c, schemaext.NamespaceScope{Targets: []string{"ydb"}, Schema: new(namespacePart("second"))})
	combined, err := first.Intersect(second)
	c.Assert(err, qt.IsNil)
	sources := combined.Sources()
	c.Assert(sources, qt.HasLen, 2)
	reconstructed, err := schemaext.NewScopedKnowledge(sources[0].Outside, sources[0].Regions...)
	c.Assert(err, qt.IsNil)
	other, err := schemaext.NewScopedKnowledge(sources[1].Outside, sources[1].Regions...)
	c.Assert(err, qt.IsNil)
	reconstructed, err = reconstructed.Intersect(other)
	c.Assert(err, qt.IsNil)
	c.Assert(reconstructed.Equal(combined), qt.IsTrue)
	subject := widgetRef("table", "value")
	subject.Schema = namespacePart("first")
	c.Assert(reconstructed.Lookup(subject).State, qt.Equals, schemaext.Uninspected)
	sources[0].Outside = schemaext.Knowledge{State: schemaext.Complete}
	sources[0].Regions[0].Scope.Targets[0] = "spanner"
	sources[0].Regions[0].Scope.Schema.Source = "changed"
	sources[0].Regions[0].Knowledge = schemaext.Knowledge{State: schemaext.Complete}
	c.Assert(reconstructed.Equal(combined), qt.IsTrue)
	c.Assert(combined.Sources()[0].Regions[0].Scope.Targets, qt.DeepEquals, []string{"ydb"})
	c.Assert((schemaext.KnowledgeDomain{}).Sources(), qt.HasLen, 0)
}
