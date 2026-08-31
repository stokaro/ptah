package embedgen_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedgen"
)

// TestIndexName_IsTheNameASpecificationBuilds is what makes the retirement's
// use of it correct.
//
// A retirement holds a registry row and no specification, so it names the index
// from the table, the column and the identity. That is only right if it is the
// same name the specification built, and the two derivations used to differ:
// the retirement passed the CURRENT specification with the column swapped in,
// and the digest in the name came from a hybrid that was no generation at all
// (stokaro/ptah#2642).
func TestIndexName_IsTheNameASpecificationBuilds(t *testing.T) {
	c := qt.New(t)
	spec := indexableSpec()

	objects, err := spec.TargetObjects()

	c.Assert(err, qt.IsNil)
	c.Assert(objects.HasIndex, qt.IsTrue)
	c.Assert(objects.Index.Name, qt.Equals, embedgen.IndexName(
		spec.Target.Table, spec.Target.Column, spec.Identity().Digest))
}

// TestIndexName_TheIdentityIsTheOneItWasGiven is the property the defect
// violated.
//
// Two generations differing in an identity field get two names, and the name
// follows the identity handed in rather than one recomputed from anything else.
// The retirement's hybrid — the new specification wearing the old column —
// produced a third digest belonging to neither.
func TestIndexName_TheIdentityIsTheOneItWasGiven(t *testing.T) {
	c := qt.New(t)
	first := indexableSpec()
	second := indexableSpec()
	// Two generations over one table differ in the column by construction --
	// two vectors need two columns -- and usually in something else as well.
	second.Model.Revision = "2"
	second.Target.Column = "embedding_v2"
	c.Assert(second.Identity().Digest, qt.Not(qt.Equals), first.Identity().Digest,
		qt.Commentf("the fixture must produce two identities, or this asserts nothing"))

	// The shape the retirement had: the second specification carrying the
	// first's column. Its identity is neither generation's.
	hybrid := second
	hybrid.Target.Column = first.Target.Column
	c.Assert(hybrid.Identity().Digest, qt.Not(qt.Equals), first.Identity().Digest)
	c.Assert(hybrid.Identity().Digest, qt.Not(qt.Equals), second.Identity().Digest)

	name := embedgen.IndexName(
		first.Target.Table, first.Target.Column, first.Identity().Digest)

	c.Assert(name, qt.Equals, "articles_embedding_"+first.Identity().Short()+"_idx")
	c.Assert(name, qt.Not(qt.Contains), hybrid.Identity().Short())
}

// indexableSpec is a specification that builds an index.
func indexableSpec() embedgen.Spec {
	return embedgen.Spec{
		Source: embedgen.Source{
			Schema: "public", Table: "articles",
			KeyFields: []string{"id"}, InputFields: []string{"title"},
		},
		Model: embedgen.Model{
			Provider: "fake", Identifier: "m", Revision: "1", ReportedDimension: 4,
		},
		Target: embedgen.Target{
			Schema: "public", Table: "articles", Column: "embedding",
			Representation: "vector", Metric: embedgen.MetricCosine, IndexMethod: "hnsw",
		},
	}
}
