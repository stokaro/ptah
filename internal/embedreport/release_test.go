package embedreport_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedreport"
	"go.5x5.cz/ptah/internal/embedspec"
)

// TestBuildRelease_SaysWhatTheChangeProposes covers the mapping a registry
// holds. Every field here is what somebody asking six months later why a corpus
// was replaced has to read, and a release built from the wrong half of a plan
// answers a question nobody asked.
func TestBuildRelease_SaysWhatTheChangeProposes(t *testing.T) {
	c := qt.New(t)

	loaded := loadedFixture()
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	plan := embedreport.Plan{Desired: "gen-desired", Current: "gen-resolved"}

	release := embedreport.BuildRelease(loaded, plan, at)

	c.Assert(release.Generation, qt.Equals, "gen-desired")
	c.Assert(release.SpecDigest, qt.Equals, loaded.Digest)
	c.Assert(release.Target, qt.Equals, "public.articles.embedding")
	c.Assert(release.Reproducibility, qt.Equals, string(loaded.Spec.Identity().Reproducibility))
	c.Assert(release.Reason, qt.Equals, loaded.Spec.Identity().ReproducibilityReason)
	c.Assert(release.CreatedAt, qt.Equals, at)
	// Nothing has been evaluated when a release is written, so the corpus is
	// absent rather than named. A path recorded here would say a number was
	// measured against it.
	c.Assert(release.CorpusDigest, qt.Equals, "")
}

// TestBuildRelease_ReplacesIsThePlansAnswer is the one field with two candidate
// sources, and the wrong one is the operator's --current flag.
//
// The plan resolves what queries read NOW, from the database. A release built
// from the flag would say a generation was replaced because somebody typed its
// name, which is the claim a rollback is later read against.
func TestBuildRelease_ReplacesIsThePlansAnswer(t *testing.T) {
	c := qt.New(t)

	loaded := loadedFixture()
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	c.Assert(embedreport.BuildRelease(
		loaded, embedreport.Plan{Desired: "gen-2", Current: "gen-1"}, at,
	).Replaces, qt.Equals, "gen-1")
	// A first generation replaces nothing, and says so by carrying nothing.
	c.Assert(embedreport.BuildRelease(
		loaded, embedreport.Plan{Desired: "gen-2"}, at,
	).Replaces, qt.Equals, "")
}

// TestBuildRelease_ADifferentDocumentIsADifferentRelease is what the
// specification digest buys. Two files that address one generation are two
// proposals, and a release that could not tell them apart would send a reader
// to the wrong document.
func TestBuildRelease_ADifferentDocumentIsADifferentRelease(t *testing.T) {
	c := qt.New(t)

	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	plan := embedreport.Plan{Desired: "gen-2"}

	first := loadedFixture()
	second := loadedFixture()
	second.Spec.Description = "the same vectors, described differently"
	second.Digest = "a-different-document"

	// The generation is unchanged: a description is not part of the identity.
	c.Assert(second.Spec.Identity().Digest, qt.Equals, first.Spec.Identity().Digest)
	c.Assert(
		embedreport.BuildRelease(second, plan, at).Digest(),
		qt.Not(qt.Equals),
		embedreport.BuildRelease(first, plan, at).Digest(),
	)
}

// loadedFixture is a specification whose model carries no immutable revision,
// so the identity it produces is the partial one -- the answer a release has to
// carry a sentence for.
func loadedFixture() embedspec.Loaded {
	return embedspec.Loaded{
		Digest: "spec-document-digest",
		Spec: embedgen.Spec{
			Source: embedgen.Source{
				Schema: "public", Table: "articles",
				KeyFields: []string{"id"}, InputFields: []string{"title"},
			},
			Model: embedgen.Model{
				Provider: "openai", Identifier: "text-embedding-3-small", RequestedDimension: 3,
			},
			Target: embedgen.Target{
				Schema: "public", Table: "articles", Column: "embedding",
				Representation: "vector", Metric: embedgen.MetricCosine,
			},
		},
	}
}
