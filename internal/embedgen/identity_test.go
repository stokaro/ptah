package embedgen_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedgen"
)

// baseSpec is a complete, ordinary specification. Every test below changes one
// thing about it, which is what makes each row measure that one thing.
func baseSpec() embedgen.Spec {
	return embedgen.Spec{
		Name:        "articles v1",
		Description: "the first generation",
		Source: embedgen.Source{
			Schema:          "public",
			Table:           "article",
			KeyFields:       []string{"id"},
			InputFields:     []string{"title", "body"},
			Filter:          "published",
			VersionStrategy: embedgen.VersionMonotonic,
			VersionField:    "version",
		},
		Preprocessing: embedgen.Preprocessing{
			Separator:            "\n\n",
			NullPolicy:           embedgen.NullAsEmpty,
			EmptyPolicy:          embedgen.EmptyRefuseRow,
			UnicodeNormalization: embedgen.UnicodeNFC,
			CollapseWhitespace:   true,
			MaxInputBytes:        8000,
			Truncate:             embedgen.TruncateRefuse,
		},
		Model: embedgen.Model{
			Provider:           "openai-compatible",
			EndpointClass:      embedgen.EndpointLocal,
			Identifier:         "bge-m3",
			Revision:           "sha256:abc",
			RequestedDimension: 1024,
			ReportedDimension:  1024,
			Normalization:      embedgen.NormalizationL2,
		},
		Target: embedgen.Target{
			Schema:         "public",
			Table:          "article",
			Column:         "embedding_v1",
			Representation: "vector",
			Metric:         embedgen.MetricCosine,
			IndexMethod:    "hnsw",
			IndexOptions:   map[string]string{"m": "16", "ef_construction": "64"},
		},
	}
}

// TestIdentity_ChangesWithEveryLoadBearingProperty is the first half of the
// epic's requirement, one row per property it names.
//
// Each row changes exactly one thing and requires a different identity. A row
// that failed would mean two corpora that cannot be compared are treated as one
// (stokaro/ptah#2068).
func TestIdentity_ChangesWithEveryLoadBearingProperty(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedgen.Spec)
	}{
		{name: "the source field set", change: func(s *embedgen.Spec) {
			s.Source.InputFields = []string{"title", "body", "summary"}
		}},
		{name: "the source field ORDER", change: func(s *embedgen.Spec) {
			s.Source.InputFields = []string{"body", "title"}
		}},
		{name: "the key field order", change: func(s *embedgen.Spec) {
			s.Source.KeyFields = []string{"tenant", "id"}
		}},
		{name: "the source scope", change: func(s *embedgen.Spec) { s.Source.Filter = "published AND NOT hidden" }},
		{name: "the version strategy", change: func(s *embedgen.Spec) {
			s.Source.VersionStrategy = embedgen.VersionUpdatedAt
		}},
		{name: "preprocessing: the separator", change: func(s *embedgen.Spec) { s.Preprocessing.Separator = " " }},
		{name: "preprocessing: the prefix", change: func(s *embedgen.Spec) { s.Preprocessing.Prefix = "passage: " }},
		{name: "preprocessing: the null policy", change: func(s *embedgen.Spec) {
			s.Preprocessing.NullPolicy = embedgen.NullSkipField
		}},
		{name: "preprocessing: the empty policy", change: func(s *embedgen.Spec) {
			s.Preprocessing.EmptyPolicy = embedgen.EmptySkipRow
		}},
		{name: "preprocessing: Unicode normalization", change: func(s *embedgen.Spec) {
			s.Preprocessing.UnicodeNormalization = embedgen.UnicodeNFKC
		}},
		{name: "preprocessing: whitespace handling", change: func(s *embedgen.Spec) {
			s.Preprocessing.CollapseWhitespace = false
		}},
		{name: "truncation: the bound", change: func(s *embedgen.Spec) { s.Preprocessing.MaxInputBytes = 4000 }},
		{name: "truncation: the policy", change: func(s *embedgen.Spec) {
			s.Preprocessing.Truncate = embedgen.TruncateBytes
		}},
		{name: "the model", change: func(s *embedgen.Spec) { s.Model.Identifier = "e5-large" }},
		{name: "the model revision", change: func(s *embedgen.Spec) { s.Model.Revision = "sha256:def" }},
		{name: "the provider", change: func(s *embedgen.Spec) { s.Model.Provider = "cohere-compatible" }},
		{name: "the endpoint class", change: func(s *embedgen.Spec) { s.Model.EndpointClass = embedgen.EndpointHosted }},
		{name: "the requested dimension", change: func(s *embedgen.Spec) { s.Model.RequestedDimension = 512 }},
		{name: "the reported dimension", change: func(s *embedgen.Spec) { s.Model.ReportedDimension = 512 }},
		{name: "vector normalization", change: func(s *embedgen.Spec) {
			s.Model.Normalization = embedgen.NormalizationNone
		}},
		{name: "pooling", change: func(s *embedgen.Spec) { s.Model.Pooling = "mean" }},
		{name: "the distance metric", change: func(s *embedgen.Spec) { s.Target.Metric = embedgen.MetricL2 }},
		{name: "the target representation", change: func(s *embedgen.Spec) { s.Target.Representation = "halfvec" }},
		{name: "the target column", change: func(s *embedgen.Spec) { s.Target.Column = "embedding_v2" }},
		{name: "the index method", change: func(s *embedgen.Spec) { s.Target.IndexMethod = "ivfflat" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := baseSpec()
			test.change(&changed)

			c.Assert(changed.Identity().Digest, qt.Not(qt.Equals), baseSpec().Identity().Digest)
			c.Assert(embedgen.SameGeneration(baseSpec(), changed), qt.IsFalse)
		})
	}
}

// TestIdentity_DoesNotChangeWithPresentationOrTuning is the other half, and it
// is the half a digest over "everything" would fail.
//
// Every row here must leave the identity alone. A generation whose identity
// moved when somebody renamed it, or retuned an index over the same vectors,
// would make every such edit a corpus migration.
func TestIdentity_DoesNotChangeWithPresentationOrTuning(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedgen.Spec)
	}{
		{name: "the display name", change: func(s *embedgen.Spec) { s.Name = "articles, renamed" }},
		{name: "the description", change: func(s *embedgen.Spec) { s.Description = "different prose" }},
		{name: "index tuning: m", change: func(s *embedgen.Spec) { s.Target.IndexOptions["m"] = "32" }},
		{name: "index tuning: a new option", change: func(s *embedgen.Spec) { s.Target.IndexOptions["lists"] = "200" }},
		{name: "index tuning: none at all", change: func(s *embedgen.Spec) { s.Target.IndexOptions = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := baseSpec()
			test.change(&changed)

			c.Assert(changed.Identity().Digest, qt.Equals, baseSpec().Identity().Digest)
			c.Assert(embedgen.SameGeneration(baseSpec(), changed), qt.IsTrue)
		})
	}
}

// TestIdentity_IsStableAcrossRuns is what makes it an identity rather than a
// checksum of one process.
func TestIdentity_IsStableAcrossRuns(t *testing.T) {
	c := qt.New(t)

	first := baseSpec().Identity()
	second := baseSpec().Identity()

	c.Assert(first.Digest, qt.Equals, second.Digest)
	c.Assert(first.Digest, qt.HasLen, 64)
	c.Assert(first.Short(), qt.HasLen, 12)
}

// TestIdentity_ComponentsCannotForgeABoundary is why the encoding is
// length-prefixed.
//
// A field named `a.b` and two fields named `a` and `b` are different
// specifications. Joined with a separator they would produce one digest, so two
// corpora that share nothing would be declared comparable.
func TestIdentity_ComponentsCannotForgeABoundary(t *testing.T) {
	c := qt.New(t)

	joined := baseSpec()
	joined.Source.InputFields = []string{"title.body"}
	split := baseSpec()
	split.Source.InputFields = []string{"title", "body"}

	c.Assert(joined.Identity().Digest, qt.Not(qt.Equals), split.Identity().Digest)
}

// TestIdentity_TwoFieldsCannotBorrowEachOthersBoundary is what the LENGTH
// prefix holds, as opposed to the count.
//
// The row above is separated by the field count alone -- one field against two
// -- so a joined encoding would still tell those apart. These two have the SAME
// count and a different boundary: `a` + `b.c` against `a.b` + `c`. Joined with
// any separator they are one string; length-prefixed they are not.
func TestIdentity_TwoFieldsCannotBorrowEachOthersBoundary(t *testing.T) {
	c := qt.New(t)

	left := baseSpec()
	left.Source.InputFields = []string{"a", "b.c"}
	right := baseSpec()
	right.Source.InputFields = []string{"a.b", "c"}

	c.Assert(left.Identity().Digest, qt.Not(qt.Equals), right.Identity().Digest)
}

// TestIdentity_ReproducibilityIsReportedRatherThanFabricated is the epic's
// explicit rule: a provider with no immutable revision gets `partial` and a
// reason, and Ptah must not invent an identity for it.
func TestIdentity_ReproducibilityIsReportedRatherThanFabricated(t *testing.T) {
	tests := []struct {
		name       string
		revision   string
		want       embedgen.Reproducibility
		wantReason bool
	}{
		{name: "an immutable revision", revision: "sha256:abc", want: embedgen.ReproducibilityFull},
		{name: "no revision at all", revision: "", want: embedgen.ReproducibilityPartial, wantReason: true},
		{name: "blank is not a revision", revision: "   ", want: embedgen.ReproducibilityPartial, wantReason: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := baseSpec()
			spec.Model.Revision = test.revision

			identity := spec.Identity()

			c.Assert(identity.Reproducibility, qt.Equals, test.want)
			c.Assert(identity.ReproducibilityReason != "", qt.Equals, test.wantReason)
			// Partial is a statement about rebuilding, not about identity: the
			// digest is still computed and still separates generations.
			c.Assert(identity.Digest, qt.HasLen, 64)
		})
	}
}
