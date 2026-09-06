package embedgen_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedgen"
)

// TestTargetObjects_DescribesTheColumnAndIndexAGenerationNeeds is Phase C in
// the vocabulary Ptah already plans in.
//
// The objects are a description rather than DDL: Ptah renders, compares and
// plans schema objects for every target it supports, so a second DDL path here
// would be a second answer to what a vector column is (stokaro/ptah#2068).
func TestTargetObjects_DescribesTheColumnAndIndexAGenerationNeeds(t *testing.T) {
	c := qt.New(t)
	spec := baseSpec()

	objects, err := spec.TargetObjects()

	c.Assert(err, qt.IsNil)
	c.Assert(objects.Column.Name, qt.Equals, "embedding_v1")
	c.Assert(objects.Column.Type, qt.Equals, "vector(1024)")
	c.Assert(objects.Column.StructName, qt.Equals, "article")
	c.Assert(objects.RequiredExtensions, qt.DeepEquals, []string{"vector"})
	c.Assert(objects.HasIndex, qt.IsTrue)
	c.Assert(objects.Index.Type, qt.Equals, "hnsw")
	c.Assert(objects.Index.Fields, qt.DeepEquals, []string{"embedding_v1"})
	c.Assert(objects.Index.Operator, qt.Equals, "vector_cosine_ops")
}

// TestTargetObjects_TheColumnIsNullable is what makes a backfill possible at
// all.
//
// A generation is populated over time, so a row without its vector yet is the
// normal state during a backfill rather than an error. NOT NULL would make the
// column uncreatable until the backfill finished, which is the wrong way round:
// the column has to exist before anything can be written into it. Coverage is
// verification's question, not the column's.
func TestTargetObjects_TheColumnIsNullable(t *testing.T) {
	c := qt.New(t)

	objects, err := baseSpec().TargetObjects()

	c.Assert(err, qt.IsNil)
	c.Assert(objects.Column.Nullable, qt.IsTrue)
}

// TestTargetObjects_TwoGenerationsDoNotCollide is Decision 6 made structural.
//
// An existing generation is not overwritten in place, so two generations over
// one table need two columns and two indexes. The index name carries the
// generation's identity precisely so the second one can exist; a name derived
// from the column alone would collide the moment a migration started, which is
// the whole shape this design is for.
func TestTargetObjects_TwoGenerationsDoNotCollide(t *testing.T) {
	c := qt.New(t)
	first := baseSpec()
	second := baseSpec()
	second.Model.Identifier = "e5-large"
	second.Target.Column = "embedding_v2"

	left, err := first.TargetObjects()
	c.Assert(err, qt.IsNil)
	right, err := second.TargetObjects()
	c.Assert(err, qt.IsNil)

	c.Assert(left.Column.Name, qt.Not(qt.Equals), right.Column.Name)
	c.Assert(left.Index.Name, qt.Not(qt.Equals), right.Index.Name)
}

// TestTargetObjects_TheIndexNameSeparatesGenerationsOnOneColumn is the sharper
// half of the row above.
//
// Two generations may share a column name across environments, or differ only
// in something the column name does not show. The identity in the index name is
// what keeps them apart, so this changes the MODEL alone and requires the index
// names to differ.
func TestTargetObjects_TheIndexNameSeparatesGenerationsOnOneColumn(t *testing.T) {
	c := qt.New(t)
	first := baseSpec()
	second := baseSpec()
	second.Model.Identifier = "e5-large"

	left, err := first.TargetObjects()
	c.Assert(err, qt.IsNil)
	right, err := second.TargetObjects()
	c.Assert(err, qt.IsNil)

	c.Assert(left.Column.Name, qt.Equals, right.Column.Name)
	c.Assert(left.Index.Name, qt.Not(qt.Equals), right.Index.Name)
}

// TestTargetObjects_TheOperatorClassFollowsTheRepresentationAndMetric pins the
// pairs pgvector actually has.
//
// They are looked up rather than composed: a composed name that happened to be
// wrong would fail at CREATE INDEX, with the target's own error, after the
// column and the whole backfill.
func TestTargetObjects_TheOperatorClassFollowsTheRepresentationAndMetric(t *testing.T) {
	tests := []struct {
		name           string
		representation string
		metric         embedgen.DistanceMetric
		want           string
	}{
		{name: "vector, cosine", representation: "vector", metric: embedgen.MetricCosine, want: "vector_cosine_ops"},
		{name: "vector, L2", representation: "vector", metric: embedgen.MetricL2, want: "vector_l2_ops"},
		{
			name: "vector, inner product", representation: "vector",
			metric: embedgen.MetricInnerProduct, want: "vector_ip_ops",
		},
		{name: "halfvec, cosine", representation: "halfvec", metric: embedgen.MetricCosine, want: "halfvec_cosine_ops"},
		{name: "halfvec, L2", representation: "halfvec", metric: embedgen.MetricL2, want: "halfvec_l2_ops"},
		{
			name: "sparsevec, inner product", representation: "sparsevec",
			metric: embedgen.MetricInnerProduct, want: "sparsevec_ip_ops",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := baseSpec()
			spec.Target.Representation = test.representation
			spec.Target.Metric = test.metric

			objects, err := spec.TargetObjects()

			c.Assert(err, qt.IsNil)
			c.Assert(objects.Index.Operator, qt.Equals, test.want)
			c.Assert(objects.Column.Type, qt.Equals, test.representation+"(1024)")
		})
	}
}

// TestTargetObjects_RefusesWhatItCannotDescribe is the other side.
//
// The dimension row is the load-bearing one: the dimension comes from the
// PROVIDER, and guessing it from what the specification requested would build a
// column the first response does not fit -- discovered after the column, the
// index and the start of a backfill.
func TestTargetObjects_RefusesWhatItCannotDescribe(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedgen.Spec)
		want   string
	}{
		{
			name:   "no reported dimension yet",
			change: func(s *embedgen.Spec) { s.Model.ReportedDimension = 0 },
			want:   `.*reports no output dimension yet.*`,
		},
		{name: "no table", change: func(s *embedgen.Spec) { s.Target.Table = "" }, want: `.*no target table.*`},
		{name: "no column", change: func(s *embedgen.Spec) { s.Target.Column = "" }, want: `.*no target column.*`},
		{
			name: "no representation", change: func(s *embedgen.Spec) { s.Target.Representation = "" },
			want: `.*no target representation.*`,
		},
		{
			name:   "a representation with no known operator class",
			change: func(s *embedgen.Spec) { s.Target.Representation = "bytevec" },
			want:   `.*no operator class is known for representation "bytevec".*`,
		},
		{
			name:   "a metric this representation has no class for",
			change: func(s *embedgen.Spec) { s.Target.Metric = "hamming" },
			want:   `.*no operator class is known for representation "vector" under metric "hamming".*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := baseSpec()
			test.change(&spec)

			_, err := spec.TargetObjects()

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestTargetObjects_AGenerationMayHaveNoIndexYet is Phase G's shape.
//
// Building the index before the backfill is what makes an ivfflat index useless
// -- ADR 0010 measured a build over too little data -- so a specification is
// allowed to describe a column with no index, and the caller decides when to
// add one.
func TestTargetObjects_AGenerationMayHaveNoIndexYet(t *testing.T) {
	c := qt.New(t)
	spec := baseSpec()
	spec.Target.IndexMethod = ""

	objects, err := spec.TargetObjects()

	c.Assert(err, qt.IsNil)
	c.Assert(objects.HasIndex, qt.IsFalse)
	c.Assert(objects.Column.Name, qt.Equals, "embedding_v1")
}
