package embedconnect_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedconnect"
	"ptah.run/internal/embedgen"
)

// A URL an inference backend reads is left to it: a PostgreSQL scheme, a
// keyword/value DSN with no scheme, a scheme Ptah does not recognize, and
// either YDB scheme.
func TestRefuseAnotherEngine_HappyPath(t *testing.T) {
	for _, dbURL := range []string{
		"postgres://localhost:5432/db",
		"postgresql://localhost:5432/db",
		"host=localhost user=ptah dbname=ptah",
		"unknown://localhost/db",
		"ydb://localhost:2136/local",
		"ydbs://ydb.example.com:2135/?database=/a/b",
	} {
		t.Run(dbURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(embedconnect.RefuseAnotherEngine(dbURL), qt.IsNil)
		})
	}
}

// Another engine's URL says what inference runs against, naming the scheme as
// written and the dialect as Ptah calls it.
func TestRefuseAnotherEngine_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		dbURL string
		want  string
	}{
		{name: "mysql", dbURL: "mysql://localhost:3306/db",
			want: `ptah inference works against PostgreSQL with pgvector and against YDB, and "mysql://" names ` +
				`another engine: a generation's run state and its vectors need one of those two, so there is ` +
				`nothing here to run against mysql`},
		{name: "the PostgreSQL family", dbURL: "crdb://localhost:26257/db",
			want: `.* "crdb://" names another engine: .* run against cockroachdb`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(embedconnect.RefuseAnotherEngine(test.dbURL), qt.ErrorMatches, test.want)
		})
	}
}

// targetSpec is a specification that describes a target on either engine but
// for its representation.
func targetSpec(representation string) embedgen.Spec {
	return embedgen.Spec{
		Source: embedgen.Source{Table: "articles", KeyFields: []string{"id"}, InputFields: []string{"title"}},
		Model:  embedgen.Model{Identifier: "m", ReportedDimension: 8},
		Target: embedgen.Target{
			Table: "articles", Column: "embedding", Representation: representation,
			Metric: embedgen.MetricCosine,
		},
	}
}

// Without a database the representation decides the engine, because each
// names a type only one engine has.
func TestTargetObjects_HappyPath(t *testing.T) {
	tests := []struct {
		representation string
		wantType       string
	}{
		{representation: "vector", wantType: "vector(8)"},
		{representation: "halfvec", wantType: "halfvec(8)"},
		{representation: "float", wantType: "String"},
		{representation: "FLOAT", wantType: "String"},
	}
	for _, test := range tests {
		t.Run(test.representation, func(t *testing.T) {
			c := qt.New(t)
			objects, err := embedconnect.TargetObjects(targetSpec(test.representation))
			c.Assert(err, qt.IsNil)
			c.Assert(objects.Column.Type, qt.Equals, test.wantType)
		})
	}
}

// A representation neither engine has is refused, by the PostgreSQL
// derivation, which names it.
func TestTargetObjects_FailurePath(t *testing.T) {
	c := qt.New(t)
	spec := targetSpec("bfloat16")
	spec.Target.IndexMethod = "hnsw"

	objects, err := embedconnect.TargetObjects(spec)

	c.Assert(err, qt.ErrorMatches, `target objects: no operator class is known for representation "bfloat16"`)
	c.Assert(objects, qt.DeepEquals, embedgen.TargetObjects{})
}
