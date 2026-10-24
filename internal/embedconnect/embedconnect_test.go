package embedconnect_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
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
			c.Assert(embedconnect.RefuseAnotherEngine(c.Context(), dbURL), qt.IsNil)
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
			want: `ptah inference works against PostgreSQL with pgvector, against YDB, and against a provider engine ` +
				`with an embedding store, and "mysql://" names another engine: a generation's run state and its ` +
				`vectors need one of those, so there is nothing here to run against mysql`},
		{name: "the PostgreSQL family", dbURL: "crdb://localhost:26257/db",
			want: `.* "crdb://" names another engine: .* run against cockroachdb`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(embedconnect.RefuseAnotherEngine(c.Context(), test.dbURL), qt.ErrorMatches, test.want)
		})
	}
}

// connector stands for a provider's connector. Nothing here opens it.
type connector struct{}

func (connector) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	panic("the refusal is decided before anything connects")
}

// A scheme a selected provider serves is the provider's to answer, even when
// Ptah knows the dialect it names: the provider owns that engine for the
// invocation, and its embedding store says what it can hold.
func TestRefuseAnotherEngine_AProviderServesTheScheme(t *testing.T) {
	c := qt.New(t)
	ctx, err := dbschema.WithNativeConnections(c.Context(),
		map[string]dbschema.NativeRegistration{"mysql": {Target: "mysql", Connector: connector{}}})
	c.Assert(err, qt.IsNil)

	c.Assert(embedconnect.RefuseAnotherEngine(ctx, "mysql://localhost:3306/db"), qt.IsNil)
	c.Assert(embedconnect.RefuseAnotherEngine(ctx, "MySQL://localhost:3306/db"), qt.IsNil)
	c.Assert(embedconnect.RefuseAnotherEngine(c.Context(), "mysql://localhost:3306/db"), qt.ErrorMatches,
		`.* "mysql://" names another engine: .*`)
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
			objects, err := embedconnect.TargetObjects(c.Context(), targetSpec(test.representation))
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

	objects, err := embedconnect.TargetObjects(c.Context(), spec)

	c.Assert(err, qt.ErrorMatches, `target objects: no operator class is known for representation "bfloat16"`)
	c.Assert(objects, qt.DeepEquals, embedgen.TargetObjects{})
}
