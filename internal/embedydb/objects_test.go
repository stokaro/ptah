package embedydb_test

import (
	"context"
	"encoding/hex"
	"math"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedydb"
)

// ydbSpec is a specification a YDB generation holds. Each test changes one
// thing about it.
func ydbSpec() embedgen.Spec {
	return embedgen.Spec{
		Name: "articles on YDB",
		Source: embedgen.Source{
			Schema: "app", Table: "articles", KeyFields: []string{"id"}, InputFields: []string{"title"},
		},
		Model: embedgen.Model{Identifier: "bge-m3", Revision: "1", ReportedDimension: 384},
		Target: embedgen.Target{
			Schema: "app", Table: "articles", Column: "embedding",
			Representation: "float", Metric: embedgen.MetricCosine,
			IndexMethod:  "vector_kmeans_tree",
			IndexOptions: map[string]string{"levels": "2", "clusters": "64"},
		},
	}
}

func TestTargetObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	spec := ydbSpec()

	objects, err := embedydb.TargetObjects(spec)

	c.Assert(err, qt.IsNil)
	c.Assert(objects.Column.Name, qt.Equals, "embedding")
	c.Assert(objects.Column.Type, qt.Equals, "String")
	c.Assert(objects.Column.Nullable, qt.IsTrue)
	c.Assert(objects.HasIndex, qt.IsTrue)
	c.Assert(objects.Index.Name, qt.Equals, embedgen.IndexName("articles", "embedding", spec.Identity().Digest))
	c.Assert(objects.Index.Type, qt.Equals, "vector_kmeans_tree")
	c.Assert(objects.Index.Operator, qt.Equals, "distance=cosine")
	c.Assert(objects.RequiredExtensions, qt.HasLen, 0)
	c.Assert(objects.OwnsTable, qt.IsFalse)
	c.Assert(objects.TableComment, qt.Equals, "")
}

// A generation that declares no index method has no index, and one in a table
// of its own carries the comment that makes the table Ptah's, without the
// foreign key a YDB table cannot have.
func TestTargetObjects_HappyPath_Layouts(t *testing.T) {
	c := qt.New(t)
	unindexed := ydbSpec()
	unindexed.Target.IndexMethod, unindexed.Target.IndexOptions = "", nil
	owned := ydbSpec()
	owned.Target.Table, owned.Target.Layout = "article_vectors", embedgen.LayoutOwnTable

	plain, err := embedydb.TargetObjects(unindexed)
	c.Assert(err, qt.IsNil)
	c.Assert(plain.HasIndex, qt.IsFalse)
	own, err := embedydb.TargetObjects(owned)
	c.Assert(err, qt.IsNil)
	c.Assert(own.OwnsTable, qt.IsTrue)
	c.Assert(own.TableComment, qt.Equals, embedgen.TableComment(owned.Identity().Digest))
	c.Assert(own.ForeignKeyName, qt.Equals, "")
}

func TestTargetObjects_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*embedgen.Spec)
		wantErr string
	}{
		{name: "pgvector representation", change: func(s *embedgen.Spec) { s.Target.Representation = "vector" },
			wantErr: `not supported on YDB: target objects: representation "vector" is not one a YDB generation ` +
				`stores.*set target.representation to "float".*`},
		{name: "quantized representation", change: func(s *embedgen.Spec) { s.Target.Representation = "uint8" },
			wantErr: `.*representation "uint8" is not one a YDB generation stores.*`},
		{name: "pgvector index method", change: func(s *embedgen.Spec) { s.Target.IndexMethod = "hnsw" },
			wantErr: `not supported on YDB: index method "hnsw" is not one YDB builds; a YDB vector index is ` +
				`vector_kmeans_tree`},
		{name: "pgvector build parameter", change: func(s *embedgen.Spec) { s.Target.IndexOptions["m"] = "16" },
			wantErr: `not supported on YDB: storage parameter "m" belongs to pgvector's hnsw index.*`},
		{name: "no levels", change: func(s *embedgen.Spec) { delete(s.Target.IndexOptions, "levels") },
			wantErr: `a vector_kmeans_tree index needs index_options levels and clusters.*`},
		{name: "clusters out of range", change: func(s *embedgen.Spec) { s.Target.IndexOptions["clusters"] = "1" },
			wantErr: `a vector index's clusters are between 2 and 2048.*`},
		{name: "own table on the source", change: func(s *embedgen.Spec) { s.Target.Layout = embedgen.LayoutOwnTable },
			wantErr: `target objects: layout "own_table" names the source table app/articles as its target.*`},
		{name: "no dimension yet", change: func(s *embedgen.Spec) { s.Model.ReportedDimension = 0 },
			wantErr: `target objects: the model reports no output dimension yet.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := ydbSpec()
			test.change(&spec)

			objects, err := embedydb.TargetObjects(spec)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(objects, qt.DeepEquals, embedgen.TargetObjects{})
		})
	}
}

// Each metric is the YDB setting that orders by it.
func TestVectorSettings_HappyPath(t *testing.T) {
	tests := []struct {
		metric embedgen.DistanceMetric
		want   string
	}{
		{metric: embedgen.MetricCosine, want: "distance=cosine"},
		{metric: embedgen.MetricL2, want: "distance=euclidean"},
		{metric: embedgen.MetricInnerProduct, want: "similarity=inner_product"},
	}
	for _, test := range tests {
		t.Run(string(test.metric), func(t *testing.T) {
			c := qt.New(t)
			spec := ydbSpec()
			spec.Target.Metric = test.metric

			settings, err := embedydb.VectorSettings(spec)

			c.Assert(err, qt.IsNil)
			c.Assert(embedydb.MetricSetting(settings), qt.Equals, test.want)
			c.Assert(settings.VectorType, qt.Equals, "float")
			c.Assert(settings.Dimension, qt.Equals, uint64(384))
			c.Assert(settings.Levels, qt.Equals, uint64(2))
			c.Assert(settings.Clusters, qt.Equals, uint64(64))
		})
	}
}

func TestMetricSetting(t *testing.T) {
	c := qt.New(t)
	c.Assert(embedydb.MetricSetting(ydbschema.VectorSettings{Distance: "manhattan"}), qt.Equals, "distance=manhattan")
	c.Assert(embedydb.MetricSetting(ydbschema.VectorSettings{Similarity: "cosine"}), qt.Equals, "similarity=cosine")
}

// The bytes are the ones Knn::ToBinaryStringFloat produces for the same
// vector, measured on YDB 25.1.4.7 and 26.2.1.14.
func TestEncodeVector_HappyPath(t *testing.T) {
	c := qt.New(t)

	encoded, err := embedydb.EncodeVector([]float32{1, -2.5, 3.25})

	c.Assert(err, qt.IsNil)
	c.Assert(hex.EncodeToString(encoded), qt.Equals, "0000803f000020c00000504001")
}

func TestEncodeVector_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		vector  []float32
		wantErr string
	}{
		{name: "NaN", vector: []float32{1, float32(math.NaN())},
			wantErr: `vector element 1 is NaN, which a stored vector cannot hold`},
		{name: "infinity", vector: []float32{float32(math.Inf(-1))},
			wantErr: `vector element 0 is -Inf, which a stored vector cannot hold`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			encoded, err := embedydb.EncodeVector(test.vector)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(encoded, qt.IsNil)
		})
	}
}

// The run-state tables render for the line they are created on: a timestamp
// is Timestamp64 where the line has the 64-bit types and Timestamp on 25.1,
// and every table has the key the PostgreSQL store gives it.
func TestSchemaSQL_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		caps      capability.Capabilities
		timestamp string
	}{
		{name: "26.2", caps: capability.YDB262(), timestamp: "`created_at` Timestamp64 NOT NULL"},
		{name: "25.1", caps: capability.YDB251(), timestamp: "`created_at` Timestamp NOT NULL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := embedydb.SchemaSQL(context.Background(), must.Must(builtin.New()), test.caps)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 4)
			c.Assert(statements[0], qt.Contains, "CREATE TABLE IF NOT EXISTS `ptah_embedding_generation`")
			c.Assert(statements[0], qt.Contains, test.timestamp)
			c.Assert(statements[0], qt.Contains, "PRIMARY KEY (`identity`)")
			c.Assert(statements[1], qt.Contains, "PRIMARY KEY (`id`)")
			c.Assert(statements[2], qt.Contains, "PRIMARY KEY (`run_id`, `sequence`)")
			c.Assert(statements[3], qt.Contains, "PRIMARY KEY (`target_schema`, `target_table`)")
			c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "INDEX")
		})
	}
}
