package ydb_test

import (
	"context"
	"encoding/hex"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	ydbmodel "ptah.run/dialect/ydb/ydbschema"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// The fields of an index description a server sent for a vector index, as
// DescribeTable returned them through ydb-go-genproto, which models no vector
// index: each is the whole of the description's unknown fields, in
// hexadecimal. Every one was read from local-ydb with the statement named.
const (
	// 26.2.1.14 and 25.1.4.7 send the same bytes for `INDEX vi GLOBAL USING
	// vector_kmeans_tree ON (emb) WITH (distance=cosine, vector_type=float,
	// vector_dimension=3, levels=2, clusters=4)`.
	measuredCosine = "4a280a0b1a09100118801020023001120b1a091001188010200230011a0c0a0608031001180310041802"
	// 26.2.1.14, `ON (gen, emb) WITH (distance=cosine, vector_type=float,
	// vector_dimension=3, levels=1, clusters=4)`: the prefix adds a table of
	// its own, field 4.
	measuredPrefixed = "4a350a0b1a09100118801020023001120b1a091001188010200230011a0c0a0608031001180310041801" +
		"220b1a09100118801020023001"
	// 26.2.1.14, `WITH (similarity=inner_product, vector_type=float,
	// vector_dimension=3, levels=1, clusters=4)`.
	measuredInnerProduct = "4a280a0b1a09100118801020023001120b1a091001188010200230011a0c0a0608011001180310041801"
	// 26.2.1.14, `WITH (distance=cosine, vector_type=bit,
	// vector_dimension=8, levels=1, clusters=4)`.
	measuredBit = "4a280a0b1a09100118801020023001120b1a091001188010200230011a0c0a0608031004180810041801"
	// 25.1.4.7, `WITH (distance=cosine, vector_type=float,
	// vector_dimension=3)`, which that line takes without levels and
	// clusters and then reports neither.
	measuredUndepthed = "4a240a0b1a09100118801020023001120b1a091001188010200230011a080a06080310011803"
	// 25.1.4.7, `ON (gen, emb) WITH (similarity=inner_product,
	// vector_type=uint8, vector_dimension=3, levels=1, clusters=4)`.
	measuredUint8 = "4a350a0b1a09100118801020023001120b1a091001188010200230011a0c0a0608011002180310041801" +
		"220b1a09100118801020023001"
	// 26.2.1.14, `WITH (distance=cosine, vector_type=float,
	// vector_dimension=3, levels=1, clusters=4, overlap_clusters=2)`.
	measuredOverlapClusters = "4a2a0a0b1a09100118801020023001120b1a091001188010200230011a0e0a06080310011803100418012002"
	// 26.2.1.14, the same with `overlap_ratio=1.5` in place of
	// overlap_clusters.
	measuredOverlapRatio = "4a310a0b1a09100118801020023001120b1a091001188010200230011a150a060803100118031004180129000000000000f83f"
)

// describedVectorIndex is an index described with the measured unknown
// fields, in hexadecimal, and no type.
func describedVectorIndex(c *qt.C, columns []string, fields string) *Ydb_Table.TableIndexDescription {
	c.Helper()
	unknown, err := hex.DecodeString(fields)
	c.Assert(err, qt.IsNil)
	index := &Ydb_Table.TableIndexDescription{Name: "vi", IndexColumns: columns}
	index.ProtoReflect().SetUnknown(unknown)
	return index
}

// vectorTable is a table keyed on id, with a Uint64 gen and a String emb,
// holding index.
func vectorTable(index *Ydb_Table.TableIndexDescription) fakeSource {
	described := plainTable(
		&Ydb_Table.ColumnMeta{Name: "gen", Type: optional(primitive(Ydb.Type_UINT64))},
		&Ydb_Table.ColumnMeta{Name: "emb", Type: optional(primitive(Ydb.Type_STRING))},
	)
	described.Indexes = []*Ydb_Table.TableIndexDescription{index}
	return fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
		tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": described},
	}
}

// TestReader_VectorIndex_HappyPath reads each measured vector index as its
// method and its settings, the YDB owner's observation bound to YDB, with
// complete coverage of the index, without asking for the implementation table
// a global index is read from: the fake source would refuse that path.
func TestReader_VectorIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		columns []string
		fields  string
		want    catalog.Index
		vector  ydbmodel.ObservedVectorIndex
	}{
		{
			name: "a cosine distance", columns: []string{"emb"}, fields: measuredCosine,
			want: catalog.Index{Name: "vi", TableName: "t", Columns: []string{"emb"}, Method: "GLOBAL USING vector_kmeans_tree",
				Definition: "INDEX `vi` GLOBAL USING vector_kmeans_tree ON (`emb`) " +
					"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=2, clusters=4)"},
			vector: ydbmodel.ObservedVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 4},
		},
		{
			name: "a prefix", columns: []string{"gen", "emb"}, fields: measuredPrefixed,
			want: catalog.Index{Name: "vi", TableName: "t", Columns: []string{"gen", "emb"}, Method: "GLOBAL USING vector_kmeans_tree",
				Definition: "INDEX `vi` GLOBAL USING vector_kmeans_tree ON (`gen`, `emb`) " +
					"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=4)"},
			vector: ydbmodel.ObservedVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 4},
		},
		{
			name: "an inner product similarity", columns: []string{"emb"}, fields: measuredInnerProduct,
			want: catalog.Index{Name: "vi", TableName: "t", Columns: []string{"emb"}, Method: "GLOBAL USING vector_kmeans_tree",
				Definition: "INDEX `vi` GLOBAL USING vector_kmeans_tree ON (`emb`) " +
					"WITH (similarity=inner_product, vector_type=float, vector_dimension=3, levels=1, clusters=4)"},
			vector: ydbmodel.ObservedVectorIndex{Similarity: "inner_product", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 4},
		},
		{
			name: "bit vectors", columns: []string{"emb"}, fields: measuredBit,
			want: catalog.Index{Name: "vi", TableName: "t", Columns: []string{"emb"}, Method: "GLOBAL USING vector_kmeans_tree",
				Definition: "INDEX `vi` GLOBAL USING vector_kmeans_tree ON (`emb`) " +
					"WITH (distance=cosine, vector_type=bit, vector_dimension=8, levels=1, clusters=4)"},
			vector: ydbmodel.ObservedVectorIndex{Distance: "cosine", VectorType: "bit", Dimension: 8, Levels: 1, Clusters: 4},
		},
		{
			name: "25.1's index without levels and clusters", columns: []string{"emb"}, fields: measuredUndepthed,
			want: catalog.Index{Name: "vi", TableName: "t", Columns: []string{"emb"}, Method: "GLOBAL USING vector_kmeans_tree",
				Definition: "INDEX `vi` GLOBAL USING vector_kmeans_tree ON (`emb`) " +
					"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=0, clusters=0)"},
			vector: ydbmodel.ObservedVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3},
		},
		{
			name: "25.1's uint8 vectors under a prefix", columns: []string{"gen", "emb"}, fields: measuredUint8,
			want: catalog.Index{Name: "vi", TableName: "t", Columns: []string{"gen", "emb"}, Method: "GLOBAL USING vector_kmeans_tree",
				Definition: "INDEX `vi` GLOBAL USING vector_kmeans_tree ON (`gen`, `emb`) " +
					"WITH (similarity=inner_product, vector_type=uint8, vector_dimension=3, levels=1, clusters=4)"},
			vector: ydbmodel.ObservedVectorIndex{Similarity: "inner_product", VectorType: "uint8", Dimension: 3, Levels: 1, Clusters: 4},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := readFrom(c, vectorTable(describedVectorIndex(c, test.columns, test.fields)))
			c.Assert(db.Indexes, qt.HasLen, 1)
			vector, _, err := schemaext.FacetAs[*ydbmodel.ObservedVectorIndex](db.Indexes[0].Facets, ydbmodel.VectorIndexKind)
			c.Assert(err, qt.IsNil)
			c.Assert(vector, qt.DeepEquals, &test.vector)
			c.Assert(db.Indexes[0].Facets.TargetScope(ydbmodel.VectorIndexKind), qt.DeepEquals, []string{"ydb"})
			subject := objectidentity.NewBuilder(identifier.ForDialect("ydb")).IndexParts("", "t", "vi")
			c.Assert(db.FeatureCoverage.Lookup(ydbmodel.VectorIndexKind, subject).State, qt.Equals, schemaext.Complete)
			db.Indexes[0].Facets = schemaext.Facets{}
			c.Assert(db.Indexes, qt.DeepEquals, []catalog.Index{test.want})
		})
	}
}

// appendMessage appends field number as a length-delimited message holding
// payload.
func appendMessage(b []byte, number protowire.Number, payload []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(b, number, protowire.BytesType), payload)
}

// appendVarint appends field number as a varint holding value.
func appendVarint(b []byte, number protowire.Number, value uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(b, number, protowire.VarintType), value)
}

// encodedVectorIndex is a vector index description's unknown fields in
// hexadecimal, built from a level table's settings and the settings of the
// tree, with extra appended to the tree's.
func encodedVectorIndex(levelTable, vectorSettings []byte) string {
	settings := appendVarint(appendVarint(appendVarint(nil, 1, 3), 2, 1), 3, 3)
	tree := appendMessage(nil, 1, settings)
	tree = appendVarint(appendVarint(tree, 2, 4), 3, 1)
	tree = append(tree, vectorSettings...)
	index := appendMessage(nil, 1, levelTable)
	index = appendMessage(index, 3, tree)
	return hex.EncodeToString(appendMessage(nil, 9, index))
}

// TestReader_VectorIndex_FailurePath refuses a vector index carrying a
// setting the reader does not model, by its name where YDB names it, rather
// than reading it as an index built without the setting.
func TestReader_VectorIndex_FailurePath(t *testing.T) {
	partitioned := appendMessage(nil, 3, appendVarint(nil, 6, 3)) // min_partitions_count = 3
	tests := []struct {
		name    string
		fields  string
		wantErr string
	}{
		{name: "overlap_clusters, as 26.2 reports it", fields: measuredOverlapClusters,
			wantErr: `YDB table /local/t: index "vi": its vector index sets overlap_clusters, which this build of Ptah does not read`},
		{name: "overlap_ratio, as 26.2 reports it", fields: measuredOverlapRatio,
			wantErr: `YDB table /local/t: index "vi": its vector index sets overlap_ratio, which this build of Ptah does not read`},
		{name: "adaptive_clusters", fields: encodedVectorIndex(nil, appendVarint(nil, 6, 1)),
			wantErr: `.*: its vector index sets adaptive_clusters, which this build of Ptah does not read`},
		{name: "a setting no line names", fields: encodedVectorIndex(nil, appendVarint(nil, 9, 1)),
			wantErr: `.*: its vector settings carry field 9, which this build of Ptah does not read`},
		{name: "a level table partitioned other than YDB's default", fields: encodedVectorIndex(partitioned, nil),
			wantErr: `.*: its vector index's level table is partitioned other than YDB gives a new index, .*`},
		{name: "a level table with its partitions named", fields: encodedVectorIndex(appendVarint(nil, 1, 4), nil),
			wantErr: `.*: its vector index's level table names its partitions, which Ptah does not read`},
		{name: "a metric no line reports",
			fields:  hex.EncodeToString(appendMessage(nil, 9, appendMessage(nil, 3, appendMessage(nil, 1, appendVarint(nil, 1, 9))))),
			wantErr: `.*: its vector index has metric 9, which this build of Ptah does not read`},
		{name: "an element type no line reports",
			fields:  hex.EncodeToString(appendMessage(nil, 9, appendMessage(nil, 3, appendMessage(nil, 1, appendVarint(nil, 2, 7))))),
			wantErr: `.*: its vector index has vector type 7, which this build of Ptah does not read`},
		{name: "no settings", fields: hex.EncodeToString(appendMessage(nil, 9, appendMessage(nil, 1, nil))),
			wantErr: `.*: its vector index carries no settings`},
		{name: "a field beside the vector index", fields: measuredCosine + hex.EncodeToString(appendVarint(nil, 15, 1)),
			wantErr: `.*: its description carries field 15 beside the vector index, which this build of Ptah does not read`},
		{name: "a description cut short", fields: measuredCosine[:20],
			wantErr: `.*: its description does not parse: .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := ydbschema.NewReaderFromSource(vectorTable(describedVectorIndex(c, []string{"emb"}, test.fields)),
				"/local", capability.YDB262()).ReadSchemaContext(context.Background())
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
