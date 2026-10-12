package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

var (
	eventsTable = annotation.Slot{Directive: "ptah:schema:table", Table: events}
	byTime      = annotation.Slot{Directive: "ptah:schema:index", Table: events, Name: "by_time"}
	idField     = annotation.Slot{Directive: "ptah:schema:field", Table: events, Name: "id"}
)

// attrs are attributes from name and value pairs.
func attrs(pairs ...string) []annotation.EncodedAttribute {
	written := make([]annotation.EncodedAttribute, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		written = append(written, annotation.EncodedAttribute{Name: pairs[i], Value: pairs[i+1]})
	}
	return written
}

func TestEncodeAnnotations_Settings_HappyPath(t *testing.T) {
	on := true
	tests := []struct {
		name string
		slot annotation.SlotFacets
		want []annotation.SlotAttributes
	}{
		{name: "column storage with the table's own attributes",
			slot: annotation.SlotFacets{Slot: eventsTable, Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnStore{
				ColumnStore: ydbschema.ColumnStore{HashColumns: []string{"id"}, Partitions: 4}}))},
			want: []annotation.SlotAttributes{{Slot: eventsTable, Attributes: attrs("store", "column", "partition_by_hash", "id", "column_shards", "4")}}},
		{name: "row partitioning after the table's properties",
			slot: annotation.SlotFacets{Slot: eventsTable, Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredTablePartitioning{
				TablePartitioning: ydbschema.TablePartitioning{MinPartitions: 2, KeyBloomFilter: &on, UniformPartitions: 8}}))},
			want: []annotation.SlotAttributes{{Slot: eventsTable, Position: annotation.AfterProperties,
				Attributes: attrs("auto_partitioning_min_partitions_count", "2", "key_bloom_filter", "ENABLED", "uniform_partitions", "8")}}},
		{name: "index partitioning with the index's own attributes",
			slot: annotation.SlotFacets{Slot: byTime, Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredIndexPartitioning{
				IndexPartitioning: ydbschema.IndexPartitioning{BySize: &on, MaxPartitions: 16}}))},
			want: []annotation.SlotAttributes{{Slot: byTime, Attributes: attrs("auto_partitioning_by_size", "ENABLED",
				"auto_partitioning_max_partitions_count", "16")}}},
		{name: "full-text options in the order YDB lists them, claimed",
			slot: annotation.SlotFacets{Slot: byTime, Reads: map[string]string{"type": "fulltext_plain"},
				Options: map[string]string{"use_filter_lowercase": "true", "tokenizer": "standard"}},
			want: []annotation.SlotAttributes{{Slot: byTime, Position: annotation.AfterProperties,
				Attributes: attrs("tokenizer", "standard", "use_filter_lowercase", "true"), Options: []string{"tokenizer", "use_filter_lowercase"}}}},
		{name: "local options in the order YDB lists them, claimed",
			slot: annotation.SlotFacets{Slot: byTime, Reads: map[string]string{"type": "bloom_ngram_filter"},
				Options: map[string]string{"case_sensitive": "true", "ngram_size": "3"}},
			want: []annotation.SlotAttributes{{Slot: byTime, Position: annotation.AfterProperties,
				Attributes: attrs("ngram_size", "3", "case_sensitive", "true"), Options: []string{"ngram_size", "case_sensitive"}}}},
		{name: "vector settings after the index's properties",
			slot: annotation.SlotFacets{Slot: byTime, Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredVectorIndex{
				Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2}))},
			want: []annotation.SlotAttributes{{Slot: byTime, Position: annotation.AfterProperties,
				Attributes: attrs("distance", "cosine", "vector_type", "float", "vector_dimension", "3", "levels", "1", "clusters", "2")}}},
		{name: "an index of another type claims no option",
			slot: annotation.SlotFacets{Slot: byTime, Reads: map[string]string{"type": "btree"}, Options: map[string]string{"fillfactor": "70"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeYDB(c.Context(), annotation.EncodeRequest{Tables: annotation.Tables{events}, Slots: []annotation.SlotFacets{test.slot}})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Attributes, qt.DeepEquals, test.want)
		})
	}
}

func TestEncodeAnnotations_Settings_FailurePath(t *testing.T) {
	partitioning := must.Must(schemaext.NewFacets(&ydbschema.DesiredIndexPartitioning{IndexPartitioning: ydbschema.IndexPartitioning{MinPartitions: 2}}))
	tests := []struct {
		name    string
		slot    annotation.SlotFacets
		wantErr string
	}{
		{name: "index partitioning of a field", slot: annotation.SlotFacets{Slot: idField, Facets: partitioning},
			wantErr: `unsupported feature: Go annotations cannot represent feature facet "ptah.run/ydb/index-partitioning"`},
		{name: "index partitioning of a table", slot: annotation.SlotFacets{Slot: eventsTable, Facets: partitioning},
			wantErr: `unsupported feature: Go annotations cannot represent feature facet "ptah.run/ydb/index-partitioning"`},
		{name: "a full-text index without options", slot: annotation.SlotFacets{Slot: byTime, Reads: map[string]string{"type": "fulltext_plain"}},
			wantErr: `index "by_time": a YDB full-text index requires analyzer settings in WITH`},
		{name: "a full-text option YDB does not know", slot: annotation.SlotFacets{Slot: byTime, Reads: map[string]string{"type": "fulltext_plain"},
			Options: map[string]string{"tokenizer": "standard", "stemmer": "porter"}},
			wantErr: `index "by_time": unknown YDB full-text index option "stemmer"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeYDB(c.Context(), annotation.EncodeRequest{Tables: annotation.Tables{events}, Slots: []annotation.SlotFacets{test.slot}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
		})
	}
}

// TestEncodeAnnotations_TypesTheFieldsOfAYDBExport pins that a YDB export's
// fields take the Go types of the YDB types they name, and that an export for
// another target types none.
func TestEncodeAnnotations_TypesTheFieldsOfAYDBExport(t *testing.T) {
	fields := []annotation.FieldDeclaration{{Slot: idField, Type: "Uint64", Key: true},
		{Slot: annotation.Slot{Directive: "ptah:schema:field", Table: events, Name: "at"}, Type: "Timestamp", Nullable: true},
		{Slot: annotation.Slot{Directive: "ptah:schema:field", Table: events, Name: "note"}, Type: "TEXT", Nullable: true}}
	for _, test := range []struct {
		target string
		want   []annotation.FieldType
	}{
		{"ydb", []annotation.FieldType{{Slot: idField, Name: "uint64"},
			{Slot: annotation.Slot{Directive: "ptah:schema:field", Table: events, Name: "at"}, Name: "time.Time", Import: "time"}}},
		{"postgres", nil},
		{"", nil},
	} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeYDB(c.Context(), annotation.EncodeRequest{Tables: annotation.Tables{events}, Fields: fields, Target: test.target})

			c.Assert(err, qt.IsNil)
			c.Assert(result.FieldTypes, qt.DeepEquals, test.want)
		})
	}
}

// TestEncodeAnnotations_RefusesANullableKeyOfAYDBExport pins that a YDB
// export refuses a nullable key column, which no declaration can ask for.
func TestEncodeAnnotations_RefusesANullableKeyOfAYDBExport(t *testing.T) {
	c := qt.New(t)
	fields := []annotation.FieldDeclaration{{Slot: idField, Type: "Uint64", Key: true, Nullable: true}}

	result, err := encodeYDB(c.Context(), annotation.EncodeRequest{Tables: annotation.Tables{events}, Fields: fields, Target: "ydb"})

	c.Assert(err, qt.ErrorMatches, `column "id" of table "events" is a nullable key column, which no Ptah declaration can represent: `+
		`Ptah writes NOT NULL on every YDB key column`)
	c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
}
