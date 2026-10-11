package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/ydbsource"
)

func settingFacets(values ...schemaext.Value) schemaext.Facets {
	return must.Must(schemaext.NewFacets(values...))
}

func textAttribute(name, value string) hclext.EncodedAttribute {
	return hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.String, Text: value}}
}

func numberAttribute(name, value string) hclext.EncodedAttribute {
	return hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.Number, Text: value}}
}

// TestHCL_EncodeHCL_WritesTheSettings writes a vector index's settings and an
// index's partitioning, declared or read, as the index block's attributes,
// vector settings first and a count as a number, and reports the table
// settings HCL has no spelling for at their table: its column families, its
// column storage and its partitioning, in that order. A table holding only a
// plain default family, and settings at their defaults, report nothing.
func TestHCL_EncodeHCL_WritesTheSettings(t *testing.T) {
	c := qt.New(t)
	yes, no := true, false
	table := func(name string) hclext.Table { return hclext.Table{Schema: "app", Name: name} }
	index := func(name string) hclext.Slot {
		return hclext.Slot{Kind: hclext.SlotIndex, Table: table("t"), Name: name}
	}
	vector := ydbschema.DesiredVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 64}
	request := hclext.EncodeRequest{
		Tables: []hclext.TableFacets{
			{Table: table("all"), Facets: settingFacets(
				&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "default"}, {Name: "cold", Data: "hdd"}, {Name: "hot", Data: "ssd"}}},
				&ydbschema.DesiredColumnStore{},
				&ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{ByLoad: &yes, UniformPartitions: 4}})},
			{Table: table("read"), Facets: settingFacets(
				&ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "default", Data: "ssd"}}},
				&ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"10"}}}})},
			{Table: table("plain"), Facets: settingFacets(
				&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "default"}}},
				&ydbschema.DesiredTablePartitioning{})},
		},
		Indexes: []hclext.SlotFacets{
			{Slot: index("v"), Facets: settingFacets(&vector,
				&ydbschema.DesiredIndexPartitioning{IndexPartitioning: ydbschema.IndexPartitioning{BySize: &no, ByLoad: &yes, PartitionSizeMB: 100,
					MinPartitions: 1, MaxPartitions: 4, ReadReplicas: "ANY_AZ:2"}})},
			{Slot: index("read"), Facets: settingFacets(&ydbschema.ObservedVectorIndex{Similarity: "inner_product", VectorType: "uint8", Dimension: 8})},
			{Slot: index("default"), Facets: settingFacets(&ydbschema.DesiredIndexPartitioning{})},
		},
	}

	result, err := ydbsource.HCL().Encoder.EncodeHCL(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Attributes, qt.DeepEquals, []hclext.SlotAttributes{
		{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table("all")}, Losses: []string{
			"column families cold, hot are not represented in HCL",
			"YDB column storage, hash partitioning and tiered TTL are not represented in HCL",
			"YDB table settings (AUTO_PARTITIONING_BY_LOAD = ENABLED, UNIFORM_PARTITIONS = 4) are not represented in HCL"}},
		{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table("read")}, Losses: []string{
			"column family default is not represented in HCL",
			"YDB table settings (PARTITION_AT_KEYS = (10)) are not represented in HCL"}},
		{Slot: index("v"), Attributes: []hclext.EncodedAttribute{
			textAttribute("distance", "cosine"), textAttribute("vector_type", "float"), numberAttribute("vector_dimension", "3"),
			numberAttribute("levels", "2"), numberAttribute("clusters", "64"),
			textAttribute("auto_partitioning_by_size", "DISABLED"), textAttribute("auto_partitioning_by_load", "ENABLED"),
			numberAttribute("auto_partitioning_partition_size_mb", "100"), numberAttribute("auto_partitioning_min_partitions_count", "1"),
			numberAttribute("auto_partitioning_max_partitions_count", "4"), textAttribute("read_replicas_settings", "ANY_AZ:2")}},
		{Slot: index("read"), Attributes: []hclext.EncodedAttribute{
			textAttribute("similarity", "inner_product"), textAttribute("vector_type", "uint8"), numberAttribute("vector_dimension", "8")}},
	})
}

// TestHCL_NamesTheLimitsHCLCannotRecord says a secret, topic, external
// object, async replication or transfer recorded as not described is one HCL
// cannot record.
func TestHCL_NamesTheLimitsHCLCannotRecord(t *testing.T) {
	c := qt.New(t)

	extension := ydbsource.HCL()

	c.Assert(extension.Unrecorded, qt.DeepEquals, []schemaext.Kind{ydbsecret.Kind, ydbtopic.Kind, ydbexternal.SourceKind, ydbexternal.TableKind,
		ydbreplication.ReplicationKind, ydbreplication.TransferKind})
}
