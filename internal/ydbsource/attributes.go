package ydbsource

import (
	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

// The frontend's own directives the owner adds attributes to.
const (
	directiveIndex = "ptah:schema:index"
	directiveTable = "ptah:schema:table"
)

// attributes are the YDB settings an index and a table declare in the
// frontend's own directives: an index's partitioning, vector settings and
// full-text or local options, and a row table's partitioning or a column
// table's storage. Each is named for the setting it becomes.
func attributes() []annotation.DirectiveAttributes {
	return []annotation.DirectiveAttributes{
		{
			Directive:  directiveIndex,
			Attributes: indexAttributes(),
			// The method makes an index a vector or a local index, and a
			// pgvector operator class names a vector index's metric.
			Reads:      []string{"type", "ops"},
			Decode:     decodeIndex,
			Parameters: indexOptions,
		},
		{Directive: directiveTable, Attributes: tableAttributes(), Decode: decodeTable},
	}
}

// decodeIndex reads an index's partitioning and its vector settings as the
// owner's facets. An index of another kind that states neither gets none.
func decodeIndex(values map[string]string) (schemaext.Facets, error) {
	partitioning, err := ydbindex.ParseDeclaration(values)
	if err != nil {
		return schemaext.Facets{}, refusal(err)
	}
	facets, err := ydbindex.WithPartitioning(schemaext.Facets{}, partitioning)
	if err != nil {
		return schemaext.Facets{}, err
	}
	vector, err := ydbindex.DeclareVector(values, values["type"], values["ops"])
	if err != nil {
		return schemaext.Facets{}, refusal(err)
	}
	if vector == nil {
		return facets, nil
	}
	return facets.With(vector)
}

// indexOptions reads a full-text index's analyzer options or a local
// index's settings into the index's WITH options, which every source and
// reader writes them to.
func indexOptions(values map[string]string) (map[string]string, error) {
	options, err := ydbindex.ParseOptionsDeclaration(values)
	if err != nil {
		return nil, refusal(err)
	}
	return options, nil
}

// decodeTable reads a row table's partitioning and a column table's storage
// as the owner's facets.
func decodeTable(values map[string]string) (schemaext.Facets, error) {
	var facets schemaext.Facets
	partitioning, err := ydbpartition.ParseTableDeclaration(values)
	if err != nil {
		return schemaext.Facets{}, refusal(err)
	}
	if partitioning != nil {
		if facets, err = facets.With(&ydbschema.DesiredTablePartitioning{TablePartitioning: *partitioning}); err != nil {
			return schemaext.Facets{}, err
		}
	}
	columnTable, err := ydbcolumn.Parse(values)
	if err != nil {
		return schemaext.Facets{}, refusal(err)
	}
	if columnTable == nil {
		return facets, nil
	}
	return facets.With(&ydbschema.DesiredColumnStore{ColumnStore: *columnTable})
}

func indexAttributes() []annotation.Attribute {
	return []annotation.Attribute{
		attr(ydbpartition.AttributeBySize, "YDB: whether the index's table splits a partition that grows past its size, ENABLED or DISABLED.",
			valueString, false, false),
		attr(ydbpartition.AttributePartitionSizeMB, "YDB: the size in MB at which the index's table splits a partition.",
			valueString, false, false),
		attr(ydbpartition.AttributeByLoad, "YDB: whether the index's table splits a busy partition, ENABLED or DISABLED.",
			valueString, false, false),
		attr(ydbpartition.AttributeMinPartitions, "YDB: the fewest partitions the index's table keeps.", valueString, false, false),
		attr(ydbpartition.AttributeMaxPartitions, "YDB: the most partitions the index's table splits into.", valueString, false, false),
		attr(ydbpartition.AttributeReadReplicas, "YDB: the index's read replicas, PER_AZ:<n> or ANY_AZ:<n>.", valueString, false, false),
		attr("false_positive_probability", "YDB local Bloom index: false-positive probability between zero and one.", valueString, false, false),
		attr("ngram_size", "YDB local n-gram index: token length.", valueString, false, false),
		attr("case_sensitive", "YDB local n-gram index: case-sensitive matching.", valueString, false, false),
		attr("tokenizer", "YDB full-text index: tokenizer.", valueString, false, false),
		attr("language", "YDB full-text index: language.", valueString, false, false),
		attr("use_filter_lowercase", "YDB full-text index: use filter lowercase.", valueString, false, false),
		attr("use_filter_stopwords", "YDB full-text index: use filter stopwords.", valueString, false, false),
		attr("use_filter_ngram", "YDB full-text index: use filter ngram.", valueString, false, false),
		attr("use_filter_edge_ngram", "YDB full-text index: use filter edge ngram.", valueString, false, false),
		attr("filter_ngram_min_length", "YDB full-text index: filter ngram min length.", valueString, false, false),
		attr("filter_ngram_max_length", "YDB full-text index: filter ngram max length.", valueString, false, false),
		attr("use_filter_length", "YDB full-text index: use filter length.", valueString, false, false),
		attr("filter_length_min", "YDB full-text index: filter length min.", valueString, false, false),
		attr("filter_length_max", "YDB full-text index: filter length max.", valueString, false, false),
		attr("use_filter_snowball", "YDB full-text index: use filter snowball.", valueString, false, false),
		attr(ydbindex.AttributeDistance, "YDB vector index: the distance it orders by, cosine, euclidean or manhattan.",
			valueString, false, false),
		attr(ydbindex.AttributeSimilarity, "YDB vector index: the similarity it orders by, inner_product or cosine.",
			valueString, false, false),
		attr(ydbindex.AttributeVectorType, "YDB vector index: the element type, float, uint8, int8 or bit.",
			valueString, false, false),
		attr(ydbindex.AttributeVectorDimension, "YDB vector index: the number of elements in a vector, 1 to 16384.",
			valueString, false, false),
		attr(ydbindex.AttributeLevels, "YDB vector index: the depth of its k-means tree, 1 to 16.", valueString, false, false),
		attr(ydbindex.AttributeClusters, "YDB vector index: the clusters each level splits into, 2 to 2048.",
			valueString, false, false),
	}
}

func tableAttributes() []annotation.Attribute {
	return []annotation.Attribute{
		attr(ydbcolumn.AttributeStore, "YDB table storage: ROW or COLUMN.", valueString, false, false),
		attr(ydbcolumn.AttributeHash, "YDB column table: hash-partitioning columns, separated by commas.", valueString, false, false),
		attr(ydbcolumn.AttributeShards, "YDB column table: initial shard count.", valueString, false, false),
		attr(ydbcolumn.AttributeTTL, "YDB column table: JSON retention policy with column, optional unit and tiers.", valueString, false, false),
		attr(ydbpartition.AttributeBySize, "YDB: whether the table splits a partition that grows past its size, ENABLED or DISABLED.",
			valueString, false, false),
		attr(ydbpartition.AttributePartitionSizeMB, "YDB: the size in MB at which the table splits a partition.",
			valueString, false, false),
		attr(ydbpartition.AttributeByLoad, "YDB: whether the table splits a busy partition, ENABLED or DISABLED.",
			valueString, false, false),
		attr(ydbpartition.AttributeMinPartitions, "YDB: the fewest partitions the table keeps.", valueString, false, false),
		attr(ydbpartition.AttributeMaxPartitions, "YDB: the most partitions the table splits into.", valueString, false, false),
		attr(ydbpartition.AttributeReadReplicas, "YDB: the table's read replicas, PER_AZ:<n> or ANY_AZ:<n>.", valueString, false, false),
		attr(ydbpartition.AttributeKeyBloomFilter, "YDB: whether the table keeps a bloom filter of its keys, ENABLED or DISABLED.",
			valueString, false, false),
		attr(ydbpartition.AttributeUniformPartitions, "YDB: the partitions a new table starts with, splitting a Uint32 or Uint64 first key evenly.",
			valueString, false, false),
		attr(ydbpartition.AttributePartitionAtKeys, "YDB: the keys a new table starts split before, such as `10, 20` or `(10, 'a'), (20)`.",
			valueString, false, false),
	}
}
