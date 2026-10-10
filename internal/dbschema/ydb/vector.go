package ydb

import (
	"fmt"
	"slices"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

// The pinned ydb-go-genproto models no vector index: a description of one
// arrives with no type, and the index sits in a field the protocol buffers do
// not know. The numbers below are ydb_table.proto's, the same on 25.1.4.7 and
// 26.2.1.14, and the reader decodes them itself.
const (
	// vectorIndexField is TableIndexDescription's
	// global_vector_kmeans_tree_index, a GlobalVectorKMeansTreeIndex.
	vectorIndexField protowire.Number = 9

	// The fields of GlobalVectorKMeansTreeIndex: the settings of the tables
	// the index is kept in, each a GlobalIndexSettings, and its own.
	vectorLevelTableField   protowire.Number = 1
	vectorPostingTableField protowire.Number = 2
	vectorSettingsField     protowire.Number = 3
	vectorPrefixTableField  protowire.Number = 4

	// The fields of GlobalIndexSettings, which the pinned protocol buffers do
	// not model either: the two arms of its partitions oneof, which name a
	// table's partitions, and its partitioning and read replicas, which they
	// do model.
	globalUniformPartitionsField protowire.Number = 1
	globalPartitionAtKeysField   protowire.Number = 2
	globalPartitioningField      protowire.Number = 3
	globalReadReplicasField      protowire.Number = 4

	// The fields of KMeansTreeSettings Ptah reads.
	kmeansSettingsField protowire.Number = 1
	kmeansClustersField protowire.Number = 2
	kmeansLevelsField   protowire.Number = 3

	// The fields of VectorIndexSettings.
	vectorMetricField    protowire.Number = 1
	vectorTypeField      protowire.Number = 2
	vectorDimensionField protowire.Number = 3
)

// kmeansUnread names the fields of KMeansTreeSettings Ptah does not model. A
// line that sets one builds an index the settings Ptah reads do not describe,
// so the read is refused by the setting's name: measured on 26.2.1.14, `WITH
// (..., overlap_clusters=2)` and `overlap_ratio=1.5` are accepted and reported
// in these fields, and 25.1.4.7 answers `Unknown index setting:
// overlap_clusters`.
var kmeansUnread = map[protowire.Number]string{
	4: "overlap_clusters",
	5: "overlap_ratio",
	6: "adaptive_clusters",
}

// vectorMetrics are VectorIndexSettings.Metric's values, as the setting that
// names each.
var vectorMetrics = map[uint64]ydbschema.VectorSettings{
	1: {Similarity: "inner_product"},
	2: {Similarity: "cosine"},
	3: {Distance: "cosine"},
	4: {Distance: "manhattan"},
	5: {Distance: "euclidean"},
}

// vectorElementTypes are VectorIndexSettings.VectorType's values.
var vectorElementTypes = map[uint64]string{
	1: "float",
	2: "uint8",
	3: "int8",
	4: ydbindex.BitVectorType,
}

// wireField is one field of a protocol buffers message as it arrived.
type wireField struct {
	number protowire.Number
	kind   protowire.Type
	// value is a varint field's value, or a fixed field's bits.
	value uint64
	// bytes is a length-delimited field's payload.
	bytes []byte
}

// splitFields reads data as the fields of one message, in the order they
// arrived. Data that does not parse is an error.
func splitFields(data []byte) ([]wireField, error) {
	var fields []wireField
	for len(data) > 0 {
		number, kind, length := protowire.ConsumeTag(data)
		if length < 0 {
			return nil, protowire.ParseError(length)
		}
		data = data[length:]
		field := wireField{number: number, kind: kind}
		switch kind {
		case protowire.VarintType:
			field.value, length = protowire.ConsumeVarint(data)
		case protowire.BytesType:
			field.bytes, length = protowire.ConsumeBytes(data)
		case protowire.Fixed64Type:
			field.value, length = protowire.ConsumeFixed64(data)
		default:
			length = protowire.ConsumeFieldValue(number, kind, data)
		}
		if length < 0 {
			return nil, protowire.ParseError(length)
		}
		data = data[length:]
		fields = append(fields, field)
	}
	return fields, nil
}

// vectorIndex reads the vector index a description carries in the field the
// pinned protocol buffers do not model, and reports false for a description
// that carries none, which is an index of another kind the reader does not
// read. A field beside it that the reader does not know is refused by number.
func vectorIndex(described *Ydb_Table.TableIndexDescription) (*ydbschema.VectorSettings, bool, error) {
	fields, err := splitFields(described.ProtoReflect().GetUnknown())
	if err != nil {
		return nil, false, fmt.Errorf("its description does not parse: %w", err)
	}
	index := slices.IndexFunc(fields, func(field wireField) bool { return field.number == vectorIndexField })
	if index < 0 {
		return nil, false, nil
	}
	for _, field := range fields {
		if field.number != vectorIndexField {
			return nil, true, fmt.Errorf("its description carries field %d beside the vector index, which this build "+
				"of Ptah does not read", field.number)
		}
	}
	if fields[index].kind != protowire.BytesType {
		return nil, true, fmt.Errorf("its vector index is not a message (wire type %d)", fields[index].kind)
	}
	spec, err := kmeansTreeIndex(fields[index].bytes)
	return spec, true, err
}

// kmeansTreeIndex reads a GlobalVectorKMeansTreeIndex: the settings the index
// was built with, and the tables it is kept in, which have to hold the
// partitioning YDB gives them, because no statement Ptah writes changes it
// (`ALTER INDEX ... SET` answers `Only index with one impl table is
// supported`) and none of it is in the model.
func kmeansTreeIndex(data []byte) (*ydbschema.VectorSettings, error) {
	fields, err := splitFields(data)
	if err != nil {
		return nil, fmt.Errorf("its vector index does not parse: %w", err)
	}
	var spec *ydbschema.VectorSettings
	for _, field := range fields {
		if field.kind != protowire.BytesType {
			return nil, fmt.Errorf("its vector index carries field %d as wire type %d, which this build of Ptah "+
				"does not read", field.number, field.kind)
		}
		switch field.number {
		case vectorLevelTableField:
			err = implementationTable("level", field.bytes)
		case vectorPostingTableField:
			err = implementationTable("posting", field.bytes)
		case vectorPrefixTableField:
			err = implementationTable("prefix", field.bytes)
		case vectorSettingsField:
			spec, err = kmeansTreeSettings(field.bytes)
		default:
			err = fmt.Errorf("its vector index carries field %d, which this build of Ptah does not read", field.number)
		}
		if err != nil {
			return nil, err
		}
	}
	if spec == nil {
		return nil, fmt.Errorf("its vector index carries no settings")
	}
	return spec, nil
}

// implementationTable holds the settings of one of the tables a vector index
// is kept in, a GlobalIndexSettings, to YDB's defaults.
func implementationTable(name string, data []byte) error {
	fields, err := splitFields(data)
	if err != nil {
		return fmt.Errorf("the settings of its vector index's %s table do not parse: %w", name, err)
	}
	partitioning := &Ydb_Table.PartitioningSettings{}
	replicas := &Ydb_Table.ReadReplicasSettings{}
	for _, field := range fields {
		switch {
		case field.number == globalUniformPartitionsField || field.number == globalPartitionAtKeysField:
			return fmt.Errorf("its vector index's %s table names its partitions, which Ptah does not read", name)
		case field.number == globalPartitioningField && field.kind == protowire.BytesType:
			err = proto.Unmarshal(field.bytes, partitioning)
		case field.number == globalReadReplicasField && field.kind == protowire.BytesType:
			err = proto.Unmarshal(field.bytes, replicas)
		default:
			return fmt.Errorf("the settings of its vector index's %s table carry field %d, which this build of "+
				"Ptah does not read", name, field.number)
		}
		if err != nil {
			return fmt.Errorf("the settings of its vector index's %s table do not parse: %w", name, err)
		}
	}
	held, err := partitionSettings(&Ydb_Table.DescribeTableResult{
		PartitioningSettings: partitioning, ReadReplicasSettings: replicas,
	})
	if err != nil {
		return fmt.Errorf("its vector index's %s table: %w", name, err)
	}
	if !held.Equal(ydbpartition.DefaultSettings()) {
		return fmt.Errorf("its vector index's %s table is partitioned other than YDB gives a new index, which "+
			"Ptah does not read for a vector index", name)
	}
	return nil
}

// kmeansTreeSettings reads a KMeansTreeSettings: the metric, the element
// type and the dimension, and the clusters and the levels of the tree. A
// setting the line leaves unreported reads as zero, as 25.1 leaves the
// clusters and the levels of an index declared without them.
func kmeansTreeSettings(data []byte) (*ydbschema.VectorSettings, error) {
	fields, err := splitFields(data)
	if err != nil {
		return nil, fmt.Errorf("its vector settings do not parse: %w", err)
	}
	var spec ydbschema.VectorSettings
	for _, field := range fields {
		if name, unread := kmeansUnread[field.number]; unread {
			return nil, fmt.Errorf("its vector index sets %s, which this build of Ptah does not read", name)
		}
		switch {
		case field.number == kmeansSettingsField && field.kind == protowire.BytesType:
			err = vectorSettings(&spec, field.bytes)
		case field.number == kmeansClustersField && field.kind == protowire.VarintType:
			spec.Clusters = field.value
		case field.number == kmeansLevelsField && field.kind == protowire.VarintType:
			spec.Levels = field.value
		default:
			err = fmt.Errorf("its vector settings carry field %d, which this build of Ptah does not read", field.number)
		}
		if err != nil {
			return nil, err
		}
	}
	return &spec, nil
}

// vectorSettings reads a VectorIndexSettings into spec.
func vectorSettings(spec *ydbschema.VectorSettings, data []byte) error {
	fields, err := splitFields(data)
	if err != nil {
		return fmt.Errorf("its vector settings do not parse: %w", err)
	}
	for _, field := range fields {
		if field.kind != protowire.VarintType {
			return fmt.Errorf("its vector settings carry field %d as wire type %d, which this build of Ptah does "+
				"not read", field.number, field.kind)
		}
		switch field.number {
		case vectorMetricField:
			metric, known := vectorMetrics[field.value]
			if !known {
				return fmt.Errorf("its vector index has metric %d, which this build of Ptah does not read", field.value)
			}
			spec.Distance, spec.Similarity = metric.Distance, metric.Similarity
		case vectorTypeField:
			elementType, known := vectorElementTypes[field.value]
			if !known {
				return fmt.Errorf("its vector index has vector type %d, which this build of Ptah does not read", field.value)
			}
			spec.VectorType = elementType
		case vectorDimensionField:
			spec.Dimension = field.value
		default:
			return fmt.Errorf("its vector settings carry field %d, which this build of Ptah does not read", field.number)
		}
	}
	return nil
}

// vectorFacets records a vector index's settings as the YDB owner's
// observation, bound to YDB. Settings the owner cannot hold refuse the read
// rather than becoming a value that claims another index.
func vectorFacets(settings ydbschema.VectorSettings) (schemaext.Facets, error) {
	observed := new(ydbschema.ObservedVectorIndex(settings))
	if err := ydbschema.ValidateObservedVectorIndex(observed); err != nil {
		return schemaext.Facets{}, err
	}
	facets, err := schemaext.NewFacets(observed)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.WithTargetScope(ydbschema.VectorIndexKind, platform.YDB)
}

// vectorCoverage records complete knowledge of the vector settings of every
// index the read returned: a vector index carries its settings, and an index
// of another kind has none. An index the read did not return is not known.
func vectorCoverage(db *catalog.Database) error {
	identities := objectidentity.NewBuilder(identifier.ForDialect(platform.YDB))
	subjects := make([]schemaext.SubjectCoverage, 0, len(db.Indexes))
	for _, index := range db.Indexes {
		subjects = append(subjects, schemaext.SubjectCoverage{
			Kind: ydbschema.VectorIndexKind, Subject: identities.IndexParts(index.Schema, index.TableName, index.Name),
			Knowledge: schemaext.Knowledge{State: schemaext.Complete},
		})
	}
	known, err := ydbschema.VectorIndexCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned indexes have inspected YDB vector settings"}, subjects)
	if err != nil {
		return fmt.Errorf("failed to record vector index coverage: %w", err)
	}
	db.FeatureCoverage, err = db.FeatureCoverage.Combine(known)
	return err
}
