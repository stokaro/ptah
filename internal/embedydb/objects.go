package embedydb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"path"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/ydbindex"
)

// ErrUnsupported is a specification or an operation YDB cannot hold, or one
// the connected server line lacks the capability for. Every refusal wrapping
// it says which.
var ErrUnsupported = errors.New("not supported on YDB")

// FloatRepresentation is the representation a YDB generation stores: the
// float element type of a YDB vector index, which is what an embedding
// provider's float32 answer fills. YDB's uint8, int8 and bit element types
// store a quantized vector, and nothing here quantizes one.
const FloatRepresentation = "float"

// ColumnType is the YQL type of a generation's vector column. YDB stores a
// vector as the bytes Knn::ToBinaryStringFloat produces, in a String column.
const ColumnType = "String"

// floatTag is the byte Knn::ToBinaryStringFloat ends a float vector with,
// after one little-endian float32 per element. Measured on 25.1.4.7 and
// 26.2.1.14: [1.0, -2.5, 3.25] serializes as 0000803f 000020c0 00005040 01.
const floatTag = 0x01

// The index options a vector_kmeans_tree index takes from a specification:
// the depth of the tree and the clusters each level splits into.
const (
	optionLevels   = ydbindex.AttributeLevels
	optionClusters = ydbindex.AttributeClusters
)

// TargetObjects derives the column and index a generation needs on YDB, or
// says why YDB cannot hold the specification.
//
// The column is String and nullable: a row without its vector is the normal
// state during a backfill. The index is a vector_kmeans_tree with the
// distance or similarity the metric names, the float element type, the
// dimension the provider reports, and the levels and clusters the
// specification's index options give; every one is required, because YDB
// 25.3 and later refuse an index that leaves one out.
func TargetObjects(spec embedgen.Spec) (embedgen.TargetObjects, error) {
	if err := validateTarget(spec); err != nil {
		return embedgen.TargetObjects{}, err
	}
	objects := embedgen.TargetObjects{
		Column: vectorColumn(spec),
		// The own-table layout keeps no reference to the source: YDB has no
		// foreign keys, so the name the PostgreSQL layout gives one stays
		// empty here.
		OwnsTable: spec.Target.Layout.OwnsTable(),
	}
	if objects.OwnsTable {
		objects.TableComment = embedgen.TableComment(spec.Identity().Digest)
	}
	if strings.TrimSpace(spec.Target.IndexMethod) == "" {
		return objects, nil
	}
	settings, err := VectorSettings(spec)
	if err != nil {
		return embedgen.TargetObjects{}, err
	}
	objects.Index.StructName = spec.Target.Table
	objects.Index.Name = embedgen.IndexName(spec.Target.Table, spec.Target.Column, spec.Identity().Digest)
	objects.Index.Fields = []string{spec.Target.Column}
	objects.Index.Type = ydbindex.VectorMethod
	objects.Index.Operator = MetricSetting(settings)
	objects.HasIndex = true
	return objects, nil
}

// vectorColumn is the vector column a generation writes.
func vectorColumn(spec embedgen.Spec) schemamodel.Field {
	return schemamodel.Field{
		StructName: spec.Target.Table,
		Name:       spec.Target.Column,
		Type:       ColumnType,
		Nullable:   true,
		Comment:    spec.ColumnComment(),
	}
}

// validateTarget refuses a target YDB cannot store or search.
func validateTarget(spec embedgen.Spec) error {
	switch {
	case strings.TrimSpace(spec.Target.Table) == "":
		return fmt.Errorf("target objects: the specification names no target table")
	case strings.TrimSpace(spec.Target.Column) == "":
		return fmt.Errorf("target objects: the specification names no target column")
	case !strings.EqualFold(strings.TrimSpace(spec.Target.Representation), FloatRepresentation):
		return fmt.Errorf("%w: target objects: representation %q is not one a YDB generation stores. "+
			"YDB keeps a vector as the bytes of its float element type, so set target.representation "+
			"to %q; YDB's uint8, int8 and bit types hold quantized vectors, which an embedding provider "+
			"does not return", ErrUnsupported, spec.Target.Representation, FloatRepresentation)
	case !embedgen.KnownLayout(spec.Target.Layout):
		return fmt.Errorf("target objects: %q is not a target layout", string(spec.Target.Layout))
	case spec.Chunks() && !spec.Target.Layout.OwnsTable():
		return fmt.Errorf(
			"target objects: the specification splits a row into a set of chunks, and layout %q "+
				"stores one vector per source row. A set needs a relation of its own: set "+
				"target.layout to %q",
			string(embedgen.LayoutSourceColumns), string(embedgen.LayoutOwnTable))
	case spec.Target.Layout.OwnsTable() && directory(spec.Target.Schema) == directory(spec.Source.Schema) &&
		strings.TrimSpace(spec.Target.Table) == strings.TrimSpace(spec.Source.Table):
		return fmt.Errorf(
			"target objects: layout %q names the source table %s as its target, and under this layout "+
				"Ptah creates that table and drops it when the generation is retired. Name a table of the "+
				"generation's own, or use the default layout to put the columns on the source",
			string(embedgen.LayoutOwnTable), tablePath(spec.Target.Schema, spec.Target.Table))
	case spec.Model.ReportedDimension <= 0:
		return fmt.Errorf(
			"target objects: the model reports no output dimension yet; run the provider test first")
	}
	return nil
}

// VectorSettings derives a generation's vector index settings from its
// specification.
//
// The metric maps onto YDB's names: cosine is distance=cosine, l2 is
// distance=euclidean, and inner_product is similarity=inner_product. The
// options are levels and clusters and nothing else; a pgvector build parameter
// is refused by the method it belongs to, and an unknown option by name.
func VectorSettings(spec embedgen.Spec) (ydbschema.VectorSettings, error) {
	method := strings.ToLower(strings.TrimSpace(spec.Target.IndexMethod))
	if method != ydbindex.VectorMethod {
		return ydbschema.VectorSettings{}, fmt.Errorf(
			"%w: index method %q is not one YDB builds; a YDB vector index is %s",
			ErrUnsupported, spec.Target.IndexMethod, ydbindex.VectorMethod)
	}
	settings := ydbschema.VectorSettings{
		VectorType: FloatRepresentation,
		Dimension:  uint64(max(spec.Model.ReportedDimension, 0)),
	}
	switch spec.Target.Metric {
	case embedgen.MetricCosine:
		settings.Distance = "cosine"
	case embedgen.MetricL2:
		settings.Distance = "euclidean"
	case embedgen.MetricInnerProduct:
		settings.Similarity = "inner_product"
	default:
		return ydbschema.VectorSettings{}, fmt.Errorf("%w: metric %q has no YDB vector index setting",
			ErrUnsupported, spec.Target.Metric)
	}
	for _, name := range slices.Sorted(maps.Keys(spec.Target.IndexOptions)) {
		value := strings.TrimSpace(spec.Target.IndexOptions[name])
		switch strings.ToLower(strings.TrimSpace(name)) {
		case optionLevels:
			count, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return ydbschema.VectorSettings{}, fmt.Errorf("index option %s is %q, not a whole number", name, value)
			}
			settings.Levels = count
		case optionClusters:
			count, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return ydbschema.VectorSettings{}, fmt.Errorf("index option %s is %q, not a whole number", name, value)
			}
			settings.Clusters = count
		default:
			return ydbschema.VectorSettings{}, fmt.Errorf("%w: %s", ErrUnsupported,
				ydbindex.StorageParameterRefusal(name))
		}
	}
	if settings.Levels == 0 || settings.Clusters == 0 {
		return ydbschema.VectorSettings{}, fmt.Errorf(
			"a %s index needs index_options %s and %s: YDB 25.3 and later refuse an index without "+
				"either, and 25.1 reports back no default for them", ydbindex.VectorMethod, optionLevels, optionClusters)
	}
	return ydbindex.ResolveVector(&settings, "")
}

// MetricSetting is the metric a vector index orders by, written as its YQL
// setting: `distance=cosine`, `distance=euclidean` or
// `similarity=inner_product`. It is what verification compares a built index
// with, the YDB counterpart of a pgvector operator class.
func MetricSetting(settings ydbschema.VectorSettings) string {
	if settings.Similarity != "" {
		return ydbindex.AttributeSimilarity + "=" + settings.Similarity
	}
	return ydbindex.AttributeDistance + "=" + settings.Distance
}

// EncodeVector writes a vector the way Knn::ToBinaryStringFloat does: each
// element as a little-endian float32, then the float tag.
//
// A non-finite element is refused. A YDB String column takes any bytes, so the
// writer is what keeps a NaN or an infinity out of a corpus, and verification
// relies on that.
func EncodeVector(vector []float32) ([]byte, error) {
	encoded := make([]byte, 0, len(vector)*4+1)
	for index, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, fmt.Errorf("vector element %d is %v, which a stored vector cannot hold", index, value)
		}
		encoded = binary.LittleEndian.AppendUint32(encoded, math.Float32bits(value))
	}
	return append(encoded, floatTag), nil
}

// directory is a schema as YDB names it: a directory relative to the
// database root, without leading or trailing slashes.
func directory(schema string) string {
	return strings.Trim(strings.TrimSpace(schema), "/")
}

// tablePath is a table's path relative to the database root.
func tablePath(schema, table string) string {
	return path.Join(directory(schema), strings.TrimSpace(table))
}

// quoteTable renders a table's path as one YQL identifier.
func quoteTable(schema, table string) string {
	return sqlutil.QuoteYQLIdentifier(tablePath(schema, table))
}

// quote renders a column or index name as a YQL identifier.
func quote(name string) string {
	return sqlutil.QuoteYQLIdentifier(name)
}
