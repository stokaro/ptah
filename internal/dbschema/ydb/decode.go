package ydb

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbgap"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbtype"
)

// table adds one described row table, its key, its indexes and the records
// of the settings Ptah does not model.
func (r *Reader) table(
	ctx context.Context,
	source Source,
	schema, name string,
	described *Ydb_Table.DescribeTableResult,
	db *catalog.Database,
) error {
	subject := fmt.Sprintf("YDB table %s", r.absolute(schema, name))
	if described.GetStoreType() == Ydb_Table.StoreType_STORE_TYPE_COLUMN {
		// The scheme service lists a column table as one, so a row table that
		// describes itself as column-oriented is a server this reader does
		// not understand.
		return fmt.Errorf("%s is listed as a row table and describes itself as a column table", subject)
	}

	key := described.GetPrimaryKey()
	// YDB keeps row counts in .sys/partition_stats, which the reader does not
	// read, so the description has no row count to give.
	table := catalog.Table{Name: name, Schema: schema, Type: "TABLE", RowStatsUnknown: true}
	for position, meta := range described.GetColumns() {
		column, err := r.column(meta, position+1)
		if err != nil {
			return fmt.Errorf("%s: %w", subject, err)
		}
		column.IsPrimaryKey = slices.Contains(key, meta.GetName())
		table.Columns = append(table.Columns, column)
	}
	db.Tables = append(db.Tables, table)

	if len(key) > 0 {
		// YDB names no key, so the constraint carries the name a renderer
		// would give it. It is a constraint rather than only a flag on the
		// columns because a plan that drops a key column has to see the key
		// to refuse it.
		db.Constraints = append(db.Constraints, catalog.Constraint{
			Name:        name + "_pkey",
			TableName:   name,
			Schema:      schema,
			Type:        "PRIMARY KEY",
			ColumnName:  key[0],
			ColumnNames: slices.Clone(key),
		})
	}

	for _, described := range described.GetIndexes() {
		index, err := r.index(ctx, source, schema, name, described)
		if err != nil {
			return fmt.Errorf("%s: %w", subject, err)
		}
		db.Indexes = append(db.Indexes, index)
	}

	db.NotDescribed = db.NotDescribed.With(unmodeledSettings(schema, name, described)...)
	return nil
}

// column decodes one column. A column whose type is not wrapped in Optional is
// NOT NULL; a column filled from a sequence is a Serial column and is reported
// as its integer type with IsAutoIncrement set, which is how the comparison
// reads an incrementing integer on either side, and with the start and the
// increment its sequence has; see [sequence].
func (r *Reader) column(meta *Ydb_Table.ColumnMeta, position int) (catalog.Column, error) {
	if unknown := unknownFields(meta); len(unknown) > 0 {
		// A default of a kind the pinned protocol buffers do not model arrives
		// here, with no default set, and so would a column setting a newer
		// YDB added; read as known, the column would lose it in silence.
		return catalog.Column{}, fmt.Errorf("column %q carries field %s of its description, which this build of "+
			"Ptah does not read", meta.GetName(), joinNumbers(unknown))
	}
	ydbType, nullable, err := columnType(meta.GetType())
	if err != nil {
		return catalog.Column{}, fmt.Errorf("column %q: %w", meta.GetName(), err)
	}
	if meta.GetNotNull() {
		nullable = false
	}
	column := catalog.Column{
		Name:            meta.GetName(),
		DataType:        ydbType,
		ColumnType:      ydbType,
		IsNullable:      "YES",
		OrdinalPosition: position,
	}
	if !nullable {
		column.IsNullable = "NO"
	}
	if precision, scale, ok := decimalOf(meta.GetType()); ok {
		column.NumericPrecision = new(precision)
		column.NumericScale = new(scale)
	}

	switch {
	case meta.GetFromSequence() != nil:
		column.IsAutoIncrement = true
		if err := sequence(&column, ydbType, meta.GetFromSequence()); err != nil {
			return catalog.Column{}, fmt.Errorf("column %q: %w", meta.GetName(), err)
		}
	case meta.GetFromLiteral() != nil:
		literal, err := defaultLiteral(ydbType, meta.GetFromLiteral())
		if err != nil {
			return catalog.Column{}, fmt.Errorf("column %q: %w", meta.GetName(), err)
		}
		column.ColumnDefault = new(literal)
	}
	return column, nil
}

// unknownFields lists the numbers of the fields a message carries that the
// pinned protocol buffers do not model, in the order they arrived.
func unknownFields(message protoreflect.ProtoMessage) []protowire.Number {
	var numbers []protowire.Number
	unknown := message.ProtoReflect().GetUnknown()
	for len(unknown) > 0 {
		number, wireType, length := protowire.ConsumeTag(unknown)
		if length < 0 {
			break
		}
		numbers = append(numbers, number)
		unknown = unknown[length:]
		valueLength := protowire.ConsumeFieldValue(number, wireType, unknown)
		if valueLength < 0 {
			break
		}
		unknown = unknown[valueLength:]
	}
	return numbers
}

// joinNumbers writes field numbers as a reader of an error expects them.
func joinNumbers(numbers []protowire.Number) string {
	written := make([]string, len(numbers))
	for i, number := range numbers {
		written[i] = strconv.Itoa(int(number))
	}
	return strings.Join(written, ", ")
}

// primitiveTypes are the YDB names of the primitive types a row table column
// may have.
var primitiveTypes = map[Ydb.Type_PrimitiveTypeId]string{
	Ydb.Type_BOOL:          ydbtype.Bool,
	Ydb.Type_INT8:          ydbtype.Int8,
	Ydb.Type_UINT8:         ydbtype.Uint8,
	Ydb.Type_INT16:         ydbtype.Int16,
	Ydb.Type_UINT16:        ydbtype.Uint16,
	Ydb.Type_INT32:         ydbtype.Int32,
	Ydb.Type_UINT32:        ydbtype.Uint32,
	Ydb.Type_INT64:         ydbtype.Int64,
	Ydb.Type_UINT64:        ydbtype.Uint64,
	Ydb.Type_FLOAT:         ydbtype.Float,
	Ydb.Type_DOUBLE:        ydbtype.Double,
	Ydb.Type_DATE:          ydbtype.Date,
	Ydb.Type_DATETIME:      ydbtype.Datetime,
	Ydb.Type_TIMESTAMP:     ydbtype.Timestamp,
	Ydb.Type_INTERVAL:      ydbtype.Interval,
	Ydb.Type_DATE32:        ydbtype.Date32,
	Ydb.Type_DATETIME64:    ydbtype.Datetime64,
	Ydb.Type_TIMESTAMP64:   ydbtype.Timestamp64,
	Ydb.Type_INTERVAL64:    ydbtype.Interval64,
	Ydb.Type_STRING:        ydbtype.String,
	Ydb.Type_UTF8:          ydbtype.Utf8,
	Ydb.Type_YSON:          ydbtype.Yson,
	Ydb.Type_JSON:          ydbtype.JSON,
	Ydb.Type_UUID:          ydbtype.UUID,
	Ydb.Type_JSON_DOCUMENT: ydbtype.JSONDocument,
	Ydb.Type_DYNUMBER:      ydbtype.DyNumber,
}

// columnType reads a column's type: the YDB spelling, and whether the type is
// Optional. A type a row table cannot hold, or one the pinned protocol buffers
// do not know, is refused rather than spelled some other way.
func columnType(t *Ydb.Type) (ydbType string, nullable bool, _ error) {
	if optional := t.GetOptionalType(); optional != nil {
		inner, innerNullable, err := columnType(optional.GetItem())
		if err != nil {
			return "", false, err
		}
		if innerNullable {
			return "", false, fmt.Errorf("its type %s is Optional twice, which a table column cannot be", inner)
		}
		return inner, true, nil
	}
	if decimal := t.GetDecimalType(); decimal != nil {
		return fmt.Sprintf("Decimal(%d,%d)", decimal.GetPrecision(), decimal.GetScale()), false, nil
	}
	if t.GetType() == nil {
		return "", false, fmt.Errorf("its type has no value Ptah can read")
	}
	if _, primitive := t.GetType().(*Ydb.Type_TypeId); !primitive {
		return "", false, fmt.Errorf("its type %T is not a type Ptah reads in a table column", t.GetType())
	}
	name, known := primitiveTypes[t.GetTypeId()]
	if !known {
		return "", false, fmt.Errorf("its type %s is not a type Ptah reads in a table column", t.GetTypeId())
	}
	return name, false, nil
}

// decimalOf reads the precision and scale of a Decimal column.
func decimalOf(t *Ydb.Type) (precision, scale int, ok bool) {
	if optional := t.GetOptionalType(); optional != nil {
		t = optional.GetItem()
	}
	decimal := t.GetDecimalType()
	if decimal == nil {
		return 0, 0, false
	}
	return int(decimal.GetPrecision()), int(decimal.GetScale()), true
}

// index decodes one index. A row table's global index is reported with its
// kind in Method, `GLOBAL SYNC` or `GLOBAL ASYNC`, and its uniqueness in
// IsUnique, which is how internal/ydbindex reads the catalog side. Any other
// kind is refused: one the pinned protocol buffers model as a oneof Ptah does
// not read yet, and one they do not model at all, whose type arrives empty and
// whose data sits in fields they do not know.
//
// The index's partitioning is read from its implementation table, which the
// description of the table leaves out: measured on 25.1.4.7 and 26.2.1.14, a
// global index whose minimum partition count was set to 4 describes itself as
// `globalIndex: {}`, and `<table>/<index>/indexImplTable` describes the 4.
func (r *Reader) index(
	ctx context.Context,
	source Source,
	schema, table string,
	described *Ydb_Table.TableIndexDescription,
) (catalog.Index, error) {
	index := catalog.Index{
		Name:           described.GetName(),
		TableName:      table,
		Schema:         schema,
		Columns:        slices.Clone(described.GetIndexColumns()),
		IncludeColumns: slices.Clone(described.GetDataColumns()),
	}
	var kind ydbindex.Kind
	switch described.GetType().(type) {
	case *Ydb_Table.TableIndexDescription_GlobalIndex:
		kind = ydbindex.Sync
	case *Ydb_Table.TableIndexDescription_GlobalUniqueIndex:
		kind = ydbindex.Sync
		index.IsUnique = true
	case *Ydb_Table.TableIndexDescription_GlobalAsyncIndex:
		kind = ydbindex.Async
	default:
		return catalog.Index{}, fmt.Errorf("index %q is a %s: %s", described.GetName(),
			unreadIndexKind(described), ydbgap.IndexFamilies.Message())
	}
	implementation, err := source.DescribeTable(ctx, r.absolute(schema, path.Join(table, described.GetName(), indexImplTable)))
	if err != nil {
		return catalog.Index{}, fmt.Errorf("index %q: %w", described.GetName(), err)
	}
	settings, err := indexSettings(implementation)
	if err != nil {
		return catalog.Index{}, fmt.Errorf("index %q: %w", described.GetName(), err)
	}
	index.Method = kind.Clause(false)
	index.Partitioning = settings.Spec()
	index.Definition = indexClause(index, kind)
	return index, nil
}

// indexImplTable is the table YDB keeps a global index in, under the index's
// own path: `<table>/<index>/indexImplTable`.
const indexImplTable = "indexImplTable"

// indexSettings reads the partitioning and the read replicas of a global
// index from the description of its implementation table. A setting the
// description leaves unspecified is YDB's default for it, and a setting the
// pinned protocol buffers do not model is refused, because read as absent it
// would be planned away on every run.
func indexSettings(described *Ydb_Table.DescribeTableResult) (ydbindex.Settings, error) {
	partitioning := described.GetPartitioningSettings()
	replicas := described.GetReadReplicasSettings()
	for _, message := range []protoreflect.ProtoMessage{partitioning, replicas} {
		if unknown := unknownFields(message); len(unknown) > 0 {
			return ydbindex.Settings{}, fmt.Errorf("its partitioning carries field %s, which this build of Ptah does not read",
				joinNumbers(unknown))
		}
	}
	settings := ydbindex.DefaultSettings()
	var err error
	if settings.BySize, err = featureFlag(partitioning.GetPartitioningBySize(), settings.BySize); err != nil {
		return ydbindex.Settings{}, fmt.Errorf("partitioning by size: %w", err)
	}
	if settings.ByLoad, err = featureFlag(partitioning.GetPartitioningByLoad(), settings.ByLoad); err != nil {
		return ydbindex.Settings{}, fmt.Errorf("partitioning by load: %w", err)
	}
	switch size := partitioning.GetPartitionSizeMb(); {
	case !settings.BySize:
		settings.PartitionSizeMB = 0
	case size != 0:
		settings.PartitionSizeMB = size
	}
	if minimum := partitioning.GetMinPartitionsCount(); minimum != 0 {
		settings.MinPartitions = minimum
	}
	settings.MaxPartitions = partitioning.GetMaxPartitionsCount()
	switch {
	case replicas.GetPerAzReadReplicasCount() != 0:
		settings.ReadReplicas = ydbindex.Replicas{PerAZ: true, Count: replicas.GetPerAzReadReplicasCount()}
	case replicas.GetAnyAzReadReplicasCount() != 0:
		settings.ReadReplicas = ydbindex.Replicas{Count: replicas.GetAnyAzReadReplicasCount()}
	}
	return settings, nil
}

// featureFlag reads a setting YDB reports as enabled, disabled or left
// unspecified, which is the default.
func featureFlag(flag Ydb.FeatureFlag_Status, unspecified bool) (bool, error) {
	switch flag {
	case Ydb.FeatureFlag_ENABLED:
		return true, nil
	case Ydb.FeatureFlag_DISABLED:
		return false, nil
	case Ydb.FeatureFlag_STATUS_UNSPECIFIED:
		return unspecified, nil
	default:
		return false, fmt.Errorf("the value %d is not one this build of Ptah reads", int32(flag))
	}
}

// unreadIndexFields names the index kinds by the field number ydb_table.proto
// gives each in TableIndexDescription's type oneof.
var unreadIndexFields = map[protowire.Number]string{
	9:  "vector_kmeans_tree index",
	10: "fulltext_plain index",
	11: "fulltext_relevance index",
	12: "bloom_filter index",
	13: "bloom_ngram_filter index",
	14: "global JSON index",
	15: "min_max index",
}

// unreadIndexKind names the kind of an index the reader does not read, from
// the first field of the description it does not know.
func unreadIndexKind(described *Ydb_Table.TableIndexDescription) string {
	if described.GetType() != nil {
		return fmt.Sprintf("%T index", described.GetType())
	}
	for _, number := range unknownFields(described) {
		if kind, named := unreadIndexFields[number]; named {
			return kind
		}
	}
	return "index of a kind this build of Ptah does not know"
}

// indexClause writes the index the way YQL declares it, for a reader of the
// description.
func indexClause(index catalog.Index, kind ydbindex.Kind) string {
	var b strings.Builder
	b.WriteString("INDEX " + sqlident.Quote("ydb", index.Name) + " " + kind.Clause(index.IsUnique) + " ON (")
	b.WriteString(quotedList(index.Columns))
	b.WriteString(")")
	if len(index.IncludeColumns) > 0 {
		b.WriteString(" COVER (" + quotedList(index.IncludeColumns) + ")")
	}
	return b.String()
}

func quotedList(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = sqlident.Quote("ydb", name)
	}
	return strings.Join(quoted, ", ")
}

// unmodeledSettings records the table settings Ptah does not model yet. A
// setting is recorded where it differs from what a table created without one
// carries, measured on local-ydb 26.2.1.14: one column family, `default`,
// uncompressed and with no pool of its own; partitioning by size at 2048 MB,
// not by load, with at least one partition; no read replicas, no key bloom
// filter, and external blobs off with no storage pools named.
func unmodeledSettings(schema, name string, described *Ydb_Table.DescribeTableResult) []coverage.Object {
	var records []coverage.Object
	if described.GetTtlSettings() != nil || described.GetTiering() != "" {
		records = append(records, unmodeled(coverage.TTL, schema, name))
	}
	for _, feed := range described.GetChangefeeds() {
		records = append(records, coverage.Object{
			Kind:       coverage.Changefeed,
			Name:       tableref.Canonical(schema, name+"/"+feed.GetName()),
			Reason:     coverage.Unsupported,
			Provenance: coverage.Observed,
		})
	}
	if hasColumnFamilies(described) {
		records = append(records, unmodeled(coverage.ColumnFamily, schema, name))
	}
	if hasTableOptions(described) {
		records = append(records, unmodeled(coverage.TableOption, schema, name))
	}
	return records
}

// hasColumnFamilies reports a family layout other than the default one. A
// table lists every family its columns name, and family names are unique, so
// any family but a plain `default` is enough to tell.
func hasColumnFamilies(described *Ydb_Table.DescribeTableResult) bool {
	for _, family := range described.GetColumnFamilies() {
		if family.GetName() != "default" || family.GetData() != nil ||
			family.GetKeepInMemory() == Ydb.FeatureFlag_ENABLED {
			return true
		}
		switch family.GetCompression() {
		case Ydb_Table.ColumnFamily_COMPRESSION_UNSPECIFIED, Ydb_Table.ColumnFamily_COMPRESSION_NONE:
		default:
			return true
		}
	}
	return false
}

// hasTableOptions reports partitioning, read replica, key bloom filter or
// storage settings other than a new table's.
func hasTableOptions(described *Ydb_Table.DescribeTableResult) bool {
	partitioning := described.GetPartitioningSettings()
	if len(partitioning.GetPartitionBy()) > 0 ||
		partitioning.GetPartitioningBySize() == Ydb.FeatureFlag_DISABLED ||
		(partitioning.GetPartitionSizeMb() != 0 && partitioning.GetPartitionSizeMb() != defaultPartitionSizeMB) ||
		partitioning.GetPartitioningByLoad() == Ydb.FeatureFlag_ENABLED ||
		partitioning.GetMinPartitionsCount() > 1 ||
		partitioning.GetMaxPartitionsCount() != 0 {
		return true
	}
	replicas := described.GetReadReplicasSettings()
	if replicas.GetPerAzReadReplicasCount() != 0 || replicas.GetAnyAzReadReplicasCount() != 0 {
		return true
	}
	if described.GetKeyBloomFilter() == Ydb.FeatureFlag_ENABLED {
		return true
	}
	storage := described.GetStorageSettings()
	return storage.GetTabletCommitLog0() != nil || storage.GetTabletCommitLog1() != nil ||
		storage.GetExternal() != nil || storage.GetStoreExternalBlobs() == Ydb.FeatureFlag_ENABLED
}

// defaultPartitionSizeMB is the partition size YDB gives a table that names
// none.
const defaultPartitionSizeMB = 2048
