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
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbcomment"
	"ptah.run/internal/ydbgap"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
	"ptah.run/internal/ydbttl"
	"ptah.run/internal/ydbtype"
)

// table adds one described row table, its key, its indexes and the records
// of the settings Ptah does not model.
func (r *Reader) table(
	ctx context.Context,
	source Source,
	schema, name string,
	described *Ydb_Table.DescribeTableResult,
	columnTable *ydbcolumn.Description,
	db *catalog.Database,
) error {
	subject := fmt.Sprintf("YDB table %s", r.absolute(schema, name))
	if described.GetStoreType() == Ydb_Table.StoreType_STORE_TYPE_COLUMN && columnTable == nil {
		// The scheme service lists a column table as one, so a row table that
		// describes itself as column-oriented is a server this reader does
		// not understand.
		return fmt.Errorf("%s is listed as a row table and describes itself as a column table", subject)
	}

	if columnTable != nil && (columnTable.Spec == nil || described.GetStoreType() != Ydb_Table.StoreType_STORE_TYPE_COLUMN || !slices.Equal(columnTable.PrimaryKey, described.GetPrimaryKey())) {
		return fmt.Errorf("%s: monitoring and table-service descriptions disagree about column storage or the primary key; retry the read after concurrent schema changes finish", subject)
	}
	key := described.GetPrimaryKey()
	comments := r.comments(described.GetAttributes())
	// YDB keeps row counts in .sys/partition_stats, which the reader does not
	// read, so the description has no row count to give.
	table := catalog.Table{Name: name, Schema: schema, Type: "TABLE", RowStatsUnknown: true, Comment: comments.Own}
	for position, meta := range described.GetColumns() {
		column, err := r.column(meta, position+1)
		if err != nil {
			return fmt.Errorf("%s: %w", subject, err)
		}
		column.IsPrimaryKey = slices.Contains(key, meta.GetName())
		column.Comment = comments.Columns[meta.GetName()]
		table.Columns = append(table.Columns, column)
	}
	changefeeds, unread, err := r.changefeeds(ctx, source, schema, name, described)
	if err != nil {
		return fmt.Errorf("%s: %w", subject, err)
	}
	table.Changefeeds = changefeeds
	if columnTable == nil || columnTable.Spec.TTL == nil {
		policy, err := rowDeletionPolicy(described.GetTtlSettings())
		if err != nil {
			return fmt.Errorf("%s: %w", subject, err)
		}
		table.RowDeletionPolicy = policy
	}
	if columnTable != nil {
		table.YDBColumnTable = columnTable.Spec.Clone()
		for _, index := range columnTable.Indexes {
			db.Indexes = append(db.Indexes, catalog.Index{Name: index.Name, TableName: name, Schema: schema, Type: index.Method, Method: index.Method, Columns: index.Columns, StorageParams: index.Options, Comment: comments.Indexes[index.Name]})
		}
	} else {
		families, familiesRead := r.columnFamilies(described)
		table.YDBColumnFamilies = families
		if !familiesRead {
			db.NotDescribed = db.NotDescribed.With(unmodeled(coverage.ColumnFamily, schema, name))
		}
		settings, err := tableSettings(described)
		if err != nil {
			return fmt.Errorf("%s: %w", subject, err)
		}
		table.YDBPartitioning = ydbpartition.TableSpec(settings)
	}
	db.Tables = append(db.Tables, table)
	db.NotDescribed = db.NotDescribed.With(unread...)

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
		index.Comment = comments.Indexes[index.Name]
		db.Indexes = append(db.Indexes, index)
	}

	db.NotDescribed = db.NotDescribed.With(unmodeledSettings(schema, name, described)...)
	r.tableAccess(schema, name, described.GetSelf(), db)
	return nil
}

// comments reads the comments a table's user attributes hold, on a target
// with [capability.CommentAttributes]; see [ydbcomment.Read]. An attribute
// under no key of Ptah's is not a comment and is left alone: a plan neither
// reads nor removes it. A comment under the key of a column or an index the
// table does not have is not read either, because no object in the
// description could carry it; YDB keeps the attribute after the column or
// the index is dropped, and a plan that drops one removes its comment.
func (r *Reader) comments(attributes map[string]string) ydbcomment.Comments {
	if !r.caps.Has(capability.CommentAttributes) {
		return ydbcomment.Comments{}
	}
	return ydbcomment.Read(attributes)
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
// IsUnique, which is how internal/ydbindex reads the catalog side. A vector
// index, which the pinned protocol buffers do not model, is decoded from the
// field it arrives in, and reported with `GLOBAL USING vector_kmeans_tree` in
// Method and its settings in Vector; see [vectorIndex]. Any other kind is
// refused: one the pinned protocol buffers model as a oneof Ptah does not read
// yet, and one they do not model at all, whose type arrives empty and whose
// data sits in fields they do not know.
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
	case nil:
		fullTextKind, options, err := fullTextIndex(described)
		if err != nil {
			return catalog.Index{}, fmt.Errorf("index %q: %w", described.GetName(), err)
		}
		if fullTextKind.IsFullText() {
			index.Method = fullTextKind.Clause(false)
			index.StorageParams = options
			index.Definition = indexClause(index, fullTextKind) + " " + ydbindex.FullTextClause(options)
			return index, nil
		}
		vector, isVector, err := vectorIndex(described)
		switch {
		case err != nil:
			return catalog.Index{}, fmt.Errorf("index %q: %w", described.GetName(), err)
		case !isVector:
			return catalog.Index{}, fmt.Errorf("index %q is a %s: %s", described.GetName(),
				unreadIndexKind(described), ydbgap.IndexFamilies.Message())
		}
		index.Method = ydbindex.Vector.Clause(false)
		index.Vector = vector
		index.Definition = indexClause(index, ydbindex.Vector) + " " + ydbindex.VectorClause(*vector)
		return index, nil
	default:
		return catalog.Index{}, fmt.Errorf("index %q is a %s: %s", described.GetName(),
			unreadIndexKind(described), ydbgap.IndexFamilies.Message())
	}
	implementation, err := source.DescribeTable(ctx, r.absolute(schema, path.Join(table, described.GetName(), indexImplTable)))
	if err != nil {
		return catalog.Index{}, fmt.Errorf("index %q: %w", described.GetName(), err)
	}
	settings, err := partitionSettings(implementation)
	if err != nil {
		return catalog.Index{}, fmt.Errorf("index %q: %w", described.GetName(), err)
	}
	index.Method = kind.Clause(false)
	index.Partitioning = ydbindex.Spec(settings)
	index.Definition = indexClause(index, kind)
	return index, nil
}

// indexImplTable is the table YDB keeps a global index in, under the index's
// own path: `<table>/<index>/indexImplTable`.
const indexImplTable = "indexImplTable"

// tableSettings reads a row table's settings: its partitioning and read
// replicas, see [partitionSettings], and its key bloom filter. Measured on
// 25.1.4.7 and 26.2.1.14, a new table describes its key bloom filter as
// unspecified and one created with KEY_BLOOM_FILTER = DISABLED as disabled,
// and both keep no filter. YDB keeps no record of the partitions a table was
// created with, so they are not read.
func tableSettings(described *Ydb_Table.DescribeTableResult) (ydbpartition.TableSettings, error) {
	settings, err := partitionSettings(described)
	if err != nil {
		return ydbpartition.TableSettings{}, err
	}
	filter, err := featureFlag(described.GetKeyBloomFilter(), false)
	if err != nil {
		return ydbpartition.TableSettings{}, fmt.Errorf("its key bloom filter: %w", err)
	}
	return ydbpartition.TableSettings{Settings: settings, KeyBloomFilter: filter}, nil
}

// partitionSettings reads the partitioning and the read replicas of a table,
// or of a global index from the description of its implementation table. A
// setting the description leaves unspecified is YDB's default for it, and a
// setting the pinned protocol buffers do not model is refused, because read as
// absent it would be planned away on every run.
func partitionSettings(described *Ydb_Table.DescribeTableResult) (ydbpartition.Settings, error) {
	partitioning := described.GetPartitioningSettings()
	replicas := described.GetReadReplicasSettings()
	for _, message := range []protoreflect.ProtoMessage{partitioning, replicas} {
		if unknown := unknownFields(message); len(unknown) > 0 {
			return ydbpartition.Settings{}, fmt.Errorf("its partitioning carries field %s, which this build of Ptah does not read",
				joinNumbers(unknown))
		}
	}
	settings := ydbpartition.DefaultSettings()
	var err error
	if settings.BySize, err = featureFlag(partitioning.GetPartitioningBySize(), settings.BySize); err != nil {
		return ydbpartition.Settings{}, fmt.Errorf("partitioning by size: %w", err)
	}
	if settings.ByLoad, err = featureFlag(partitioning.GetPartitioningByLoad(), settings.ByLoad); err != nil {
		return ydbpartition.Settings{}, fmt.Errorf("partitioning by load: %w", err)
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
		settings.ReadReplicas = ydbpartition.Replicas{PerAZ: true, Count: replicas.GetPerAzReadReplicasCount()}
	case replicas.GetAnyAzReadReplicasCount() != 0:
		settings.ReadReplicas = ydbpartition.Replicas{Count: replicas.GetAnyAzReadReplicasCount()}
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

// rowDeletionPolicy reads a table's TTL as its row deletion policy: the column,
// the interval as the seconds YDB keeps written the way YDB shows them, and an
// integer column's unit. A table with no TTL has no policy.
//
// A TTL the pinned protocol buffers do not model is refused by name. Measured
// on 25.1.4.7 and 26.2.1.14, a row table describes its TTL in one of the two
// modes read here even when it was set in the tiered mode, field 4, as one
// DELETE tier; a tier that moves rows elsewhere is refused on a row table.
func rowDeletionPolicy(settings *Ydb_Table.TtlSettings) (*ast.RowDeletionPolicySpec, error) {
	if settings == nil {
		return nil, nil
	}
	if unknown := unknownFields(settings); len(unknown) > 0 {
		return nil, fmt.Errorf("its TTL carries field %s (field 4 is a tiered TTL), which this build of Ptah does not read",
			joinNumbers(unknown))
	}
	switch mode := settings.GetMode().(type) {
	case *Ydb_Table.TtlSettings_DateTypeColumn:
		date := mode.DateTypeColumn
		if unknown := unknownFields(date); len(unknown) > 0 {
			return nil, fmt.Errorf("its TTL on a date column carries field %s, which this build of Ptah does not read",
				joinNumbers(unknown))
		}
		return &ast.RowDeletionPolicySpec{
			Column:   date.GetColumnName(),
			Interval: ydbttl.FormatInterval(uint64(date.GetExpireAfterSeconds())),
		}, nil
	case *Ydb_Table.TtlSettings_ValueSinceUnixEpoch:
		epoch := mode.ValueSinceUnixEpoch
		if unknown := unknownFields(epoch); len(unknown) > 0 {
			return nil, fmt.Errorf("its TTL on an integer column carries field %s, which this build of Ptah does not read",
				joinNumbers(unknown))
		}
		unit, known := epochUnits[epoch.GetColumnUnit()]
		if !known {
			return nil, fmt.Errorf("its TTL reads column %q in unit %s, which this build of Ptah does not read",
				epoch.GetColumnName(), epoch.GetColumnUnit())
		}
		return &ast.RowDeletionPolicySpec{
			Column:   epoch.GetColumnName(),
			Interval: ydbttl.FormatInterval(uint64(epoch.GetExpireAfterSeconds())),
			Unit:     unit,
		}, nil
	default:
		return nil, fmt.Errorf("its TTL has a mode this build of Ptah does not read (%T)", settings.GetMode())
	}
}

// epochUnits names the units an integer TTL column counts in.
var epochUnits = map[Ydb_Table.ValueSinceUnixEpochModeSettings_Unit]string{
	Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_SECONDS:      ydbttl.Seconds,
	Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MILLISECONDS: ydbttl.Milliseconds,
	Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_MICROSECONDS: ydbttl.Microseconds,
	Ydb_Table.ValueSinceUnixEpochModeSettings_UNIT_NANOSECONDS:  ydbttl.Nanoseconds,
}

// unmodeledSettings records the table settings Ptah does not model yet. A
// changefeed and the column families are read rather than recorded here; see
// [Reader.changefeeds] and [Reader.columnFamilies]. A setting is recorded
// where it differs from what a table created without one carries, measured on
// local-ydb 26.2.1.14: no TTL run interval and no tiering policy, and external
// blobs off with no storage pools named. A row table's partitioning, read
// replicas and key bloom filter are read, by [tableSettings].
//
// The TTL itself is the table's row deletion policy. What is recorded under
// [coverage.TTL] is what YQL cannot write about it: the run interval, which
// only the SDK and the CLI set and which `SET (TTL = ...)` resets (a table set
// to 1800 seconds with `ydb table ttl set --run-interval` reads back with none
// after it, on 25.1.4.7 and 26.2.1.14), and a column table's tiering policy.
func unmodeledSettings(schema, name string, described *Ydb_Table.DescribeTableResult) []coverage.Object {
	var records []coverage.Object
	if described.GetTtlSettings().GetRunIntervalSeconds() != 0 || described.GetTiering() != "" {
		records = append(records, unmodeled(coverage.TTL, schema, name))
	}
	if hasStorageSettings(described) {
		records = append(records, unmodeled(coverage.TableOption, schema, name))
	}
	return records
}

// hasStorageSettings reports storage settings other than a new table's: its
// tablet's commit log pools, an external pool, external blobs, or partitioning
// by columns, which only a column table has.
func hasStorageSettings(described *Ydb_Table.DescribeTableResult) bool {
	if described.GetStoreType() != Ydb_Table.StoreType_STORE_TYPE_COLUMN && len(described.GetPartitioningSettings().GetPartitionBy()) > 0 {
		return true
	}
	storage := described.GetStorageSettings()
	return storage.GetTabletCommitLog0() != nil || storage.GetTabletCommitLog1() != nil ||
		storage.GetExternal() != nil || storage.GetStoreExternalBlobs() == Ydb.FeatureFlag_ENABLED
}
