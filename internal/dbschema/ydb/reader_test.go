package ydb_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// fakeSource answers from fixed listings and descriptions, keyed by absolute
// path. A path it does not know is an error, so a reader that asks about a
// directory it should skip fails the test.
type fakeSource struct {
	directories map[string][]*Ydb_Scheme.Entry
	tables      map[string]*Ydb_Table.DescribeTableResult
}

func (f fakeSource) ListDirectory(_ context.Context, path string) ([]*Ydb_Scheme.Entry, error) {
	entries, ok := f.directories[path]
	if !ok {
		return nil, fmt.Errorf("listed %s, which the fixture does not hold", path)
	}
	return entries, nil
}

func (f fakeSource) DescribeTable(_ context.Context, path string) (*Ydb_Table.DescribeTableResult, error) {
	described, ok := f.tables[path]
	if !ok {
		return nil, fmt.Errorf("described %s, which the fixture does not hold", path)
	}
	return described, nil
}

func entry(name string, entryType Ydb_Scheme.Entry_Type) *Ydb_Scheme.Entry {
	return &Ydb_Scheme.Entry{Name: name, Type: entryType}
}

func primitive(id Ydb.Type_PrimitiveTypeId) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: id}}
}

func optional(t *Ydb.Type) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: t}}}
}

func decimal(precision, scale uint32) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_DecimalType{DecimalType: &Ydb.DecimalType{Precision: precision, Scale: scale}}}
}

func literal(t *Ydb.Type, value *Ydb.Value) *Ydb_Table.ColumnMeta_FromLiteral {
	return &Ydb_Table.ColumnMeta_FromLiteral{FromLiteral: &Ydb.TypedValue{Type: t, Value: value}}
}

// plainTable is a table with one Int64 key, described as a table created
// without settings is.
func plainTable(columns ...*Ydb_Table.ColumnMeta) *Ydb_Table.DescribeTableResult {
	return &Ydb_Table.DescribeTableResult{
		Columns:    append([]*Ydb_Table.ColumnMeta{{Name: "id", Type: primitive(Ydb.Type_INT64)}}, columns...),
		PrimaryKey: []string{"id"},
		ColumnFamilies: []*Ydb_Table.ColumnFamily{
			{Name: "default", Compression: Ydb_Table.ColumnFamily_COMPRESSION_NONE},
		},
		PartitioningSettings: &Ydb_Table.PartitioningSettings{
			PartitioningBySize: Ydb.FeatureFlag_ENABLED,
			PartitionSizeMb:    2048,
			PartitioningByLoad: Ydb.FeatureFlag_DISABLED,
			MinPartitionsCount: 1,
		},
		StorageSettings: &Ydb_Table.StorageSettings{StoreExternalBlobs: Ydb.FeatureFlag_DISABLED},
	}
}

func readFrom(c *qt.C, source fakeSource) *catalog.Database {
	c.Helper()
	db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).ReadSchemaContext(context.Background())
	c.Assert(err, qt.IsNil)
	return db
}

// The reader walks every directory, skips the server's dot-directories without
// listing them, and names a table in a directory by that directory.
func TestReader_WalksTheTree(t *testing.T) {
	c := qt.New(t)
	source := fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{
			"/local": {
				entry("users", Ydb_Scheme.Entry_TABLE),
				entry(".sys", Ydb_Scheme.Entry_DIRECTORY),
				entry(".metadata", Ydb_Scheme.Entry_DIRECTORY),
				entry("app", Ydb_Scheme.Entry_DIRECTORY),
				entry("nested", Ydb_Scheme.Entry_DATABASE),
			},
			"/local/app":     {entry("sub", Ydb_Scheme.Entry_DIRECTORY), entry("orders", Ydb_Scheme.Entry_TABLE)},
			"/local/app/sub": {entry("items", Ydb_Scheme.Entry_TABLE)},
		},
		tables: map[string]*Ydb_Table.DescribeTableResult{
			"/local/users":         plainTable(),
			"/local/app/orders":    plainTable(),
			"/local/app/sub/items": plainTable(),
		},
	}

	db := readFrom(c, source)

	var got []string
	for _, table := range db.Tables {
		got = append(got, table.Schema+"|"+table.Name)
	}
	c.Assert(got, qt.DeepEquals, []string{"app|orders", "app/sub|items", "|users"})
}

// A schema list narrows the read to the directories it names, and a
// directory's own subdirectories are schemas of their own.
func TestReader_SetSchemas(t *testing.T) {
	tests := []struct {
		name    string
		schemas []string
		want    []string
	}{
		{name: "the root alone", schemas: []string{""}, want: []string{"|users"}},
		{name: "a directory without its subdirectory", schemas: []string{"app"}, want: []string{"app|orders"}},
		{name: "slashes around a name", schemas: []string{"/app/sub/"}, want: []string{"app/sub|items"}},
		{name: "an empty list reads everything", schemas: []string{}, want: []string{"app|orders", "app/sub|items", "|users"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{
					"/local":         {entry("users", Ydb_Scheme.Entry_TABLE), entry("app", Ydb_Scheme.Entry_DIRECTORY)},
					"/local/app":     {entry("orders", Ydb_Scheme.Entry_TABLE), entry("sub", Ydb_Scheme.Entry_DIRECTORY)},
					"/local/app/sub": {entry("items", Ydb_Scheme.Entry_TABLE)},
				},
				tables: map[string]*Ydb_Table.DescribeTableResult{
					"/local/users":         plainTable(),
					"/local/app/orders":    plainTable(),
					"/local/app/sub/items": plainTable(),
				},
			}
			reader := ydbschema.NewReaderFromSource(source, "local", capability.YDB262())
			reader.SetSchemas(test.schemas)

			db, err := reader.ReadSchemaContext(context.Background())

			c.Assert(err, qt.IsNil)
			var got []string
			for _, table := range db.Tables {
				got = append(got, table.Schema+"|"+table.Name)
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// Each column carries its YDB type, its nullability from whether the type is
// Optional, and its default as the literal ydbtype writes. A Serial column is
// its integer type, incrementing, with no default of its own.
func TestReader_Columns(t *testing.T) {
	c := qt.New(t)
	described := plainTable(
		&Ydb_Table.ColumnMeta{Name: "name", Type: optional(primitive(Ydb.Type_UTF8)),
			DefaultValue: literal(primitive(Ydb.Type_UTF8), &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: "it's"}})},
		&Ydb_Table.ColumnMeta{Name: "n", Type: primitive(Ydb.Type_INT8),
			DefaultValue: literal(primitive(Ydb.Type_INT8), &Ydb.Value{Value: &Ydb.Value_Int32Value{Int32Value: -5}})},
		&Ydb_Table.ColumnMeta{Name: "price", Type: optional(decimal(10, 2)),
			DefaultValue: literal(decimal(10, 2), &Ydb.Value{Value: &Ydb.Value_Low_128{Low_128: 1250}})},
		&Ydb_Table.ColumnMeta{Name: "seq", Type: primitive(Ydb.Type_INT32),
			DefaultValue: &Ydb_Table.ColumnMeta_FromSequence{FromSequence: &Ydb_Table.SequenceDescription{}}},
		&Ydb_Table.ColumnMeta{Name: "column_store_flag", Type: primitive(Ydb.Type_BOOL), NotNull: new(true)},
	)
	source := fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
		tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": described},
	}

	db := readFrom(c, source)

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Columns, qt.DeepEquals, []catalog.Column{
		{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true},
		{Name: "name", DataType: "Utf8", ColumnType: "Utf8", IsNullable: "YES", OrdinalPosition: 2,
			ColumnDefault: new(`'it\'s'u`)},
		{Name: "n", DataType: "Int8", ColumnType: "Int8", IsNullable: "NO", OrdinalPosition: 3,
			ColumnDefault: new("-5t")},
		{Name: "price", DataType: "Decimal(10,2)", ColumnType: "Decimal(10,2)", IsNullable: "YES", OrdinalPosition: 4,
			ColumnDefault: new("Decimal('12.5', 10, 2)"), NumericPrecision: new(10), NumericScale: new(2)},
		{Name: "seq", DataType: "Int32", ColumnType: "Int32", IsNullable: "NO", OrdinalPosition: 5,
			IsAutoIncrement: true},
		{Name: "column_store_flag", DataType: "Bool", ColumnType: "Bool", IsNullable: "NO", OrdinalPosition: 6},
	})
}

// The key is a PRIMARY KEY constraint as well as a flag on its columns, so a
// plan that drops a key column sees the key.
func TestReader_PrimaryKey(t *testing.T) {
	c := qt.New(t)
	described := plainTable(&Ydb_Table.ColumnMeta{Name: "tenant", Type: primitive(Ydb.Type_UTF8)})
	described.PrimaryKey = []string{"tenant", "id"}
	source := fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("app", Ydb_Scheme.Entry_DIRECTORY)},
			"/local/app": {entry("t", Ydb_Scheme.Entry_TABLE)}},
		tables: map[string]*Ydb_Table.DescribeTableResult{"/local/app/t": described},
	}

	db := readFrom(c, source)

	c.Assert(db.Constraints, qt.DeepEquals, []catalog.Constraint{{
		Name: "t_pkey", TableName: "t", Schema: "app", Type: "PRIMARY KEY",
		ColumnName: "tenant", ColumnNames: []string{"tenant", "id"},
	}})
}

// A global index carries its kind in Method and its uniqueness in IsUnique,
// the way internal/ydbindex reads a catalog index, and its covered columns in
// order.
func TestReader_Indexes(t *testing.T) {
	c := qt.New(t)
	described := plainTable(
		&Ydb_Table.ColumnMeta{Name: "a", Type: optional(primitive(Ydb.Type_UTF8))},
		&Ydb_Table.ColumnMeta{Name: "b", Type: optional(primitive(Ydb.Type_UTF8))},
	)
	described.Indexes = []*Ydb_Table.TableIndexDescription{
		{Name: "by_a", IndexColumns: []string{"a"}, DataColumns: []string{"b", "id"},
			Type: &Ydb_Table.TableIndexDescription_GlobalIndex{GlobalIndex: &Ydb_Table.GlobalIndex{}}},
		{Name: "by_b", IndexColumns: []string{"b", "a"},
			Type: &Ydb_Table.TableIndexDescription_GlobalAsyncIndex{GlobalAsyncIndex: &Ydb_Table.GlobalAsyncIndex{}}},
		{Name: "uniq", IndexColumns: []string{"b"},
			Type: &Ydb_Table.TableIndexDescription_GlobalUniqueIndex{GlobalUniqueIndex: &Ydb_Table.GlobalUniqueIndex{}}},
	}
	source := fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
		tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": described},
	}

	db := readFrom(c, source)

	c.Assert(db.Indexes, qt.DeepEquals, []catalog.Index{
		{Name: "by_a", TableName: "t", Columns: []string{"a"}, IncludeColumns: []string{"b", "id"},
			Method: "GLOBAL SYNC", Definition: "INDEX `by_a` GLOBAL SYNC ON (`a`) COVER (`b`, `id`)"},
		{Name: "by_b", TableName: "t", Columns: []string{"b", "a"},
			Method: "GLOBAL ASYNC", Definition: "INDEX `by_b` GLOBAL ASYNC ON (`b`, `a`)"},
		{Name: "uniq", TableName: "t", Columns: []string{"b"}, IsUnique: true,
			Method: "GLOBAL SYNC", Definition: "INDEX `uniq` GLOBAL UNIQUE SYNC ON (`b`)"},
	})
}

// Every object Ptah does not model is recorded by its path, so a description's
// silence about it is not read as its absence. The access model is recorded
// as a whole kind, because the reader does not read it.
func TestReader_RecordsWhatItDoesNotDescribe(t *testing.T) {
	c := qt.New(t)
	settings := plainTable(&Ydb_Table.ColumnMeta{Name: "ts", Type: optional(primitive(Ydb.Type_TIMESTAMP))})
	settings.TtlSettings = &Ydb_Table.TtlSettings{}
	settings.Changefeeds = []*Ydb_Table.ChangefeedDescription{{Name: "feed"}}
	settings.ColumnFamilies = append(settings.ColumnFamilies, &Ydb_Table.ColumnFamily{Name: "cold"})
	settings.KeyBloomFilter = Ydb.FeatureFlag_ENABLED
	source := fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{
			"/local": {
				entry("v", Ydb_Scheme.Entry_VIEW),
				entry("events", Ydb_Scheme.Entry_TOPIC),
				entry("legacy_queue", Ydb_Scheme.Entry_PERS_QUEUE_GROUP),
				entry("olap", Ydb_Scheme.Entry_COLUMN_TABLE),
				entry("store", Ydb_Scheme.Entry_COLUMN_STORE),
				entry("locks", Ydb_Scheme.Entry_COORDINATION_NODE),
				entry("seq", Ydb_Scheme.Entry_SEQUENCE),
				entry("repl", Ydb_Scheme.Entry_REPLICATION),
				entry("xfer", Ydb_Scheme.Entry_TRANSFER),
				entry("src", Ydb_Scheme.Entry_EXTERNAL_DATA_SOURCE),
				entry("ext", Ydb_Scheme.Entry_EXTERNAL_TABLE),
				entry("key", Ydb_Scheme.Entry_SECRET),
				entry("pool", Ydb_Scheme.Entry_RESOURCE_POOL),
				entry("health", Ydb_Scheme.Entry_SYS_VIEW),
				entry("app", Ydb_Scheme.Entry_DIRECTORY),
			},
			"/local/app": {entry("t", Ydb_Scheme.Entry_TABLE), entry("plain", Ydb_Scheme.Entry_TABLE)},
		},
		tables: map[string]*Ydb_Table.DescribeTableResult{
			"/local/app/t":     settings,
			"/local/app/plain": plainTable(),
		},
	}

	db := readFrom(c, source)

	observed := func(kind coverage.Kind, name string) coverage.Object {
		return coverage.Object{Kind: kind, Name: name, Reason: coverage.Unsupported, Provenance: coverage.Observed}
	}
	derived := func(kind coverage.Kind) coverage.Object {
		return coverage.Object{Kind: kind, Reason: coverage.Unsupported, Provenance: coverage.DerivedFromTarget}
	}
	c.Assert(db.NotDescribed, qt.DeepEquals, coverage.Set{}.With(
		observed(coverage.TTL, "app.t"),
		observed(coverage.Changefeed, "app.t/feed"),
		observed(coverage.ColumnFamily, "app.t"),
		observed(coverage.TableOption, "app.t"),
		observed(coverage.Topic, "events"),
		observed(coverage.ExternalTable, "ext"),
		observed(coverage.Secret, "key"),
		observed(coverage.Topic, "legacy_queue"),
		observed(coverage.CoordinationNode, "locks"),
		observed(coverage.ColumnTable, "olap"),
		observed(coverage.ResourcePool, "pool"),
		observed(coverage.Replication, "repl"),
		observed(coverage.Sequence, "seq"),
		observed(coverage.ExternalDataSource, "src"),
		observed(coverage.ColumnTable, "store"),
		observed(coverage.View, "v"),
		observed(coverage.Transfer, "xfer"),
		derived(coverage.Role),
		derived(coverage.Grant),
	))
	c.Assert(db.Tables, qt.HasLen, 2)
}

// A table setting is recorded only where it differs from what a table created
// without settings carries, measured on local-ydb 26.2.1.14.
func TestReader_RecordsEachTableOption(t *testing.T) {
	tests := []struct {
		name         string
		partitioning *Ydb_Table.PartitioningSettings
		replicas     *Ydb_Table.ReadReplicasSettings
		storage      *Ydb_Table.StorageSettings
	}{
		{
			name: "partitioning by load",
			partitioning: &Ydb_Table.PartitioningSettings{PartitioningBySize: Ydb.FeatureFlag_ENABLED,
				PartitionSizeMb: 2048, PartitioningByLoad: Ydb.FeatureFlag_ENABLED, MinPartitionsCount: 1},
			storage: defaultStorage(),
		},
		{
			name: "partitioning by size off",
			partitioning: &Ydb_Table.PartitioningSettings{PartitioningBySize: Ydb.FeatureFlag_DISABLED,
				PartitionSizeMb: 2048, PartitioningByLoad: Ydb.FeatureFlag_DISABLED, MinPartitionsCount: 1},
			storage: defaultStorage(),
		},
		{
			name: "a partition size",
			partitioning: &Ydb_Table.PartitioningSettings{PartitioningBySize: Ydb.FeatureFlag_ENABLED,
				PartitionSizeMb: 512, PartitioningByLoad: Ydb.FeatureFlag_DISABLED, MinPartitionsCount: 1},
			storage: defaultStorage(),
		},
		{
			name: "a minimum partition count",
			partitioning: &Ydb_Table.PartitioningSettings{PartitioningBySize: Ydb.FeatureFlag_ENABLED,
				PartitionSizeMb: 2048, PartitioningByLoad: Ydb.FeatureFlag_DISABLED, MinPartitionsCount: 3},
			storage: defaultStorage(),
		},
		{
			name: "a maximum partition count",
			partitioning: &Ydb_Table.PartitioningSettings{PartitioningBySize: Ydb.FeatureFlag_ENABLED,
				PartitionSizeMb: 2048, PartitioningByLoad: Ydb.FeatureFlag_DISABLED, MinPartitionsCount: 1,
				MaxPartitionsCount: 50},
			storage: defaultStorage(),
		},
		{
			name:         "read replicas",
			partitioning: defaultPartitioning(),
			replicas: &Ydb_Table.ReadReplicasSettings{
				Settings: &Ydb_Table.ReadReplicasSettings_PerAzReadReplicasCount{PerAzReadReplicasCount: 1},
			},
			storage: defaultStorage(),
		},
		{
			name:         "external blobs",
			partitioning: defaultPartitioning(),
			storage:      &Ydb_Table.StorageSettings{StoreExternalBlobs: Ydb.FeatureFlag_ENABLED},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			described := plainTable()
			described.PartitioningSettings = test.partitioning
			described.ReadReplicasSettings = test.replicas
			described.StorageSettings = test.storage
			source := fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
				tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": described},
			}

			db := readFrom(c, source)

			c.Assert(db.NotDescribed.Describes(coverage.TableOption, "t"), qt.IsFalse)
			c.Assert(db.NotDescribed.Describes(coverage.TTL, "t"), qt.IsTrue)
		})
	}
}

func defaultPartitioning() *Ydb_Table.PartitioningSettings {
	return plainTable().PartitioningSettings
}

func defaultStorage() *Ydb_Table.StorageSettings {
	return plainTable().StorageSettings
}

// The control for the rows above: a table created without settings records
// none of them.
func TestReader_PlainTableRecordsNoSetting(t *testing.T) {
	c := qt.New(t)
	source := fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
		tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": plainTable()},
	}

	db := readFrom(c, source)

	for _, kind := range []coverage.Kind{coverage.TTL, coverage.Changefeed, coverage.ColumnFamily, coverage.TableOption} {
		c.Assert(db.NotDescribed.Describes(kind, "t"), qt.IsTrue, qt.Commentf("kind %s", kind))
	}
}

// vectorIndex is an index described with the type the pinned protocol buffers
// do not model: the oneof is empty, and the kind sits in field 9 of the
// message as bytes the decoder keeps unread.
func vectorIndex() *Ydb_Table.TableIndexDescription {
	index := &Ydb_Table.TableIndexDescription{Name: "embedding_idx", IndexColumns: []string{"emb"}}
	unknown := protowire.AppendTag(nil, 9, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, nil)
	index.ProtoReflect().SetUnknown(unknown)
	return index
}

func TestReader_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		source  fakeSource
		wantErr string
	}{
		{
			// The SDK reads such an index back as a plain global one; the
			// reader refuses it by name instead.
			name: "an index kind the protocol buffers do not model",
			source: fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
				tables: map[string]*Ydb_Table.DescribeTableResult{"/local/t": func() *Ydb_Table.DescribeTableResult {
					described := plainTable(&Ydb_Table.ColumnMeta{Name: "emb", Type: optional(primitive(Ydb.Type_STRING))})
					described.Indexes = []*Ydb_Table.TableIndexDescription{vectorIndex()}
					return described
				}()},
			},
			wantErr: `YDB table /local/t: index "embedding_idx" is a vector_kmeans_tree index: reading or creating ` +
				`a YDB vector, full-text, JSON or column-table index is not implemented yet \(stokaro/ptah#4015, phase 10\)`,
		},
		{
			name: "a scheme object of a type Ptah does not read",
			source: fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("vol", Ydb_Scheme.Entry_BLOCK_STORE_VOLUME)}},
			},
			wantErr: `YDB object /local/vol is a BLOCK_STORE_VOLUME, which Ptah does not read; remove it from the ` +
				`database or read a schema that does not hold it`,
		},
		{
			name: "a scheme object of a type the protocol buffers do not know",
			source: fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("x", Ydb_Scheme.Entry_Type(99))}},
			},
			wantErr: `YDB object /local/x is a scheme entry of type 99, which Ptah does not read; .*`,
		},
		{
			name: "a column of a type a table column cannot have",
			source: fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
				tables: map[string]*Ydb_Table.DescribeTableResult{"/local/t": plainTable(&Ydb_Table.ColumnMeta{
					Name: "l", Type: &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: primitive(Ydb.Type_INT32)}}},
				})},
			},
			wantErr: `YDB table /local/t: column "l": its type \*Ydb.Type_ListType is not a type Ptah reads in a table column`,
		},
		{
			name: "a primitive type the reader does not know",
			source: fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
				tables: map[string]*Ydb_Table.DescribeTableResult{"/local/t": plainTable(&Ydb_Table.ColumnMeta{
					Name: "tz", Type: primitive(Ydb.Type_TZ_TIMESTAMP),
				})},
			},
			wantErr: `YDB table /local/t: column "tz": its type TZ_TIMESTAMP is not a type Ptah reads in a table column`,
		},
		{
			name: "a listing the server refuses",
			source: fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("gone", Ydb_Scheme.Entry_DIRECTORY)}},
			},
			wantErr: `listed /local/gone, which the fixture does not hold`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := ydbschema.NewReaderFromSource(test.source, "/local", capability.YDB262()).
				ReadSchemaContext(context.Background())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// errorSource fails every call, which is how a read with no server ends.
type errorSource struct{}

func (errorSource) ListDirectory(context.Context, string) ([]*Ydb_Scheme.Entry, error) {
	return nil, errors.New("connection refused")
}

func (errorSource) DescribeTable(context.Context, string) (*Ydb_Table.DescribeTableResult, error) {
	return nil, errors.New("connection refused")
}

func TestReader_FailurePath_SourceFails(t *testing.T) {
	c := qt.New(t)

	db, err := ydbschema.NewReaderFromSource(errorSource{}, "/local", capability.YDB262()).ReadSchema()

	c.Assert(err, qt.ErrorMatches, "connection refused")
	c.Assert(db, qt.IsNil)
}

// An object outside the directories a read is scoped to is not read, so one
// the reader would refuse does not refuse a read that never meets it.
func TestReader_ScopedReadPassesAnObjectOutsideIt(t *testing.T) {
	c := qt.New(t)
	source := fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{
			"/local":     {entry("vol", Ydb_Scheme.Entry_BLOCK_STORE_VOLUME), entry("app", Ydb_Scheme.Entry_DIRECTORY)},
			"/local/app": {entry("orders", Ydb_Scheme.Entry_TABLE)},
		},
		tables: map[string]*Ydb_Table.DescribeTableResult{"/local/app/orders": plainTable()},
	}
	reader := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262())
	reader.SetSchemas([]string{"app"})

	db, err := reader.ReadSchemaContext(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
}
