package ydb

import (
	"fmt"
	"slices"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbfamily"
)

// columnFamilyCacheModeField is the field number of `cache_mode` in
// ydb_table.proto's ColumnFamily, which the pinned protocol buffers do not
// model. YDB fills it from 25.3 on, where a family declares CACHE_MODE.
const columnFamilyCacheModeField protowire.Number = 6

// cacheModes names the values of ColumnFamily.CacheMode by number:
// CACHE_MODE_REGULAR and CACHE_MODE_IN_MEMORY. Zero, unspecified, is a family
// that declared none.
var cacheModes = map[uint64]string{
	0: "",
	1: ydbfamily.CacheModeRegular,
	2: ydbfamily.CacheModeInMemory,
}

// columnFamilies reads a row table's column families, each with the columns
// DescribeTable gives its name, in the form [ydbfamily.Normalize] gives, and
// reports false where a family holds something Ptah does not read. The caller
// then records the table's families as unrepresentable and attaches none, so
// a plan neither changes nor drops them, and a table rebuild refuses the
// table.
//
// Each setting is read as the table holds it, YDB's own values included
// (`off`, `regular`), so a comparison sees what a table profile gave the
// table. keep_in_memory is read into KeepInMemory: no YQL statement sets it,
// and a table profile's `column_cache` turns it on for every new table.
//
// What is not read: a compression YQL cannot set on a row table (zstd), and a
// compression level; a storage pool or a family carrying a field the pinned
// protocol buffers do not model, but the cache mode, which this reads from
// field 6; and a column naming a family the table does not list.
func (r *Reader) columnFamilies(described *Ydb_Table.DescribeTableResult) ([]ydbschema.ColumnFamily, bool) {
	held := described.GetColumnFamilies()
	families := make([]ydbschema.ColumnFamily, 0, len(held))
	for _, family := range held {
		spec, read := columnFamily(family)
		if !read {
			return nil, false
		}
		families = append(families, spec)
	}
	for _, column := range described.GetColumns() {
		name := column.GetFamily()
		if name == "" || name == ydbfamily.Default {
			continue
		}
		index := slices.IndexFunc(families, func(family ydbschema.ColumnFamily) bool { return family.Name == name })
		if index < 0 {
			return nil, false
		}
		families[index].Columns = append(families[index].Columns, column.GetName())
	}
	return ydbfamily.Normalize(families), true
}

// columnFamily reads one family's settings, and reports false where it holds
// something Ptah does not read; see [Reader.columnFamilies].
func columnFamily(family *Ydb_Table.ColumnFamily) (ydbschema.ColumnFamily, bool) {
	spec := ydbschema.ColumnFamily{
		Name:         family.GetName(),
		KeepInMemory: family.GetKeepInMemory() == Ydb.FeatureFlag_ENABLED,
	}
	cacheMode, read := familyCacheMode(family)
	if !read {
		return ydbschema.ColumnFamily{}, false
	}
	spec.CacheMode = cacheMode
	if pool := family.GetData(); pool != nil {
		if len(unknownFields(pool)) > 0 {
			return ydbschema.ColumnFamily{}, false
		}
		spec.Data = pool.GetMedia()
	}
	switch family.GetCompression() {
	case Ydb_Table.ColumnFamily_COMPRESSION_UNSPECIFIED:
	case Ydb_Table.ColumnFamily_COMPRESSION_NONE:
		spec.Compression = ydbfamily.CompressionOff
	case Ydb_Table.ColumnFamily_COMPRESSION_LZ4:
		spec.Compression = ydbfamily.CompressionLZ4
	default:
		return ydbschema.ColumnFamily{}, false
	}
	return spec, true
}

// familyCacheMode reads the cache mode out of the fields of a family the
// pinned protocol buffers do not model, and reports false where another such
// field arrives, or the cache mode holds a value it does not name.
func familyCacheMode(family *Ydb_Table.ColumnFamily) (string, bool) {
	var mode string
	unknown := family.ProtoReflect().GetUnknown()
	for len(unknown) > 0 {
		number, wireType, length := protowire.ConsumeTag(unknown)
		if length < 0 || number != columnFamilyCacheModeField || wireType != protowire.VarintType {
			return "", false
		}
		unknown = unknown[length:]
		value, valueLength := protowire.ConsumeVarint(unknown)
		if valueLength < 0 {
			return "", false
		}
		unknown = unknown[valueLength:]
		named, known := cacheModes[value]
		if !known {
			return "", false
		}
		mode = named
	}
	return mode, true
}

// familyFacets adds the observed families to facets, bound to YDB.
func familyFacets(facets schemaext.Facets, families []ydbschema.ColumnFamily) (schemaext.Facets, error) {
	observed := &ydbschema.ObservedColumnFamilies{Families: families}
	if err := ydbschema.ValidateObservedColumnFamilies(observed); err != nil {
		return schemaext.Facets{}, err
	}
	facets, err := facets.With(observed)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.WithTargetScope(ydbschema.ColumnFamiliesKind, platform.YDB)
}

// unreadFamiliesReason is what a table whose families hold a setting the
// reader does not read is recorded with.
const unreadFamiliesReason = "the table's column families hold a setting Ptah does not read"

// familyCoverage records what the read knows about the families of the table
// name in the directory schema: complete knowledge where it read them, and
// unrepresentable where they hold a setting it does not read. A table the read
// did not return is not known to have no families.
func familyCoverage(db *catalog.Database, schema, name string, knowledge schemaext.Knowledge) error {
	return tableCoverage(db, ydbschema.ColumnFamiliesKind, ydbschema.ColumnFamiliesCoverage,
		"only returned tables have inspected column families", schema, name, knowledge)
}

// tableCoverage records knowledge of a table facet of kind for the table name
// in the directory schema, enrolling the kind first, through enroll, with
// uninspected knowledge for every table the read did not return.
func tableCoverage(db *catalog.Database, kind schemaext.Kind,
	enroll func(schemaext.Representation, schemaext.Knowledge, []schemaext.SubjectCoverage) (schemaext.Coverage, error),
	uninspected, schema, name string, knowledge schemaext.Knowledge,
) error {
	subject := objectidentity.NewBuilder(identifier.ForDialect(platform.YDB)).TableParts(schema, name)
	return subjectCoverage(db, kind, enroll, uninspected, subject, knowledge)
}

// subjectCoverage records knowledge of a facet of kind for subject, enrolling
// the kind first, through enroll, with uninspected knowledge for every subject
// the read did not record.
func subjectCoverage(db *catalog.Database, kind schemaext.Kind,
	enroll func(schemaext.Representation, schemaext.Knowledge, []schemaext.SubjectCoverage) (schemaext.Coverage, error),
	uninspected string, subject objectidentity.ID, knowledge schemaext.Knowledge,
) error {
	kinds := db.FeatureCoverage.KindRecords()
	if !slices.ContainsFunc(kinds, func(record schemaext.KindCoverage) bool { return record.Model.Kind == kind }) {
		enrolled, err := enroll(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: uninspected}, nil)
		if err != nil {
			return err
		}
		kinds = append(kinds, enrolled.KindRecords()...)
	}
	known, err := schemaext.NewCoverage(schemaext.Observed, kinds, append(db.FeatureCoverage.SubjectRecords(),
		schemaext.SubjectCoverage{Kind: kind, Subject: subject, Knowledge: knowledge}))
	if err != nil {
		return fmt.Errorf("failed to record %s coverage: %w", kind, err)
	}
	db.FeatureCoverage = known
	return nil
}
