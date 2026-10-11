package atlashclrender

import (
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// WritesTableFacet returns whether a render with owners handles a table facet
// kind itself rather than as platform properties:
//   - a facet an owner of the set writes, such as a hypertable, a table's
//     row-level security switches or a MySQL table's engine;
//   - YDB column families, whose loss it reports by family and which it leaves
//     out where a table has only the default family stating nothing;
//   - a YDB table's settings, whose loss it reports by setting, and YDB column
//     storage, whose loss it reports by table.
//
// A caller that encodes the other facets as platform properties sets these
// aside first.
func WritesTableFacet(owners hclext.Set) func(kind schemaext.Kind) bool {
	return func(kind schemaext.Kind) bool {
		return owners.Encodes(kind) || kind == ydbschema.ColumnFamiliesKind || kind == ydbschema.TablePartitioningKind ||
			kind == ydbschema.ColumnStoreKind
	}
}

// WritesColumnFacet returns whether a render with owners handles a column
// facet kind itself rather than as platform properties: a facet an owner of
// the set writes, such as a MySQL column's charset.
func WritesColumnFacet(owners hclext.Set) func(kind schemaext.Kind) bool {
	return owners.Encodes
}

// WritesIndexFacet returns whether a render with owners handles an index
// facet kind itself rather than as platform properties: a facet an owner of
// the set writes, such as a MySQL FULLTEXT index's parser, and a YDB vector
// index's settings and a YDB index's partitioning and read replicas, which
// it writes as attributes of the index block.
func WritesIndexFacet(owners hclext.Set) func(kind schemaext.Kind) bool {
	return func(kind schemaext.Kind) bool {
		return owners.Encodes(kind) || kind == ydbschema.VectorIndexKind || kind == ydbschema.IndexPartitioningKind
	}
}

// WritesKeyFacet returns whether a render with owners accounts for a facet
// kind of a primary key or a constraint itself: a facet an owner of the set
// writes, such as a MySQL key's block-size hint, whose loss the owner reports
// at the key.
func WritesKeyFacet(owners hclext.Set) func(kind schemaext.Kind) bool {
	return owners.Encodes
}
