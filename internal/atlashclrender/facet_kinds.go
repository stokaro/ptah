package atlashclrender

import (
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/feature/pgpolicy"
)

// WritesTableFacet returns whether a render with owners handles a table facet
// kind itself rather than as platform properties: a facet an owner of the set
// writes, such as a hypertable, YDB column families, whose loss it reports by
// family and which it leaves out where a table has only the default family
// stating nothing, a YDB table's settings, whose loss it reports by setting,
// YDB column storage, whose loss it reports by table, MySQL table options,
// which it writes as the table's engine, charset and auto_increment, SQLite
// table options, which it writes as the table's strict and without_rowid
// attributes, the spelling the community binary reads, and a SQLite virtual
// table's module declaration, which no HCL block carries and whose loss the
// inspect command reports with the module. A caller that encodes the other
// facets as platform properties sets these aside first.
func WritesTableFacet(owners hclext.Set) func(kind schemaext.Kind) bool {
	return func(kind schemaext.Kind) bool {
		return owners.Encodes(kind) || kind == ydbschema.ColumnFamiliesKind || kind == mysqlschema.TableKind || kind == ydbschema.TablePartitioningKind ||
			kind == ydbschema.ColumnStoreKind || kind == pgpolicy.TableStateKind || kind == sqlitetable.TableKind || kind == sqlitetable.VirtualKind
	}
}

// WritesColumnFacet reports a column facet kind the renderer writes itself
// rather than as platform properties: the MySQL owner's column settings,
// which it writes as the column's charset and on_update attributes.
func WritesColumnFacet(kind schemaext.Kind) bool { return kind == mysqlschema.ColumnSettingsKind }

// WritesIndexFacet reports an index facet kind the renderer writes itself
// rather than as platform properties: a YDB vector index's settings, a YDB
// index's partitioning and read replicas, and a MySQL FULLTEXT index's parser,
// which it writes as attributes of the index block, and a MySQL index's
// block-size hint, whose loss it reports.
func WritesIndexFacet(kind schemaext.Kind) bool {
	return kind == ydbschema.VectorIndexKind || kind == ydbschema.IndexPartitioningKind || kind == mysqlschema.IndexKind ||
		kind == mysqlschema.IndexBlockSizeKind
}

// WritesKeyFacet reports a facet kind of a primary key or a constraint the
// renderer accounts for itself: a MySQL key's block-size hint, whose loss it
// reports at the key.
func WritesKeyFacet(kind schemaext.Kind) bool { return kind == mysqlschema.IndexBlockSizeKind }
