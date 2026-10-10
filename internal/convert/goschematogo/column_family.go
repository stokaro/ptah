package goschematogo

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/ydbfamily"
)

// captureColumnFamilies validates the YDB column families each table carries
// and keeps the ones an annotation writes: every family but a default family
// that holds what YDB gives a family stating nothing (see [ydbfamily.Stated]),
// so a table nobody gave families exports none. An invalid value fails the
// export rather than disappearing from it.
func (ctx *renderContext) captureColumnFamilies() error {
	ctx.familiesByTable = make(map[string][]ydbschema.ColumnFamily)
	for _, table := range ctx.db.Tables {
		value, found, err := schemaext.FacetAs[*ydbschema.DesiredColumnFamilies](table.Facets, ydbschema.ColumnFamiliesKind)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := ydbschema.ValidateDesiredColumnFamilies(value); err != nil {
			return err
		}
		ctx.familiesByTable[table.QualifiedName()] = ydbfamily.Stated(value.Families)
	}
	return nil
}

// validateVectorDeclaration refuses vector index settings the export cannot
// write as the declaration they are: an observation that was not converted,
// or an invalid value. Either fails the export rather than disappearing from
// it.
func validateVectorDeclaration(facets schemaext.Facets) error {
	declared, found, err := schemaext.FacetAs[*ydbschema.DesiredVectorIndex](facets, ydbschema.VectorIndexKind)
	if err != nil || !found {
		return err
	}
	return ydbschema.ValidateDesiredVectorIndex(declared)
}

// isAnnotatedIndexFacet reports an index facet the export writes as
// attributes of the index annotation rather than through the selected target's
// property encoding: a YDB vector index's settings, a YDB index's
// partitioning, and the MySQL index options, which it writes for every target
// they are bound to.
func isAnnotatedIndexFacet(kind schemaext.Kind) bool {
	return kind == ydbschema.VectorIndexKind || kind == ydbschema.IndexPartitioningKind || kind == mysqlschema.IndexKind
}

// captureTablePartitioning validates the YDB settings each table carries and
// keeps them for its table directive. An invalid value fails the export
// rather than disappearing from it.
func (ctx *renderContext) captureTablePartitioning() error {
	ctx.partitioningByTable = make(map[string]*ydbschema.TablePartitioning)
	for _, table := range ctx.db.Tables {
		value, found, err := schemaext.FacetAs[*ydbschema.DesiredTablePartitioning](table.Facets, ydbschema.TablePartitioningKind)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := ydbschema.ValidateDesiredTablePartitioning(value); err != nil {
			return err
		}
		ctx.partitioningByTable[table.QualifiedName()] = &value.TablePartitioning
	}
	return nil
}

// indexKey names an index within the structs the export writes.
type indexKey struct{ table, name string }

// captureIndexPartitioning validates the YDB settings each index carries and
// keeps them for its index directive.
func (ctx *renderContext) captureIndexPartitioning() error {
	ctx.partitioningByIndex = make(map[indexKey]*ydbschema.IndexPartitioning)
	for _, index := range ctx.db.Indexes {
		value, found, err := schemaext.FacetAs[*ydbschema.DesiredIndexPartitioning](index.Facets, ydbschema.IndexPartitioningKind)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := ydbschema.ValidateDesiredIndexPartitioning(value); err != nil {
			return err
		}
		ctx.partitioningByIndex[indexKey{table: index.StructName, name: index.Name}] = &value.IndexPartitioning
	}
	return nil
}

// isAnnotatedFacet reports a table facet the export writes as an annotation of
// its own, or as attributes of the table's directive, rather than as platform
// properties.
func isAnnotatedFacet(kind schemaext.Kind) bool {
	return isTimescaleFacet(kind) || kind == ydbschema.ColumnFamiliesKind || kind == ydbschema.TablePartitioningKind ||
		kind == ydbschema.ColumnStoreKind || kind == pgpolicy.TableStateKind
}
