package goschematogo

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

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
// property encoding: a YDB vector index's settings and a YDB index's
// partitioning.
func isAnnotatedIndexFacet(kind schemaext.Kind) bool {
	return kind == ydbschema.VectorIndexKind || kind == ydbschema.IndexPartitioningKind
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

// isAnnotatedFacet reports a table facet the export writes as attributes of
// the table's directive rather than as platform properties.
func isAnnotatedFacet(kind schemaext.Kind) bool {
	return kind == ydbschema.TablePartitioningKind || kind == ydbschema.ColumnStoreKind
}
