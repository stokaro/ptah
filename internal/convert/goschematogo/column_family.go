package goschematogo

import (
	"ptah.run/core/schemaext"
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

// isAnnotatedFacet reports a table facet the export writes as an annotation of
// its own, or as attributes of the table's directive, rather than as platform
// properties.
func isAnnotatedFacet(kind schemaext.Kind) bool {
	return isTimescaleFacet(kind) || kind == ydbschema.ColumnFamiliesKind || kind == ydbschema.TablePartitioningKind ||
		kind == ydbschema.ColumnStoreKind || kind == pgpolicy.TableStateKind
}
