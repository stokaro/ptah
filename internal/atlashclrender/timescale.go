package atlashclrender

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/feature/pgpolicy"
)

// hypertableBlock holds the validated inputs for one hypertable block.
type hypertableBlock struct {
	table schemamodel.Table
	value *tsschema.DesiredHypertable
}

// aggregateBlock holds the validated inputs for one continuous_aggregate block.
type aggregateBlock struct {
	ref   objectidentity.ID
	value *tsschema.DesiredContinuousAggregate
}

// captureTimescale validates every TimescaleDB value the document carries
// before rendering, so a malformed known value cannot become an export warning.
// A parsed document claims to describe every hypertable and aggregate, so a
// description that could not describe one is refused rather than written.
func (r *renderer) captureTimescale() error {
	if err := tsschema.RequireNoLimits(r.db.FeatureCoverage); err != nil {
		return err
	}
	for _, table := range r.db.Tables {
		value, found, err := schemaext.FacetAs[*tsschema.DesiredHypertable](table.Facets, tsschema.HypertableKind)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := tsschema.ValidateDesiredHypertable(value); err != nil {
			return err
		}
		r.hypertables = append(r.hypertables, hypertableBlock{table: table, value: value})
	}
	objects, err := r.db.FeatureObjects.All()
	if err != nil {
		return err
	}
	for _, object := range objects {
		if object.Ref.Kind != objectidentity.Kind(tsschema.ContinuousAggregateKind) {
			continue
		}
		value, ok := object.Value.(*tsschema.DesiredContinuousAggregate)
		if !ok {
			return fmt.Errorf("%w: HCL requires desired continuous aggregate values", schemaext.ErrInvalidValue)
		}
		if err := tsschema.ValidateContinuousAggregateRef(object.Ref); err != nil {
			return err
		}
		if err := tsschema.ValidateDesiredContinuousAggregate(value); err != nil {
			return err
		}
		r.aggregates = append(r.aggregates, aggregateBlock{ref: object.Ref, value: value})
	}
	return nil
}

// WritesTableFacet reports a table facet kind the renderer handles itself
// rather than as platform properties: a hypertable it writes as a block, YDB
// column families, whose loss it reports by family and which it leaves out
// where a table has only the default family stating nothing, a YDB table's
// settings, whose loss it reports by setting, and YDB column storage, whose
// loss it reports by table. A caller that encodes the other facets as
// platform properties sets these aside first.
func WritesTableFacet(kind schemaext.Kind) bool {
	return kind == tsschema.HypertableKind || kind == ydbschema.ColumnFamiliesKind || kind == ydbschema.TablePartitioningKind ||
		kind == ydbschema.ColumnStoreKind || kind == pgpolicy.TableStateKind
}

// WritesIndexFacet reports an index facet kind the renderer writes itself
// rather than as platform properties: a YDB vector index's settings, which
// it writes as attributes of the index block.
func WritesIndexFacet(kind schemaext.Kind) bool { return kind == ydbschema.VectorIndexKind }

// renderHypertables writes the TimescaleDB hypertable blocks.
//
// The block exists because nothing else in a description can say a table is
// partitioned. Measured on TimescaleDB 2.29.2 / PostgreSQL 17.11, a hypertable
// answers `relkind = 'r'` and carries no extension ownership in `pg_depend`, so
// a document that describes the table describes an ORDINARY table -- complete
// on its face, and wrong. Replaying it creates a table that is not partitioned,
// and a diff between the two reports no difference (stokaro/ptah#1026).
//
// The label is the table, because a hypertable has no name of its own.
func (r *renderer) renderHypertables() {
	blocks := slices.Clone(r.hypertables)
	slices.SortFunc(blocks, func(a, b hypertableBlock) int {
		return cmp.Or(cmp.Compare(a.table.Schema, b.table.Schema), cmp.Compare(a.table.Name, b.table.Name))
	})
	for _, block := range blocks {
		r.linef(`hypertable %s {`, quote(block.table.Name))
		if resolved := r.schemaFor(block.table.Schema); resolved != "" {
			r.rawAttr(1, "schema", r.schemaRef(resolved))
		}
		r.stringAttr(1, "column", block.value.Column)
		r.stringAttr(1, "chunk_interval", block.value.ChunkInterval)
		if block.value.IfNotExists {
			r.rawAttr(1, "if_not_exists", "true")
		}
		r.stringAttr(1, "comment", block.value.Comment)
		r.line("}")
		r.line("")
	}
}

// renderContinuousAggregates writes the TimescaleDB continuous aggregate
// blocks.
//
// The block exists because the alternative is worse than silence. To PostgreSQL
// a continuous aggregate is a view -- pg_class reports relkind 'v' -- so a
// document that had no block for one would either omit it, and plan a DROP for
// an object the server refuses to drop that way, or describe it as a view and
// replay it with the rewritten body TimescaleDB stores, which selects from a
// relation in a schema the extension owns (stokaro/ptah#1026).
//
// It is a name of its own, unlike a hypertable, so the label is that name and
// the schema is a reference like every other block's.
func (r *renderer) renderContinuousAggregates() {
	for _, block := range r.aggregates {
		r.linef(`continuous_aggregate %s {`, quote(block.ref.Name.Source))
		if resolved := r.schemaFor(tsschema.AuthoredSchema(block.ref)); resolved != "" {
			r.rawAttr(1, "schema", r.schemaRef(resolved))
		}
		r.stringAttr(1, "as", block.value.Body)
		if block.value.MaterializedOnly != nil {
			r.rawAttr(1, "materialized_only", strconv.FormatBool(*block.value.MaterializedOnly))
		}
		r.stringAttr(1, "comment", block.value.Comment)
		r.line("}")
		r.line("")
	}
}
