package tssource

import (
	"context"
	"fmt"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
)

// hclEncoder writes the hypertables and the continuous aggregates of one
// document.
type hclEncoder struct{}

// EncodeHCL writes a hypertable block for each table that is one and a
// continuous_aggregate block for each aggregate, after the tables they are
// about. A value that is not a valid declaration is refused rather than
// written.
//
// A document's claim is that it describes every hypertable and aggregate, so
// a coverage record that limits what is known about one is refused too: the
// format has no spelling for the limit, and writing the document would turn a
// hypertable nobody could describe into one the document says is absent.
func (hclEncoder) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if err := tsschema.RequireNoLimits(request.Coverage); err != nil {
		return hclext.EncodeResult{}, err
	}
	blocks := make([]hclext.EncodedBlock, 0, len(request.Tables)+len(request.Objects))
	for _, table := range request.Tables {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		hypertable, found, err := schemaext.FacetAs[*tsschema.DesiredHypertable](table.Facets, tsschema.HypertableKind)
		if err != nil {
			return hclext.EncodeResult{}, err
		}
		if !found {
			continue
		}
		if err := tsschema.ValidateDesiredHypertable(hypertable); err != nil {
			return hclext.EncodeResult{}, err
		}
		blocks = append(blocks, hclHypertableBlock(table.Table, hypertable))
	}
	for _, object := range request.Objects {
		aggregate, ok := object.Value.(*tsschema.DesiredContinuousAggregate)
		if !ok {
			return hclext.EncodeResult{}, fmt.Errorf("%w: HCL requires desired continuous aggregate values", schemaext.ErrInvalidValue)
		}
		if err := tsschema.ValidateContinuousAggregateRef(object.Ref); err != nil {
			return hclext.EncodeResult{}, err
		}
		if err := tsschema.ValidateDesiredContinuousAggregate(aggregate); err != nil {
			return hclext.EncodeResult{}, err
		}
		attributes := hclStrings("as", aggregate.Body)
		if aggregate.MaterializedOnly != nil {
			attributes = append(attributes, hclext.EncodedAttribute{Name: "materialized_only", Value: hclext.Value{Kind: hclext.Bool, Bool: *aggregate.MaterializedOnly}})
		}
		blocks = append(blocks, hclext.EncodedBlock{Type: aggregateBlock, Placement: hclext.AfterTables,
			Schema: tsschema.AuthoredSchema(object.Ref), Name: object.Ref.Name.Source, Object: object.Ref,
			Attributes: append(attributes, hclStrings("comment", aggregate.Comment)...)})
	}
	return hclext.EncodeResult{Blocks: blocks}, nil
}

// hclHypertableBlock writes the hypertable block of table.
//
// The block exists because nothing else in a description can say a table is
// partitioned. Measured on TimescaleDB 2.29.2 / PostgreSQL 17.11, a hypertable
// answers `relkind = 'r'` and carries no extension ownership in `pg_depend`,
// so a document that describes the table describes an ORDINARY table --
// complete on its face, and wrong. Replaying it creates a table that is not
// partitioned, and a diff between the two reports no difference
// (stokaro/ptah#1026). The label is the table, because a hypertable has no
// name of its own.
//
// The continuous_aggregate block exists for the same issue: to PostgreSQL an
// aggregate is a view, and a document without the block would either omit
// it, planning a DROP the server refuses, or describe it as a view and replay
// the rewritten body TimescaleDB stores.
func hclHypertableBlock(table hclext.Table, hypertable *tsschema.DesiredHypertable) hclext.EncodedBlock {
	attributes := hclStrings("column", hypertable.Column, "chunk_interval", hypertable.ChunkInterval)
	if hypertable.IfNotExists {
		attributes = append(attributes, hclext.EncodedAttribute{Name: "if_not_exists", Value: hclext.Value{Kind: hclext.Bool, Bool: true}})
	}
	return hclext.EncodedBlock{Type: hypertableBlock, Placement: hclext.AfterTables, Schema: table.Schema, Name: table.Name,
		Attributes: append(attributes, hclStrings("comment", hypertable.Comment)...)}
}

// hclStrings writes each named string a block sets, in order, and leaves out
// one it leaves empty.
func hclStrings(pairs ...string) []hclext.EncodedAttribute {
	attributes := make([]hclext.EncodedAttribute, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			attributes = append(attributes, hclext.EncodedAttribute{Name: pairs[i], Value: hclext.Value{Kind: hclext.String, Text: pairs[i+1]}})
		}
	}
	return attributes
}
