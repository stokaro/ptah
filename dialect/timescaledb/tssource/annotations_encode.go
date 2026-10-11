package tssource

import (
	"context"
	"fmt"
	"strconv"

	"ptah.run/core/annotation"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
)

// tableDirective is the frontend's directive that declares a table, the one
// declaration a hypertable belongs beside.
const tableDirective = "ptah:schema:table"

// annotationEncoder writes the hypertables and the continuous aggregates of
// one schema as Go annotations.
type annotationEncoder struct{}

// EncodeAnnotations writes a hypertable directive after the directive of each
// table that is one, and a continuous aggregate directive for each aggregate
// after the schema's other objects. A value that is not a valid declaration is
// refused rather than written, and so is a hypertable that is not a setting
// of a table: no directive declares one on an index, a field, a key or a
// view.
//
// A Go annotation source claims to describe every hypertable and aggregate,
// so a coverage record that limits what is known about one is refused too:
// the source has no spelling for the limit, and writing it would turn a
// hypertable nobody could describe into one the source says is absent.
func (annotationEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	var directives []annotation.EncodedDirective
	for _, slot := range request.Slots {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		if slot.Slot.Directive != tableDirective || slot.Slot.PrimaryKey {
			return annotation.EncodeResult{}, fmt.Errorf("%w: Go annotations cannot represent feature facet %q",
				ptaherr.ErrUnsupportedFeature, tsschema.HypertableKind)
		}
		hypertable, _, err := schemaext.FacetAs[*tsschema.DesiredHypertable](slot.Facets, tsschema.HypertableKind)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		if err := tsschema.ValidateDesiredHypertable(hypertable); err != nil {
			return annotation.EncodeResult{}, err
		}
		directives = append(directives, hypertableDirectiveOf(slot.Slot.Table, hypertable))
	}
	if err := tsschema.RequireNoLimits(request.Coverage); err != nil {
		return annotation.EncodeResult{}, err
	}
	for _, object := range request.Objects {
		aggregate, ok := object.Value.(*tsschema.DesiredContinuousAggregate)
		if !ok {
			return annotation.EncodeResult{}, fmt.Errorf("%w: Go annotations cannot represent feature object %s with value %T",
				ptaherr.ErrUnsupportedFeature, object.Ref, object.Value)
		}
		if err := tsschema.ValidateContinuousAggregateRef(object.Ref); err != nil {
			return annotation.EncodeResult{}, err
		}
		if err := tsschema.ValidateDesiredContinuousAggregate(aggregate); err != nil {
			return annotation.EncodeResult{}, err
		}
		attributes := annotationStrings("name", object.Ref.Name.Source, "schema", tsschema.AuthoredSchema(object.Ref), "body", aggregate.Body)
		if aggregate.MaterializedOnly != nil {
			attributes = append(attributes, annotation.EncodedAttribute{Name: "materialized_only", Value: strconv.FormatBool(*aggregate.MaterializedOnly)})
		}
		directives = append(directives, annotation.EncodedDirective{Directive: ContinuousAggregateDirective,
			Placement: annotation.AfterPrivileges, Attributes: append(attributes, annotationStrings("comment", aggregate.Comment)...)})
	}
	return annotation.EncodeResult{Directives: directives}, nil
}

// hypertableDirectiveOf writes the hypertable directive of table, which names
// the table it partitions.
func hypertableDirectiveOf(table annotation.Table, hypertable *tsschema.DesiredHypertable) annotation.EncodedDirective {
	attributes := annotationStrings("table", table.QualifiedName(), "column", hypertable.Column, "chunk_interval", hypertable.ChunkInterval)
	if hypertable.IfNotExists {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "if_not_exists", Value: "true"})
	}
	return annotation.EncodedDirective{Directive: HypertableDirective, Placement: annotation.AfterTable, Table: &table,
		Attributes: append(attributes, annotationStrings("comment", hypertable.Comment)...)}
}

// annotationStrings writes each named string a directive sets, in order, and
// leaves out one it leaves empty.
func annotationStrings(pairs ...string) []annotation.EncodedAttribute {
	attributes := make([]annotation.EncodedAttribute, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			attributes = append(attributes, annotation.EncodedAttribute{Name: pairs[i], Value: pairs[i+1]})
		}
	}
	return attributes
}
