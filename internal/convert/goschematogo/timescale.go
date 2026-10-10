package goschematogo

import (
	"strconv"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
)

// captureHypertables validates the hypertable settings each table carries.
// Annotations exist for them, so a valid value is written beside its table;
// an invalid one fails the export rather than disappearing from it.
func (ctx *renderContext) captureHypertables() error {
	ctx.hypertablesByTable = make(map[string]*tsschema.DesiredHypertable)
	for _, table := range ctx.db.Tables {
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
		ctx.hypertablesByTable[table.QualifiedName()] = value
	}
	return tsschema.RequireNoLimits(ctx.db.FeatureCoverage)
}

// captureContinuousAggregate records the annotation for one aggregate.
func (ctx *renderContext) captureContinuousAggregate(object schemaext.Object, value *tsschema.DesiredContinuousAggregate) error {
	if err := tsschema.ValidateContinuousAggregateRef(object.Ref); err != nil {
		return err
	}
	if err := tsschema.ValidateDesiredContinuousAggregate(value); err != nil {
		return err
	}
	schema := tsschema.AuthoredSchema(object.Ref)
	attrs := []attr{
		{name: "name", value: object.Ref.Name.Source, set: true},
		{name: "schema", value: schema, set: schema != ""},
		{name: "body", value: value.Body, set: true},
	}
	if value.MaterializedOnly != nil {
		attrs = append(attrs, attr{name: "materialized_only", value: strconv.FormatBool(*value.MaterializedOnly), set: true})
	}
	attrs = append(attrs, attr{name: "comment", value: value.Comment, set: value.Comment != ""})
	ctx.aggregateAnnotations = append(ctx.aggregateAnnotations, annotation("ptah:schema:continuousaggregate", attrs...))
	return nil
}

// hypertableAnnotation writes the hypertable settings of one table.
func hypertableAnnotation(table schemamodel.Table, value *tsschema.DesiredHypertable) string {
	return annotation("ptah:schema:hypertable",
		attr{name: "table", value: table.QualifiedName(), set: true},
		attr{name: "column", value: value.Column, set: true},
		attr{name: "chunk_interval", value: value.ChunkInterval, set: value.ChunkInterval != ""},
		attr{name: "if_not_exists", value: "true", set: value.IfNotExists},
		attr{name: "comment", value: value.Comment, set: value.Comment != ""},
	)
}

// writeContinuousAggregates writes the aggregate annotations in identity order.
func (ctx *renderContext) writeContinuousAggregates(w *sourceWriter) {
	for _, text := range ctx.aggregateAnnotations {
		w.writeComment(text)
	}
}

// isTimescaleFacet reports a table facet the export writes as an annotation.
func isTimescaleFacet(kind schemaext.Kind) bool {
	return kind == tsschema.HypertableKind
}

// isTimescaleObject reports a named object the export writes as an annotation.
func isTimescaleObject(ref objectidentity.ID) bool {
	return ref.Kind == objectidentity.Kind(tsschema.ContinuousAggregateKind)
}
