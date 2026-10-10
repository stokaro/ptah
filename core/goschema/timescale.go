package goschema

import (
	"errors"
	"fmt"
	"go/ast"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
)

// pendingHypertable is a hypertable annotation waiting for the table it
// partitions, which the file may declare after it.
type pendingHypertable struct {
	structName string
	table      string
	value      tsschema.DesiredHypertable
	ctx        annotationErrorContext
}

// parseHypertableComment reads a TimescaleDB hypertable declaration. The
// settings belong to the table its `table` attribute names and are attached to
// it once the whole file is read.
//
// There is no dialect scope here, for the reason [schemaParseState.parseSynonymComment]
// gives: a hypertable belongs to TimescaleDB and to nothing else.
//
// `chunk_interval` is kept as the string it was written as. The catalog reports
// the server's own spelling -- `7 days`, `1 day` -- and a declaration converted
// to something else to compare would differ from it on every run.
func (s *schemaParseState) parseHypertableComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:hypertable", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	s.hypertables = append(s.hypertables, pendingHypertable{
		structName: structName,
		table:      kv["table"],
		value: tsschema.DesiredHypertable{
			Column:        kv["column"],
			ChunkInterval: kv["chunk_interval"],
			IfNotExists:   kv["if_not_exists"] == "true",
			Comment:       kv["comment"],
		},
		ctx: ctx,
	})
	return nil
}

// attachHypertables gives each table of the file the hypertable settings an
// annotation declared for it. A hypertable whose table the file does not
// declare is refused rather than dropped, as a changefeed's is: the settings
// belong to the table, and partitioning a table nobody declares has no effect.
func (s *schemaParseState) attachHypertables() error {
	for _, pending := range s.hypertables {
		index, err := s.ownerTable(pending.structName, pending.table, pending.ctx, "ptah:schema:hypertable", "a hypertable")
		if err != nil {
			return err
		}
		table := &s.tableDirectives[index]
		facets, err := table.Facets.With(new(pending.value))
		if errors.Is(err, schemaext.ErrDuplicate) {
			return s.placementError(pending.ctx, "ptah:schema:hypertable", fmt.Sprintf("table %q declares a hypertable twice", table.Name))
		}
		if err != nil {
			return err
		}
		table.Facets = facets
	}
	return nil
}

// parseContinuousAggregateComment reads a TimescaleDB continuous aggregate
// declaration.
//
// The body is kept as it was written. The catalog stores a rewritten SELECT,
// and a live comparison puts the declaration through the same rewrite rather
// than folding either text -- so a declaration normalized here would be
// normalized twice and match nothing.
//
// There is no dialect scope here, for the reason
// [schemaParseState.parseSynonymComment] gives.
func (s *schemaParseState) parseContinuousAggregateComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:continuousaggregate", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	// A schema-qualified name without a schema attribute names the schema
	// too, as a table annotation's does: `metrics.hourly` is hourly in
	// metrics, not a relation whose name holds a dot.
	schemaName, name := tableDirectiveName(kv["schema"], kv["name"])
	object := tsschema.DesiredContinuousAggregateObject(schemaName, name, tsschema.DesiredContinuousAggregate{
		Body:             kv["body"],
		MaterializedOnly: optionalBoolAttribute(kv, "materialized_only"),
		Comment:          kv["comment"],
		StructName:       structName,
	})
	objects, err := s.featureObjects.With(object)
	if errors.Is(err, schemaext.ErrDuplicate) {
		return s.placementError(ctx, "ptah:schema:continuousaggregate", fmt.Sprintf("continuous aggregate %q is declared twice", tsschema.QualifiedName(object.Ref)))
	}
	if err != nil {
		return s.placementError(ctx, "ptah:schema:continuousaggregate", err.Error())
	}
	s.featureObjects = objects
	return nil
}

// optionalBoolAttribute answers nil for an attribute the annotation did not
// write, so a caller can tell "unset" from "set to false".
func optionalBoolAttribute(kv map[string]string, name string) *bool {
	written, present := kv[name]
	if !present {
		return nil
	}
	value := written == "true"
	return &value
}
