package goschema

import (
	"errors"
	"fmt"
	"go/ast"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
)

// pendingFacet is a table facet an owner directive declared. The file may
// declare the table after the directive, so the facet is attached once the
// whole file is read.
type pendingFacet struct {
	structName string
	table      string
	value      schemaext.Value
	label      string
	directive  string
	ctx        annotationErrorContext
}

// parseOwnerDirective reads a directive a selected owner declares. The
// frontend validates the attributes against the owner's grammar and hands the
// owner the declaration; what the owner returns joins the schema the way the
// frontend's own feature declarations do.
func (s *schemaParseState) parseOwnerDirective(comment *ast.Comment, directive, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//"+directive, structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	contributions, err := s.annotations.Decode(annotation.Declaration{Directive: directive, Attributes: kv, Struct: structName})
	if err != nil {
		return s.placementError(ctx, directive, err.Error())
	}
	for _, contribution := range contributions {
		if contribution.Facet != nil {
			s.pendingFacets = append(s.pendingFacets, pendingFacet{structName: structName, table: contribution.Table,
				value: contribution.Facet, label: contribution.Label, directive: directive, ctx: ctx})
			continue
		}
		objects, err := s.featureObjects.With(*contribution.Object)
		if errors.Is(err, schemaext.ErrDuplicate) {
			return s.placementError(ctx, directive, fmt.Sprintf("%s is declared twice", contribution.Label))
		}
		if err != nil {
			return s.placementError(ctx, directive, err.Error())
		}
		s.featureObjects = objects
	}
	return nil
}

// attachOwnerFacets gives each table of the file the facets owner directives
// declared for it. A facet whose table the file does not declare is refused
// rather than dropped: the setting belongs to the table, and setting it on a
// table nobody declares has no effect.
func (s *schemaParseState) attachOwnerFacets() error {
	for _, pending := range s.pendingFacets {
		index, err := s.ownerTable(pending.structName, pending.table, pending.ctx, pending.directive, pending.label)
		if err != nil {
			return err
		}
		table := &s.tableDirectives[index]
		facets, err := table.Facets.With(pending.value)
		if errors.Is(err, schemaext.ErrDuplicate) {
			return s.placementError(pending.ctx, pending.directive, fmt.Sprintf("table %q declares %s twice", table.Name, pending.label))
		}
		if err != nil {
			return err
		}
		table.Facets = facets
	}
	return nil
}
