package goschema

import (
	"errors"
	"fmt"
	"go/ast"

	"ptah.run/core/annotation"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// pendingFacet is a table facet an owner directive declared. The file may
// declare the table after the directive, so the facet is attached once the
// whole file is read.
type pendingFacet struct {
	structName string
	table      string
	value      schemaext.Value
	targets    []string
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
	declaration := annotation.Declaration{Directive: directive, Attributes: kv, Struct: structName, Line: ctx.line, File: ctx.file}
	contributions, err := s.owners.Decode(declaration)
	if err != nil {
		return s.ownerError(declaration, err)
	}
	return s.contribute(declaration, contributions)
}

// finishOwnerDirectives asks the owners for what the file's declarations
// declare together, now that the file's tables are known.
func (s *schemaParseState) finishOwnerDirectives() error {
	tables := make(annotation.Tables, 0, len(s.tableDirectives))
	for _, table := range s.tableDirectives {
		tables = append(tables, annotation.Table{Schema: table.Schema, Name: table.Name, Struct: table.StructName})
	}
	contributions, err := s.owners.Finish(tables)
	if err != nil {
		return s.ownerError(annotation.Declaration{}, err)
	}
	for _, contribution := range contributions {
		if err := s.contribute(contribution.Source, []annotation.Contribution{contribution}); err != nil {
			return err
		}
	}
	return nil
}

// contribute joins what an owner declared for declaration to the file's
// schema: an object at once, and a facet once the file's tables are known.
func (s *schemaParseState) contribute(declaration annotation.Declaration, contributions []annotation.Contribution) error {
	ctx := s.declarationContext(declaration)
	for _, contribution := range contributions {
		if contribution.Facet != nil {
			s.pendingFacets = append(s.pendingFacets, pendingFacet{structName: declaration.Struct, table: contribution.Table,
				value: contribution.Facet, targets: contribution.Targets, label: contribution.Label, directive: declaration.Directive, ctx: ctx})
			continue
		}
		objects, err := s.featureObjects.With(*contribution.Object)
		if errors.Is(err, schemaext.ErrDuplicate) {
			return s.placementError(ctx, declaration.Directive, fmt.Sprintf("%s is declared twice", contribution.Label))
		}
		if err != nil {
			return s.placementError(ctx, declaration.Directive, err.Error())
		}
		s.featureObjects = objects
	}
	return nil
}

// ownerError reports an owner's refusal of the declaration being decoded, or
// of the one a [annotation.DeclarationError] names. It wraps
// [ptaherr.ErrInvalidAttributeValue] and the owner's own error, so a sentinel
// the owner's refusal carries, such as [schemaext.ErrDuplicate], still
// matches.
func (s *schemaParseState) ownerError(declaration annotation.Declaration, err error) error {
	attribute := ""
	if declared, ok := errors.AsType[*annotation.DeclarationError](err); ok {
		attribute = declared.Attribute
		if declared.Declaration.Directive != "" {
			declaration = declared.Declaration
		}
		err = declared.Err
	}
	ctx := s.declarationContext(declaration)
	return &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: declaration.Directive,
		Attribute: attribute,
		Err:       fmt.Errorf("%w: %w", ptaherr.ErrInvalidAttributeValue, err),
		Message:   fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location),
	}
}

// declarationContext locates a declaration an owner was handed.
func (s *schemaParseState) declarationContext(declaration annotation.Declaration) annotationErrorContext {
	return annotationErrorContext{
		file:      s.filename,
		line:      declaration.Line,
		directive: "//" + declaration.Directive,
		location:  declaration.Struct,
		catalog:   s.catalog,
	}
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
		if len(pending.targets) > 0 {
			if facets, err = facets.WithTargetScope(pending.value.Kind(), pending.targets...); err != nil {
				return err
			}
		}
		table.Facets = facets
	}
	return nil
}

// ownerTable finds the table an annotation declaring one of the table's parts
// belongs to, under the rule [annotation.Tables.Owning] states, so an owner
// and the frontend place a part alike. object names the part, as the refusal
// reads it.
func (s *schemaParseState) ownerTable(structName, table string, ctx annotationErrorContext, directive, object string) (int, error) {
	tables := make(annotation.Tables, 0, len(s.tableDirectives))
	for _, declared := range s.tableDirectives {
		tables = append(tables, annotation.Table{Schema: declared.Schema, Name: declared.Name, Struct: declared.StructName})
	}
	index, err := tables.Owning(structName, table, object)
	if err != nil {
		return -1, s.placementError(ctx, directive, err.Error())
	}
	return index, nil
}

// placementError reports an annotation that cannot be placed: one whose table
// cannot be found, or that declares a part its table already has.
func (s *schemaParseState) placementError(ctx annotationErrorContext, directive, reason string) error {
	return &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: directive,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%s on %s at %s", reason, ctx.directive, ctx.location),
	}
}
