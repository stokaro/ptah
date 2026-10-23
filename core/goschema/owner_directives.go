package goschema

import (
	"errors"
	"fmt"
	"go/ast"
	"go/token"

	"ptah.run/core/annotation"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/annotationmeta"
)

// pendingFacet is a table facet an owner directive declared. The file may
// declare the table after the directive, so the facet is attached once the
// whole file is read.
type pendingFacet struct {
	structName string
	table      string
	// subject is the column or index the facet belongs to instead of the table.
	subject   annotation.Subject
	value     schemaext.Value
	targets   []string
	label     string
	directive string
	ctx       annotationErrorContext
}

// parseOwnerDirective reads a directive a selected owner declares. The
// frontend validates the attributes against the owner's grammar and hands the
// owner the declaration; what the owner returns joins the schema the way the
// frontend's own feature declarations do.
func (s *schemaParseState) parseOwnerDirective(comment *ast.Comment, directive annotationmeta.Directive, target schemaCommentTarget) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//"+directive.Name, target.structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	subject, err := s.ownerSubject(directive, target, ctx)
	if err != nil {
		return err
	}
	declaration := annotation.Declaration{Directive: directive.Name, Attributes: kv, Struct: target.structName, Line: ctx.line, File: ctx.file, Subject: subject}
	declaration.Start, declaration.End, declaration.FileSize = s.commentSpan(comment)
	contributions, err := s.owners.Decode(declaration)
	if err != nil {
		return s.ownerError(declaration, err)
	}
	return s.contribute(declaration, contributions)
}

// declareOwnerAttributes hands the attributes owners add to one of the
// frontend's own directives, which they read per file, to those owners with the
// part of the table the directive describes. The facets they return attach to
// that part once the file is read.
func (s *schemaParseState) declareOwnerAttributes(comment *ast.Comment, directive string, kv map[string]string, subject annotation.Subject, location string) error {
	ctx := s.annotationContext(comment, "//"+directive, location)
	declaration := annotation.Declaration{Directive: directive, Attributes: kv, Struct: subject.Struct, Line: ctx.line, File: ctx.file, Subject: subject}
	declaration.Start, declaration.End, declaration.FileSize = s.commentSpan(comment)
	contributions, err := s.owners.DecodeAttributes(declaration)
	if err != nil {
		return ownerAttributeError(ctx, err)
	}
	if len(contributions) == 0 {
		return nil
	}
	return s.contribute(declaration, contributions)
}

// ownerSubject finds the column or index a directive describes, from the
// ptah:schema:field or ptah:schema:index directive in the comment group it is
// written in. A group that names none, or an index group that names several, is
// refused: the owner would otherwise decide for a part the author did not name.
func (s *schemaParseState) ownerSubject(directive annotationmeta.Directive, target schemaCommentTarget, ctx annotationErrorContext) (annotation.Subject, error) {
	var (
		sibling string
		kind    annotation.SubjectKind
	)
	switch directive.Describes {
	case annotation.SubjectColumn:
		sibling, kind = "ptah:schema:field", annotation.SubjectColumn
	case annotation.SubjectIndex:
		sibling, kind = "ptah:schema:index", annotation.SubjectIndex
	default:
		return annotation.Subject{}, nil
	}
	article := map[annotation.SubjectKind]string{annotation.SubjectColumn: "a column", annotation.SubjectIndex: "an index"}
	var written []map[string]string
	if target.group != nil {
		for _, other := range target.group.List {
			if matched, ok := s.catalog.MatchCommentDirective(other.Text); ok && matched.Name == sibling {
				written = append(written, s.kv.ParseKeyValueComment(other.Text))
			}
		}
	}
	switch {
	case len(written) == 0:
		return annotation.Subject{}, s.placementError(ctx, directive.Name, fmt.Sprintf("%s directive is written beside the %s directive of its %s", article[kind], sibling, kind))
	case len(written) > 1:
		return annotation.Subject{}, s.placementError(ctx, directive.Name, fmt.Sprintf("this comment has %d %s directives, and %s directive describes exactly one", len(written), sibling, article[kind]))
	case written[0]["name"] == "":
		return annotation.Subject{}, s.placementError(ctx, directive.Name, fmt.Sprintf("the %s directive it describes needs a name attribute", sibling))
	}
	subject := annotation.Subject{Kind: kind, Struct: target.structName, Name: written[0]["name"]}
	if kind == annotation.SubjectIndex {
		subject.Table = written[0]["table"]
	}
	return subject, nil
}

// commentSpan locates a comment in its file for an owner that reports where a
// declaration is written. It returns zero positions where the parse has no
// file set.
func (s *schemaParseState) commentSpan(comment *ast.Comment) (start, end annotation.SourcePosition, size int) {
	if s.fset == nil || comment == nil {
		return annotation.SourcePosition{}, annotation.SourcePosition{}, 0
	}
	position := func(pos token.Pos) annotation.SourcePosition {
		located := s.fset.Position(pos)
		return annotation.SourcePosition{Offset: located.Offset, Line: located.Line, Column: located.Column}
	}
	if file := s.fset.File(comment.Slash); file != nil {
		size = file.Size()
	}
	return position(comment.Slash), position(comment.End()), size
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
			s.pendingFacets = append(s.pendingFacets, pendingFacet{structName: declaration.Struct, table: contribution.Table, subject: contribution.Subject,
				value: contribution.Facet, targets: contribution.Targets, label: contribution.Label, directive: declaration.Directive, ctx: ctx})
			continue
		}
		objects, err := s.featureObjects.Declare(*contribution.Object)
		if redeclarable, ok := contribution.Object.Value.(schemaext.Redeclarable); ok && redeclarable.Redeclarable() &&
			errors.Is(err, schemaext.ErrDuplicate) {
			return s.placementError(ctx, declaration.Directive, fmt.Sprintf("conflicting %s definitions", contribution.Label))
		}
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
	message := fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location)
	if declaration.Directive == "" {
		// A refusal that names no declaration has no place to report.
		message = err.Error()
	}
	return &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: declaration.Directive,
		Attribute: attribute,
		Err:       fmt.Errorf("%w: %w", ptaherr.ErrInvalidAttributeValue, err),
		Message:   message,
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
		if pending.subject.Kind == annotation.SubjectColumn || pending.subject.Kind == annotation.SubjectIndex {
			if err := s.attachPartFacet(pending); err != nil {
				return err
			}
			continue
		}
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

// attachPartFacet gives the facet an owner declared for a column or an index
// to every field or index of the file that the subject names. The facet's
// subject is refused rather than dropped when the file declares no such part,
// or when the part already carries a facet of the same model.
func (s *schemaParseState) attachPartFacet(pending pendingFacet) error {
	subject := pending.subject
	var slots []*schemaext.Facets
	switch subject.Kind {
	case annotation.SubjectColumn:
		for i := range s.schemaFields {
			if s.schemaFields[i].StructName == subject.Struct && s.schemaFields[i].Name == subject.Name {
				slots = append(slots, &s.schemaFields[i].Facets)
			}
		}
	case annotation.SubjectIndex:
		for i := range s.schemaIndexes {
			if s.schemaIndexes[i].StructName == subject.Struct && s.schemaIndexes[i].Name == subject.Name && s.schemaIndexes[i].TableName == subject.Table {
				slots = append(slots, &s.schemaIndexes[i].Facets)
			}
		}
	}
	if len(slots) == 0 {
		return s.placementError(pending.ctx, pending.directive, fmt.Sprintf("%s %q of struct %s is not declared in this file, and %s is declared beside it",
			subject.Kind, subject.Name, subject.Struct, pending.label))
	}
	for _, slot := range slots {
		facets, err := slot.With(pending.value)
		if errors.Is(err, schemaext.ErrDuplicate) {
			return s.placementError(pending.ctx, pending.directive, fmt.Sprintf("%s %q declares %s twice", subject.Kind, subject.Name, pending.label))
		}
		if err != nil {
			return err
		}
		if len(pending.targets) > 0 {
			if facets, err = facets.WithTargetScope(pending.value.Kind(), pending.targets...); err != nil {
				return err
			}
		}
		*slot = facets
	}
	return nil
}
