package goschema

import (
	"errors"
	"fmt"
	"go/ast"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/dialect/ydb/ydbworkload"
)

// parseResourcePoolComment reads a YDB resource pool declaration.
//
// There is no dialect scope here, for the reason a synonym has none: a pool is
// a YDB object and nothing else, and every other target refuses one.
func (s *schemaParseState) parseResourcePoolComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:resourcepool", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	name, spec, err := ydbworkload.ParsePool(kv)
	if err != nil {
		return resourcePoolAttributeError(ctx, "ptah:schema:resourcepool", err)
	}
	s.featureObjects, err = s.featureObjects.With(ydbworkload.DesiredPoolObject(name, structName, spec))
	return err
}

// parseResourcePoolClassifierComment reads a YDB resource pool classifier
// declaration. A classifier names its pool by an attribute rather than by
// where it is written, since it may name the pool `default`, which no
// declaration creates.
func (s *schemaParseState) parseResourcePoolClassifierComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:resourcepool:classifier", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	name, spec, err := ydbworkload.ParseClassifier(kv)
	if err != nil {
		return resourcePoolAttributeError(ctx, "ptah:schema:resourcepool:classifier", err)
	}
	s.featureObjects, err = s.featureObjects.With(ydbworkload.DesiredClassifierObject(name, structName, spec))
	return err
}

// resourcePoolAttributeError reports a value a pool or classifier declaration
// cannot carry, naming the attribute.
func resourcePoolAttributeError(ctx annotationErrorContext, directive string, err error) error {
	parseErr := &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: directive,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location),
	}
	if declared, ok := errors.AsType[*ydbworkload.DeclarationError](err); ok {
		parseErr.Attribute = declared.Attribute
	}
	return parseErr
}
