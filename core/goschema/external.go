package goschema

import (
	"errors"
	"fmt"
	"go/ast"
	"strings"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/dialect/ydb/ydbexternal"
)

// parseExternalDataSourceComment reads a YDB external data source declaration
// and declares it as a feature object. There is no dialect scope here, for the
// reason a secret has none: an external data source is a YDB object and
// nothing else, and every other target refuses one.
func (s *schemaParseState) parseExternalDataSourceComment(comment *ast.Comment, structName string) error {
	const directive = "ptah:schema:externaldatasource"
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//"+directive, structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	options, err := ydbexternal.ParseOptions(kv[ydbexternal.AttributeOptions], ydbexternal.DataSourceReserved...)
	if err != nil {
		return externalAttributeError(ctx, directive, err)
	}
	objects, err := ydbexternal.DeclareSource(s.featureObjects, kv[ydbexternal.AttributeSchema], kv[ydbexternal.AttributeName], structName,
		ydbexternal.DataSource{
			SourceType: strings.TrimSpace(kv[ydbexternal.AttributeSourceType]),
			Location:   strings.TrimSpace(kv[ydbexternal.AttributeLocation]),
			AuthMethod: strings.TrimSpace(kv[ydbexternal.AttributeAuthMethod]),
			Options:    options,
		})
	if err != nil {
		return externalAttributeError(ctx, directive, err)
	}
	s.featureObjects = objects
	return nil
}

// parseExternalTableComment reads a YDB external table declaration, its
// columns included, and declares it as a feature object.
func (s *schemaParseState) parseExternalTableComment(comment *ast.Comment, structName string) error {
	const directive = "ptah:schema:externaltable"
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//"+directive, structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	columns, err := ydbexternal.ParseColumns(kv[ydbexternal.AttributeColumns])
	if err != nil {
		return externalAttributeError(ctx, directive, err)
	}
	options, err := ydbexternal.ParseOptions(kv[ydbexternal.AttributeOptions], ydbexternal.TableReserved...)
	if err != nil {
		return externalAttributeError(ctx, directive, err)
	}
	objects, err := ydbexternal.DeclareTable(s.featureObjects, kv[ydbexternal.AttributeSchema], kv[ydbexternal.AttributeName], structName,
		ydbexternal.Table{
			DataSource: strings.TrimSpace(kv[ydbexternal.AttributeDataSource]),
			Location:   strings.TrimSpace(kv[ydbexternal.AttributeLocation]),
			Columns:    columns,
			Options:    options,
		})
	if err != nil {
		return externalAttributeError(ctx, directive, err)
	}
	s.featureObjects = objects
	return nil
}

// externalAttributeError reports a value an external object's declaration
// cannot carry, naming the attribute. The error wraps
// [ptaherr.ErrInvalidAttributeValue] and the owner's own error, so an object
// declared twice still matches [schemaext.ErrDuplicate], as it does in YAML,
// in YQL and across merged files.
func externalAttributeError(ctx annotationErrorContext, directive string, err error) error {
	parseErr := &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: directive,
		Err:       fmt.Errorf("%w: %w", ptaherr.ErrInvalidAttributeValue, err),
		Message:   fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location),
	}
	if declared, ok := errors.AsType[*ydbexternal.DeclarationError](err); ok {
		parseErr.Attribute = declared.Attribute
	}
	if _, ok := errors.AsType[*ydbexternal.DuplicateError](err); ok {
		parseErr.Attribute = ydbexternal.AttributeName
	}
	return parseErr
}
