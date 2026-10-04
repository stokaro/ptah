package goschema

import (
	"errors"
	"fmt"
	"go/ast"
	"strings"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbexternal"
)

// parseExternalDataSourceComment reads a YDB external data source declaration.
// There is no dialect scope here, for the reason a secret has none: an
// external data source is a YDB object and nothing else, and every other
// target refuses one.
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
	name, err := externalName(kv)
	if err != nil {
		return externalAttributeError(ctx, directive, err)
	}
	options, err := ydbexternal.ParseOptions(kv[ydbexternal.AttributeOptions], ydbexternal.DataSourceReserved...)
	if err != nil {
		return externalAttributeError(ctx, directive, err)
	}
	s.externalDataSources = append(s.externalDataSources, schemamodel.ExternalDataSource{
		StructName: structName,
		Name:       name,
		Schema:     externalSchema(kv),
		SourceType: strings.TrimSpace(kv[ydbexternal.AttributeSourceType]),
		Location:   strings.TrimSpace(kv[ydbexternal.AttributeLocation]),
		AuthMethod: strings.TrimSpace(kv[ydbexternal.AttributeAuthMethod]),
		Options:    options,
	})
	return nil
}

// parseExternalTableComment reads a YDB external table declaration, its
// columns included.
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
	name, err := externalName(kv)
	if err != nil {
		return externalAttributeError(ctx, directive, err)
	}
	columns, err := ydbexternal.ParseColumns(kv[ydbexternal.AttributeColumns])
	if err != nil {
		return externalAttributeError(ctx, directive, err)
	}
	options, err := ydbexternal.ParseOptions(kv[ydbexternal.AttributeOptions], ydbexternal.TableReserved...)
	if err != nil {
		return externalAttributeError(ctx, directive, err)
	}
	table := schemamodel.ExternalTable{
		StructName: structName,
		Name:       name,
		Schema:     externalSchema(kv),
		DataSource: strings.TrimSpace(kv[ydbexternal.AttributeDataSource]),
		Location:   strings.TrimSpace(kv[ydbexternal.AttributeLocation]),
		Options:    options,
	}
	for _, column := range columns {
		table.Columns = append(table.Columns, schemamodel.ExternalColumn{
			Name: column.Name, Type: column.Type, NotNull: column.NotNull,
		})
	}
	s.externalTables = append(s.externalTables, table)
	return nil
}

// externalName reads an external object's name: one path segment, since its
// directories are its schema.
func externalName(kv map[string]string) (string, error) {
	name := strings.TrimSpace(kv[ydbexternal.AttributeName])
	if strings.Contains(name, "/") {
		return "", &ydbexternal.DeclarationError{Attribute: ydbexternal.AttributeName,
			Reason: fmt.Sprintf("%q holds a slash; name the directory with %s", name, ydbexternal.AttributeSchema)}
	}
	return name, nil
}

// externalSchema reads the directory that holds an external object.
func externalSchema(kv map[string]string) string {
	return strings.Trim(strings.TrimSpace(kv[ydbexternal.AttributeSchema]), "/")
}

// externalAttributeError reports a value an external object's declaration
// cannot carry, naming the attribute.
func externalAttributeError(ctx annotationErrorContext, directive string, err error) error {
	parseErr := &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: directive,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location),
	}
	if declared, ok := errors.AsType[*ydbexternal.DeclarationError](err); ok {
		parseErr.Attribute = declared.Attribute
	}
	return parseErr
}
