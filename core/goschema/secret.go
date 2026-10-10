package goschema

import (
	"errors"
	"fmt"
	"go/ast"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/dialect/ydb/ydbsecret"
)

// parseSecretComment reads a YDB secret declaration: the secret's path and
// the environment variable its value comes from.
//
// A declaration that writes the value itself is refused before anything else
// reads it, and the refusal names the attribute and never its value, so a
// secret written into a source file by mistake reaches no log and no error.
// There is no dialect scope here, for the reason a synonym has none: a secret
// is a YDB object and nothing else, and every other target refuses one.
func (s *schemaParseState) parseSecretComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:secret", structName)
	if _, written := kv[ydbsecret.AttributeValue]; written {
		return secretAttributeError(ctx, &ydbsecret.DeclarationError{
			Attribute: ydbsecret.AttributeValue, Reason: ydbsecret.LiteralValueRefusal})
	}
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	valueEnv, err := ydbsecret.ParseValueEnv(kv)
	if err != nil {
		return secretAttributeError(ctx, err)
	}
	objects, err := ydbsecret.Declare(s.featureObjects, kv[ydbsecret.AttributeSchema], kv[ydbsecret.AttributeName], structName, valueEnv)
	if err != nil {
		return secretAttributeError(ctx, err)
	}
	s.featureObjects = objects
	return nil
}

// secretAttributeError reports a value a secret declaration cannot carry,
// naming the attribute. The error wraps [ptaherr.ErrInvalidAttributeValue] and
// the owner's own error, so a secret declared twice still matches
// [schemaext.ErrDuplicate].
func secretAttributeError(ctx annotationErrorContext, err error) error {
	parseErr := &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: "ptah:schema:secret",
		Err:       fmt.Errorf("%w: %w", ptaherr.ErrInvalidAttributeValue, err),
		Message:   fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location),
	}
	if declared, ok := errors.AsType[*ydbsecret.DeclarationError](err); ok {
		parseErr.Attribute = declared.Attribute
	}
	if _, ok := errors.AsType[*ydbsecret.DuplicateError](err); ok {
		parseErr.Attribute = ydbsecret.AttributeName
	}
	return parseErr
}
