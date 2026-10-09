package goschema

import (
	"errors"
	"fmt"
	"go/ast"
	"strings"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
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
	name := strings.TrimSpace(kv[ydbsecret.AttributeName])
	if err := ydbsecret.CheckName(name); err != nil {
		return secretAttributeError(ctx, err)
	}
	valueEnv, err := ydbsecret.ParseValueEnv(kv)
	if err != nil {
		return secretAttributeError(ctx, err)
	}
	object := ydbsecret.DesiredObject(strings.Trim(strings.TrimSpace(kv[ydbsecret.AttributeSchema]), "/"), name, structName, valueEnv)
	if err := ydbsecret.ValidateIdentity(object.Ref); err != nil {
		return secretAttributeError(ctx, err)
	}
	s.featureObjects, err = s.featureObjects.With(object)
	if errors.Is(err, schemaext.ErrDuplicate) {
		return secretAttributeError(ctx, fmt.Errorf("secret %s is declared twice", ydbsecret.Display(object.Ref.Schema.Source, name)))
	}
	return err
}

// secretAttributeError reports a value a secret declaration cannot carry,
// naming the attribute.
func secretAttributeError(ctx annotationErrorContext, err error) error {
	parseErr := &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: "ptah:schema:secret",
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location),
	}
	if declared, ok := errors.AsType[*ydbsecret.DeclarationError](err); ok {
		parseErr.Attribute = declared.Attribute
	}
	return parseErr
}
