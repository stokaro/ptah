package goschema

import (
	"fmt"
	"go/ast"
	"strconv"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbstreaming"
)

func (s *schemaParseState) parseStreamingQueryComment(comment *ast.Comment, structName string) error {
	values := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:streamingquery", structName)
	if err := validateAttributes(values, ctx); err != nil {
		return err
	}
	if err := requireAttributes(values, ctx); err != nil {
		return err
	}
	query := ydbstreaming.Desired{StructName: structName,
		Spec: ydbstreaming.Spec{Text: values["text"], ResourcePool: values["resource_pool"]}}
	if raw, exists := values["run"]; exists {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("streaming query %q run: %w", values["name"], err)
		}
		query.Spec.Run = new(value)
	}
	if raw, exists := values["allow_state_reset"]; exists {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("streaming query %q allow_state_reset: %w", values["name"], err)
		}
		query.AllowStateReset = value
	}
	if err := ydbstreaming.Validate(query.Spec); err != nil {
		return fmt.Errorf("streaming query %q: %w", values["name"], err)
	}
	ref := ydbstreaming.Ref(values["schema"], values["name"])
	if err := ydbstreaming.ValidateIdentity(ref); err != nil {
		return err
	}
	var err error
	s.featureObjects, err = s.featureObjects.With(schemaext.Object{Ref: ref, Value: &query})
	return err
}
