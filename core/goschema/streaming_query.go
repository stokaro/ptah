package goschema

import (
	"fmt"
	"go/ast"
	"strconv"

	coreast "ptah.run/core/ast"
	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbstream"
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
	query := schemamodel.StreamingQuery{StructName: structName, Name: values["name"], Schema: values["schema"],
		Spec: coreast.StreamingQuerySpec{Text: values["text"], ResourcePool: values["resource_pool"]}}
	if raw, exists := values["run"]; exists {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("streaming query %q run: %w", query.Name, err)
		}
		query.Spec.Run = new(value)
	}
	if raw, exists := values["allow_state_reset"]; exists {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("streaming query %q allow_state_reset: %w", query.Name, err)
		}
		query.AllowStateReset = value
	}
	if err := ydbstream.Validate(query.Spec); err != nil {
		return fmt.Errorf("streaming query %q: %w", query.Name, err)
	}
	s.streamingQueries = append(s.streamingQueries, query)
	return nil
}
