package pgtypesource

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/dialectscope"
	"ptah.run/internal/tableref"
)

const rangeDirective = "ptah:schema:range"

// Annotations reads and writes range declarations through the type owner.
// Empty optional attributes request their removal; absent attributes leave
// the corresponding catalog properties unmanaged.
func Annotations() annotation.Extension {
	attrs := []annotation.Attribute{
		{Name: "name", Description: "Range type name.", Value: "string", Required: true},
		{Name: "schema", Description: "Target schema/namespace.", Value: "string"},
		{Name: "subtype", Description: "Element subtype the range is built over.", Value: "string", Required: true},
		{Name: "subtype_opclass", Description: "Operator class for the subtype.", Value: "string"},
		{Name: "collation", Description: "Collation for the subtype.", Value: "string"},
		{Name: "canonical", Description: "Canonicalization function.", Value: "string"},
		{Name: "subtype_diff", Description: "Subtype difference function.", Value: "string"},
		{Name: "comment", Description: "Range type comment.", Value: "string"},
		{Name: "dialects", Description: "Comma-separated target dialects for this declaration.", Value: "list"},
	}
	return annotation.Extension{Owner: pgtype.Owner, Kinds: []schemaext.Kind{pgtype.RangeKind},
		Directives: []annotation.Directive{{Name: rangeDirective, Description: "Declares a PostgreSQL range type.", Scopes: []annotation.Scope{annotation.ScopeStruct}, Attributes: attrs}},
		Decode:     decodeRangeAnnotation, Coverage: rangeCoverage, Limits: []string{"range"},
		Encoder: rangeAnnotations{}, Retainer: rangeAnnotations{}, Encodes: []schemaext.Kind{pgtype.RangeKind},
	}
}

func decodeRangeAnnotation(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	object, err := rangeAnnotationObject(declaration)
	if err != nil {
		return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
	}
	return []annotation.Contribution{{Object: &object, Source: declaration, Label: "range " + pgtype.QualifiedName(object.Ref)}}, nil
}

func rangeAnnotationObject(declaration annotation.Declaration) (schemaext.Object, error) {
	if declaration.Directive != rangeDirective {
		return schemaext.Object{}, fmt.Errorf("%w: unexpected range directive %q", schemaext.ErrInvalidValue, declaration.Directive)
	}
	attrs := declaration.Attributes
	targets := declaration.Targets
	if raw, present := attrs["dialects"]; present {
		var err error
		targets, err = dialectscope.Parse(raw)
		if err != nil {
			return schemaext.Object{}, &annotation.DeclarationError{Declaration: declaration, Attribute: "dialects", Err: err}
		}
	}
	schema, name := rangeAnnotationNames(attrs["schema"], attrs["name"])
	value := &pgtype.DesiredRange{Subtype: strings.TrimSpace(attrs["subtype"]), SubtypeOpClass: optionalRangeValue(attrs["subtype_opclass"]),
		Collation: optionalRangeValue(attrs["collation"]), Canonical: optionalRangeValue(attrs["canonical"]), SubtypeDiff: optionalRangeValue(attrs["subtype_diff"]),
		Comment: strings.TrimSpace(attrs["comment"]), StructName: declaration.Struct}
	for _, key := range pgtype.RangeAttributes() {
		if text, present := attrs[key]; present && strings.TrimSpace(text) == "" {
			value.Cleared = append(value.Cleared, key)
		}
	}
	object, err := pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, schema, name), value)
	object.Targets = slices.Clone(targets)
	return object, err
}

type rangeAnnotations struct{}

func (rangeAnnotations) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	if ctx == nil {
		return annotation.EncodeResult{}, fmt.Errorf("%w: range export requires a context", schemaext.ErrInvalidValue)
	}
	limits, err := rangeLimits(request.Coverage)
	if err != nil {
		return annotation.EncodeResult{}, err
	}
	result := annotation.EncodeResult{Limits: limits}
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		value, ok := object.Value.(*pgtype.DesiredRange)
		if !ok {
			return annotation.EncodeResult{}, fmt.Errorf("%w: range export requires a declaration", schemaext.ErrInvalidValue)
		}
		if _, err := pgtype.DesiredObject(object.Ref, value); err != nil {
			return annotation.EncodeResult{}, err
		}
		attrs := []annotation.EncodedAttribute{{Name: "name", Value: tableref.CanonicalExact("", object.Ref.Name.Source)}}
		if schema := pgtype.AuthoredSchema(object.Ref); schema != "" {
			attrs = append(attrs, annotation.EncodedAttribute{Name: "schema", Value: tableref.CanonicalExact("", schema)})
		}
		attrs = append(attrs, annotation.EncodedAttribute{Name: "subtype", Value: value.Subtype})
		for _, name := range pgtype.RangeAttributes() {
			if text := value.Attribute(name); text != "" || value.Clears(name) {
				attrs = append(attrs, annotation.EncodedAttribute{Name: name, Value: text})
			}
		}
		if value.Comment != "" {
			attrs = append(attrs, annotation.EncodedAttribute{Name: "comment", Value: value.Comment})
		}
		result.Directives = append(result.Directives, annotation.EncodedDirective{Directive: rangeDirective, Placement: annotation.AfterSchemas, Attributes: attrs, Targets: slices.Clone(object.Targets)})
	}
	if err := ctx.Err(); err != nil {
		return annotation.EncodeResult{}, err
	}
	return result, nil
}

func (rangeAnnotations) RetainAnnotations(ctx context.Context, request annotation.RetentionRequest) ([]bool, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: range retention requires a context", schemaext.ErrInvalidValue)
	}
	result := make([]bool, len(request.Declarations))
	for i, declaration := range request.Declarations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		expected, err := rangeAnnotationObject(declaration)
		if err != nil {
			return nil, err
		}
		actual, found, err := request.Objects.Get(expected.Ref)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		value, ok := actual.Value.(*pgtype.DesiredRange)
		if !ok || value == nil {
			continue
		}
		before, ok := expected.Value.(*pgtype.DesiredRange)
		if !ok || before == nil {
			return nil, fmt.Errorf("%w: range annotation does not declare a range", schemaext.ErrInvalidValue)
		}
		after := value.Copy()
		before.StructName, after.StructName = "", ""
		result[i] = before.Equal(after) && slices.Equal(expected.Targets, actual.Targets)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// optionalRangeValue makes a whitespace-only attribute an explicit clearing.
func optionalRangeValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return value
}

// rangeAnnotationNames decodes SQL identifier quotes before building an
// identity. Schema and name attributes may quote significant spaces or dots.
func rangeAnnotationNames(schema, name string) (schemaName, typeName string) {
	schema, name = annotation.QualifiedName(schema, name)
	if parsed, valid := tableref.Parse(schema); valid && !parsed.Qualified {
		schema = parsed.Name
	}
	return schema, name
}
