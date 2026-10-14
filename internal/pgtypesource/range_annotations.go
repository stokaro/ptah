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

// Annotations reads and writes PostgreSQL user type declarations. The range
// directive is the owner's own. The domain and composite directives are the
// frontend's, and the owner reads a declaration of either that names no
// target or only targets: a declaration scoped to another target, such as
// Oracle, stays on the common model. Empty optional range attributes request
// their removal; absent ones leave the catalog properties unmanaged.
func Annotations(targets []string) annotation.Extension {
	scope := func(directive string) annotation.TargetScope {
		return annotation.TargetScope{Directive: directive, Targets: slices.Clone(targets), Unscoped: true, Label: "PostgreSQL-family targets"}
	}
	return annotation.Extension{Owner: pgtype.Owner, Kinds: pgtype.Kinds(),
		Directives: rangeDirectives(), TargetScopes: []annotation.TargetScope{scope(domainDirective), scope(compositeDirective)},
		Decode: decodeTypeAnnotation, Coverage: sourceCoverage, Limits: []string{"range"},
		Encoder: typeAnnotations{}, Retainer: typeAnnotations{}, Encodes: pgtype.Kinds()}
}

func decodeTypeAnnotation(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	object, err := typeAnnotationObject(declaration)
	if err != nil {
		return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
	}
	return []annotation.Contribution{{Object: &object, Source: declaration, Label: pgtype.Label(object.Ref) + " " + pgtype.QualifiedName(object.Ref)}}, nil
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

type typeAnnotations struct{}

func (typeAnnotations) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	if ctx == nil {
		return annotation.EncodeResult{}, fmt.Errorf("%w: range export requires a context", schemaext.ErrInvalidValue)
	}
	limits, err := rangeLimits(request.Coverage.SelectKinds([]schemaext.Kind{pgtype.RangeKind}))
	if err != nil {
		return annotation.EncodeResult{}, err
	}
	result := annotation.EncodeResult{Limits: limits}
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		directive, err := encodeTypeAnnotation(object)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Directives = append(result.Directives, directive)
	}
	if err := ctx.Err(); err != nil {
		return annotation.EncodeResult{}, err
	}
	return result, nil
}

func (typeAnnotations) RetainAnnotations(ctx context.Context, request annotation.RetentionRequest) ([]bool, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: range retention requires a context", schemaext.ErrInvalidValue)
	}
	result := make([]bool, len(request.Declarations))
	for i, declaration := range request.Declarations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		expected, err := typeAnnotationObject(declaration)
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
		before, err := annotationTypeValue(expected.Value)
		if err != nil {
			return nil, err
		}
		after, err := annotationTypeValue(actual.Value)
		if err != nil {
			return nil, err
		}
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
