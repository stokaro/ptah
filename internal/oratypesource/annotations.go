// Package oratypesource connects the Oracle user type models of package
// oratype to schema sources. It reads and writes domain and object type
// declarations in Go annotations, HCL and SQL through the source contracts,
// without owning a source frontend.
//
// The syntax is shared with PostgreSQL: the ptah:schema:domain and
// ptah:schema:composite directives, the domain and composite HCL blocks, and
// CREATE DOMAIN. A declaration reaches this owner only where its scope or its
// document names Oracle; one that names no target belongs to the PostgreSQL
// owner.
package oratypesource

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/feature/oratype"
	"ptah.run/internal/tableref"
)

// The frontend's user type directives, which the owner reads by their target
// scope; see [Annotations].
const (
	domainDirective    = "ptah:schema:domain"
	compositeDirective = "ptah:schema:composite"
)

// Annotations reads and writes Oracle user type declarations: a domain or
// composite declaration whose dialects name Oracle alone. A declaration that
// also names another target is refused by the frontend, since it declares two
// objects, and one that names no target belongs to the PostgreSQL owner. The
// writer scopes every declaration it writes to Oracle.
func Annotations() annotation.Extension {
	scope := func(directive string) annotation.TargetScope {
		return annotation.TargetScope{Directive: directive, Targets: []string{platform.Oracle}, Label: "Oracle"}
	}
	return annotation.Extension{Owner: oratype.Owner, Kinds: oratype.Kinds(),
		TargetScopes: []annotation.TargetScope{scope(domainDirective), scope(compositeDirective)},
		Decode:       decodeAnnotation, Coverage: sourceCoverage,
		Encoder: typeAnnotations{}, Retainer: typeAnnotations{}, Encodes: oratype.Kinds()}
}

// sourceCoverage is what a source that can declare both kinds knows: every
// type it holds, so one it leaves out is absent. A not-described record of the
// domain or composite coverage kind narrows it before the owner compares.
func sourceCoverage([]coverage.Object) (schemaext.Coverage, error) {
	return oratype.CompleteCoverage(schemaext.Desired)
}

func decodeAnnotation(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	object, err := annotationObject(declaration)
	if _, named := errors.AsType[*annotation.DeclarationError](err); err != nil && !named {
		err = &annotation.DeclarationError{Declaration: declaration, Err: err}
	}
	if err != nil {
		return nil, err
	}
	return []annotation.Contribution{{Object: &object, Source: declaration,
		Label: oratype.Label(object.Ref) + " " + oratype.QualifiedName(object.Ref)}}, nil
}

// annotationObject reads one declaration. The frontend validated its
// attributes and parsed its target scope before handing it over.
func annotationObject(declaration annotation.Declaration) (schemaext.Object, error) {
	attrs := declaration.Attributes
	schema, name := annotationNames(attrs["schema"], attrs["name"])
	var value schemaext.Value
	switch declaration.Directive {
	case domainDirective:
		notNull := false
		if raw, present := attrs["not_null"]; present {
			var err error
			if notNull, err = strconv.ParseBool(raw); err != nil {
				return schemaext.Object{}, &annotation.DeclarationError{Declaration: declaration, Attribute: "not_null", Err: err}
			}
		}
		value = &oratype.DesiredDomain{BaseType: strings.TrimSpace(attrs["type"]), NotNull: notNull, Default: attrs["default"],
			DefaultExpr: attrs["default_expr"], Check: strings.TrimSpace(attrs["check"]), Comment: strings.TrimSpace(attrs["comment"]),
			StructName: declaration.Struct}
	case compositeDirective:
		attributes, err := annotationAttributes(attrs["fields"])
		if err != nil {
			return schemaext.Object{}, &annotation.DeclarationError{Declaration: declaration, Attribute: "fields", Err: err}
		}
		value = &oratype.DesiredObjectType{Attributes: attributes, Comment: strings.TrimSpace(attrs["comment"]), StructName: declaration.Struct}
	default:
		return schemaext.Object{}, fmt.Errorf("%w: unexpected type directive %q", schemaext.ErrInvalidValue, declaration.Directive)
	}
	object, err := oratype.DesiredObject(oratype.Ref(value.Kind(), schema, name), value)
	object.Targets = slices.Clone(declaration.Targets)
	return object, err
}

// annotationNames reads the schema and name attributes, either of which may
// carry the schema and may quote a part.
func annotationNames(schema, name string) (schemaName, typeName string) {
	schema, name = annotation.QualifiedName(schema, name)
	if parsed, valid := tableref.Parse(schema); valid && !parsed.Qualified {
		schema = parsed.Name
	}
	return schema, name
}

// annotationAttributes reads a "name:type,name:type" list. A comma inside
// parentheses belongs to its type, so NUMBER(10,2) stays one type.
func annotationAttributes(text string) ([]oratype.Attribute, error) {
	var attributes []oratype.Attribute
	depth, start := 0, 0
	split := func(end int) error {
		part := strings.TrimSpace(text[start:end])
		if part == "" {
			return nil
		}
		name, typ, ok := strings.Cut(part, ":")
		name, typ = strings.TrimSpace(name), strings.TrimSpace(typ)
		if !ok || name == "" || typ == "" {
			return fmt.Errorf("invalid object type attribute %q; expected \"name:type,name:type\"", part)
		}
		attributes = append(attributes, oratype.Attribute{Name: name, Type: typ})
		return nil
	}
	for i := range len(text) {
		switch text[i] {
		case '(':
			depth++
		case ')':
			depth = max(depth-1, 0)
		case ',':
			if depth > 0 {
				continue
			}
			if err := split(i); err != nil {
				return nil, err
			}
			start = i + 1
		}
	}
	if err := split(len(text)); err != nil {
		return nil, err
	}
	if len(attributes) == 0 {
		return nil, fmt.Errorf("an object type requires at least one attribute")
	}
	return attributes, nil
}

// typeAnnotations writes and retains the owner's declarations.
type typeAnnotations struct{}

// EncodeAnnotations writes one directive for each type, scoped to Oracle.
func (typeAnnotations) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	if ctx == nil {
		return annotation.EncodeResult{}, fmt.Errorf("%w: Oracle user type export requires a context", schemaext.ErrInvalidValue)
	}
	var result annotation.EncodeResult
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		directive, err := encodeAnnotation(object)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Directives = append(result.Directives, directive)
	}
	return result, ctx.Err()
}

func encodeAnnotation(object schemaext.Object) (annotation.EncodedDirective, error) {
	if _, err := oratype.DesiredObject(object.Ref, object.Value); err != nil {
		return annotation.EncodedDirective{}, err
	}
	// A name is written as Oracle spells it, quoted only where it holds a
	// character that would split it: an unquoted name folds to upper case on
	// read and on the server alike, so DOM_EMAIL and dom_email name one type.
	attrs := []annotation.EncodedAttribute{{Name: "name", Value: tableref.Canonical("", object.Ref.Name.Source)}}
	if schema := oratype.AuthoredSchema(object.Ref); schema != "" {
		attrs = append(attrs, annotation.EncodedAttribute{Name: "schema", Value: tableref.Canonical("", schema)})
	}
	appendValue := func(name, value string) {
		if value != "" {
			attrs = append(attrs, annotation.EncodedAttribute{Name: name, Value: value})
		}
	}
	var directive, comment string
	switch value := object.Value.(type) {
	case *oratype.DesiredDomain:
		directive, comment = domainDirective, value.Comment
		appendValue("type", value.BaseType)
		if value.NotNull {
			appendValue("not_null", "true")
		}
		appendValue("default", value.Default)
		appendValue("default_expr", value.DefaultExpr)
		appendValue("check", value.Check)
	case *oratype.DesiredObjectType:
		directive, comment = compositeDirective, value.Comment
		fields := make([]string, 0, len(value.Attributes))
		for _, attribute := range value.Attributes {
			fields = append(fields, attribute.Name+":"+attribute.Type)
		}
		appendValue("fields", strings.Join(fields, ","))
	}
	appendValue("comment", comment)
	targets := slices.Clone(object.Targets)
	if len(targets) == 0 {
		// A declaration without a scope is the PostgreSQL owner's, so the
		// writer names the target that makes it this owner's.
		targets = []string{platform.Oracle}
	}
	return annotation.EncodedDirective{Directive: directive, Placement: annotation.AfterSchemas, Attributes: attrs, Targets: targets}, nil
}

// RetainAnnotations reports, for each declaration, whether the schema still
// holds the type it declares, unchanged and under the same scope.
func (typeAnnotations) RetainAnnotations(ctx context.Context, request annotation.RetentionRequest) ([]bool, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: Oracle user type retention requires a context", schemaext.ErrInvalidValue)
	}
	result := make([]bool, len(request.Declarations))
	for i, declaration := range request.Declarations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		expected, err := annotationObject(declaration)
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
		result[i] = withoutStruct(expected.Value).Equal(withoutStruct(actual.Value)) && slices.Equal(expected.Targets, actual.Targets)
	}
	return result, ctx.Err()
}

// withoutStruct is a declaration without the Go struct it was read from,
// which no export preserves.
func withoutStruct(value schemaext.Value) schemaext.Value {
	switch typed := value.(type) {
	case *oratype.DesiredDomain:
		copied := typed.Copy()
		copied.StructName = ""
		return copied
	case *oratype.DesiredObjectType:
		copied := typed.Copy()
		copied.StructName = ""
		return copied
	default:
		return value
	}
}
