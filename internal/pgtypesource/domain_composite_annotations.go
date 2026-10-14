package pgtypesource

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/lexer"
	"ptah.run/internal/tableref"
)

// The frontend's user type directives, which the owner reads by their target
// scope; see [Annotations].
const (
	domainDirective    = "ptah:schema:domain"
	compositeDirective = "ptah:schema:composite"
)

// rangeDirectives is the owner's own directive.
func rangeDirectives() []annotation.Directive {
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
	return []annotation.Directive{{Name: rangeDirective, Description: "Declares a PostgreSQL range type.",
		Scopes: []annotation.Scope{annotation.ScopeStruct}, Attributes: attrs}}
}

// typeAnnotationObject reads one declaration the owner decodes. The frontend
// has parsed the target scope of a domain or composite type declaration it
// hands over.
func typeAnnotationObject(declaration annotation.Declaration) (schemaext.Object, error) {
	if declaration.Directive == rangeDirective {
		return rangeAnnotationObject(declaration)
	}
	attrs := declaration.Attributes
	schema, name := rangeAnnotationNames(attrs["schema"], attrs["name"])
	var value schemaext.Value
	switch declaration.Directive {
	case domainDirective:
		nullable := false
		if raw, present := attrs["not_null"]; present {
			var err error
			nullable, err = strconv.ParseBool(raw)
			if err != nil {
				return schemaext.Object{}, &annotation.DeclarationError{Declaration: declaration, Attribute: "not_null", Err: err}
			}
		}
		value = &pgtype.DesiredDomain{BaseType: strings.TrimSpace(attrs["type"]), NotNull: nullable, Default: attrs["default"],
			DefaultExpr: strings.TrimSpace(attrs["default_expr"]), Check: strings.TrimSpace(attrs["check"]), Comment: strings.TrimSpace(attrs["comment"]), StructName: declaration.Struct}
	case compositeDirective:
		fields, err := annotationFields(attrs["fields"])
		if err != nil {
			return schemaext.Object{}, &annotation.DeclarationError{Declaration: declaration, Attribute: "fields", Err: err}
		}
		value = &pgtype.DesiredComposite{Attributes: fields, Comment: strings.TrimSpace(attrs["comment"]), StructName: declaration.Struct}
	default:
		return schemaext.Object{}, fmt.Errorf("%w: unexpected type directive %q", schemaext.ErrInvalidValue, declaration.Directive)
	}
	object, err := pgtype.DesiredObject(pgtype.Ref(value.Kind(), schema, name), value)
	object.Targets = slices.Clone(declaration.Targets)
	return object, err
}

// annotationFields reads SQL-quoted names and balanced type modifiers so a
// comma in either stays inside its attribute.
func annotationFields(text string) ([]pgtype.Attribute, error) {
	tokens := newRangeTokens("", sqlext.Claim{Text: text})
	var result []pgtype.Attribute
	for tokens.current().Type != lexer.TokenEOF {
		name, err := tokens.name("composite attribute")
		if err != nil {
			return nil, err
		}
		if err := tokens.punctuation(":"); err != nil {
			return nil, err
		}
		typ, err := tokens.optionValue(name)
		if err != nil {
			return nil, err
		}
		result = append(result, pgtype.Attribute{Name: name, Type: typ})
		if !tokens.operator(",") {
			break
		}
		tokens.pos++
		if tokens.current().Type == lexer.TokenEOF {
			return nil, fmt.Errorf("expected a composite attribute after comma")
		}
	}
	if err := tokens.end(); err != nil {
		return nil, err
	}
	return result, nil
}

func encodeTypeAnnotation(object schemaext.Object) (annotation.EncodedDirective, error) {
	if _, err := pgtype.DesiredObject(object.Ref, object.Value); err != nil {
		return annotation.EncodedDirective{}, err
	}
	attrs := []annotation.EncodedAttribute{{Name: "name", Value: tableref.CanonicalExact("", object.Ref.Name.Source)}}
	if schema := pgtype.AuthoredSchema(object.Ref); schema != "" {
		attrs = append(attrs, annotation.EncodedAttribute{Name: "schema", Value: tableref.CanonicalExact("", schema)})
	}
	appendValue := func(name, value string) {
		if value != "" {
			attrs = append(attrs, annotation.EncodedAttribute{Name: name, Value: value})
		}
	}
	var directive, comment string
	switch value := object.Value.(type) {
	case *pgtype.DesiredDomain:
		directive, comment = domainDirective, value.Comment
		appendValue("type", value.BaseType)
		if value.NotNull {
			appendValue("not_null", "true")
		}
		appendValue("default", value.Default)
		appendValue("default_expr", value.DefaultExpr)
		appendValue("check", value.Check)
	case *pgtype.DesiredComposite:
		directive, comment = compositeDirective, value.Comment
		fields := make([]string, 0, len(value.Attributes))
		for _, field := range value.Attributes {
			fields = append(fields, tableref.CanonicalExact("", field.Name)+":"+field.Type)
		}
		appendValue("fields", strings.Join(fields, ","))
	case *pgtype.DesiredRange:
		directive, comment = rangeDirective, value.Comment
		appendValue("subtype", value.Subtype)
		for _, name := range pgtype.RangeAttributes() {
			if text := value.Attribute(name); text != "" || value.Clears(name) {
				attrs = append(attrs, annotation.EncodedAttribute{Name: name, Value: text})
			}
		}
	}
	appendValue("comment", comment)
	return annotation.EncodedDirective{Directive: directive, Placement: annotation.AfterSchemas, Attributes: attrs, Targets: slices.Clone(object.Targets)}, nil
}

func annotationTypeValue(value schemaext.Value) (schemaext.Value, error) {
	switch typed := value.(type) {
	case *pgtype.DesiredDomain:
		result := typed.Copy()
		result.StructName = ""
		result.Normalized = nil
		return result, nil
	case *pgtype.DesiredComposite:
		result := typed.Copy()
		result.StructName = ""
		return result, nil
	case *pgtype.DesiredRange:
		result := typed.Copy()
		result.StructName = ""
		return result, nil
	default:
		return nil, fmt.Errorf("%w: type annotation requires a declaration", schemaext.ErrInvalidValue)
	}
}
