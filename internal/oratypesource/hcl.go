package oratypesource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/feature/oratype"
)

// HCL reads and writes the domain and composite blocks of an Oracle document,
// or of a block whose dialect attribute names Oracle. The PostgreSQL owner
// reads the same block types in the documents of its targets and of no
// target. A block is written without a dialect attribute where the document
// is written for Oracle.
func HCL() hclext.Extension {
	scope := []string{platform.Oracle}
	return hclext.Extension{Owner: oratype.Owner, Kinds: oratype.Kinds(), Encoder: hclCodec{}, Decoder: hclCodec{},
		Blocks: []hclext.Block{
			{Site: hclext.TopLevel, Type: "domain", Naming: hclext.SchemaAndName, DeclaresType: true, Dialects: scope,
				Attributes: []hclext.Attribute{
					{Name: "schema", Kind: hclext.Ref, Refers: "schema", Optional: true},
					{Name: "type", Kind: hclext.TypeName}, {Name: "null", Kind: hclext.Bool},
					{Name: "default", Kind: hclext.SQL}, {Name: "check", Kind: hclext.Text}, {Name: "comment", Kind: hclext.String},
				}},
			{Site: hclext.TopLevel, Type: "composite", Naming: hclext.SchemaAndName, DeclaresType: true, Dialects: scope,
				Attributes: []hclext.Attribute{{Name: "schema", Kind: hclext.Ref, Refers: "schema", Optional: true}, {Name: "comment", Kind: hclext.String}},
				Children:   []hclext.Child{{Type: "field", Attributes: []hclext.Attribute{{Name: "type", Kind: hclext.TypeName}}}}},
		},
		Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) { return sourceCoverage(nil) },
	}
}

// hclCodec reads and writes the types of one document.
type hclCodec struct{}

// DecodeHCL reads one declaration per claimed block.
func (hclCodec) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	if ctx == nil {
		return hclext.Result{}, fmt.Errorf("%w: Oracle user type decoding requires a context", schemaext.ErrInvalidValue)
	}
	result := hclext.Result{Declarations: make([]hclext.Declaration, 0, len(request.Claims))}
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return hclext.Result{}, err
		}
		object, err := hclObject(claim)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		result.Declarations = append(result.Declarations, hclext.Declaration{Objects: []schemaext.Object{object}})
	}
	return result, ctx.Err()
}

func hclObject(claim hclext.Claim) (schemaext.Object, error) {
	switch claim.Block {
	case "domain":
		attrs := claim.Attributes
		value := &oratype.DesiredDomain{BaseType: strings.TrimSpace(attrs["type"].Text), Check: strings.TrimSpace(attrs["check"].Text),
			Comment: strings.TrimSpace(attrs["comment"].Text)}
		if value.BaseType == "" {
			return schemaext.Object{}, fmt.Errorf("%w: domain %q requires type", schemaext.ErrInvalidValue, claim.Name)
		}
		if nullable, present := attrs["null"]; present {
			value.NotNull = !nullable.Bool
		}
		if def, present := attrs["default"]; present {
			if def.Kind == hclext.SQL {
				value.DefaultExpr = def.Text
			} else {
				value.Default = def.Text
			}
		}
		return oratype.DesiredObject(oratype.Ref(oratype.DomainKind, claim.Schema, claim.Name), value)
	case "composite":
		value := &oratype.DesiredObjectType{Comment: strings.TrimSpace(claim.Attributes["comment"].Text)}
		for _, child := range claim.Children {
			if child.Type != "field" {
				return schemaext.Object{}, fmt.Errorf("unsupported composite child %q", child.Type)
			}
			typ, written := child.Attributes["type"]
			if !written || strings.TrimSpace(typ.Text) == "" {
				return schemaext.Object{}, fmt.Errorf("%w: composite field %q requires type", schemaext.ErrInvalidValue, child.Name)
			}
			value.Attributes = append(value.Attributes, oratype.Attribute{Name: child.Name, Type: strings.TrimSpace(typ.Text)})
		}
		if len(value.Attributes) == 0 {
			return schemaext.Object{}, fmt.Errorf("%w: composite %q requires at least one field", schemaext.ErrInvalidValue, claim.Name)
		}
		return oratype.DesiredObject(oratype.Ref(oratype.ObjectTypeKind, claim.Schema, claim.Name), value)
	default:
		return schemaext.Object{}, fmt.Errorf("%w: unexpected Oracle type block %q", schemaext.ErrInvalidValue, claim.Block)
	}
}

// EncodeHCL writes one block for each type. A value that is not a valid
// declaration is refused rather than written.
func (hclCodec) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if ctx == nil {
		return hclext.EncodeResult{}, fmt.Errorf("%w: Oracle user type export requires a context", schemaext.ErrInvalidValue)
	}
	blocks := make([]hclext.EncodedBlock, 0, len(request.Objects))
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		block, err := typeBlock(object)
		if err != nil {
			return hclext.EncodeResult{}, err
		}
		blocks = append(blocks, block)
	}
	return hclext.EncodeResult{Blocks: blocks}, ctx.Err()
}

// typeBlock is the block of one type: its type and attributes by its model,
// its schema as the source wrote it, and its name.
func typeBlock(object schemaext.Object) (hclext.EncodedBlock, error) {
	if _, err := oratype.DesiredObject(object.Ref, object.Value); err != nil {
		return hclext.EncodedBlock{}, err
	}
	block := hclext.EncodedBlock{Placement: hclext.UserTypes, Schema: oratype.AuthoredSchema(object.Ref), Name: object.Ref.Name.Source,
		Object: object.Ref}
	switch value := object.Value.(type) {
	case *oratype.DesiredDomain:
		block.Type = "domain"
		block.Attributes = domainAttributes(value)
	case *oratype.DesiredObjectType:
		block.Type = "composite"
		block.Attributes = appendComment(nil, value.Comment)
		for _, attribute := range value.Attributes {
			block.Children = append(block.Children, hclext.EncodedChild{Type: "field", Name: attribute.Name,
				Attributes: []hclext.EncodedAttribute{sqlAttribute("type", attribute.Type)}})
		}
	}
	return block, nil
}

// domainAttributes are a domain's attributes in the order the writer writes
// them for every target: its type, `null = false` for NOT NULL, a default
// expression as SQL or else a literal default as a string, its CHECK, and its
// comment.
func domainAttributes(domain *oratype.DesiredDomain) []hclext.EncodedAttribute {
	attributes := []hclext.EncodedAttribute{sqlAttribute("type", strings.TrimSpace(domain.BaseType))}
	if domain.NotNull {
		attributes = append(attributes, hclext.EncodedAttribute{Name: "null", Value: hclext.Value{Kind: hclext.Bool}})
	}
	switch {
	case domain.DefaultExpr != "":
		attributes = append(attributes, sqlAttribute("default", domain.DefaultExpr))
	case domain.Default != "":
		attributes = append(attributes, hclext.EncodedAttribute{Name: "default", Value: hclext.Value{Kind: hclext.String, Text: domain.Default}})
	}
	if check := strings.TrimSpace(domain.Check); check != "" {
		attributes = append(attributes, sqlAttribute("check", check))
	}
	return appendComment(attributes, domain.Comment)
}

// appendComment appends a comment that is not blank, without the space around
// it.
func appendComment(attributes []hclext.EncodedAttribute, comment string) []hclext.EncodedAttribute {
	if comment = strings.TrimSpace(comment); comment == "" {
		return attributes
	}
	return append(attributes, hclext.EncodedAttribute{Name: "comment", Value: hclext.Value{Kind: hclext.String, Text: comment}})
}

func sqlAttribute(name, text string) hclext.EncodedAttribute {
	return hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.SQL, Text: text}}
}
