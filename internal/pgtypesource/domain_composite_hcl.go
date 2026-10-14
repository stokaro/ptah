package pgtypesource

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// domainBlock is the domain shape for documents of targets. It also reads a
// document that names no target, and the writer writes no dialect selector
// for it.
func domainBlock(targets []string) hclext.Block {
	return hclext.Block{Site: hclext.TopLevel, Type: "domain", Naming: hclext.SchemaAndName, DeclaresType: true,
		Dialects: slices.Clone(targets), Unscoped: true,
		Attributes: []hclext.Attribute{
			{Name: "schema", Kind: hclext.Ref, Refers: "schema", Optional: true},
			{Name: "type", Kind: hclext.TypeName}, {Name: "null", Kind: hclext.Bool},
			{Name: "default", Kind: hclext.SQL}, {Name: "check", Kind: hclext.Text}, {Name: "comment", Kind: hclext.String},
		}}
}

// compositeBlockShape is the composite type shape, scoped as [domainBlock].
func compositeBlockShape(targets []string) hclext.Block {
	return hclext.Block{Site: hclext.TopLevel, Type: "composite", Naming: hclext.SchemaAndName, DeclaresType: true,
		Dialects: slices.Clone(targets), Unscoped: true,
		Attributes: []hclext.Attribute{{Name: "schema", Kind: hclext.Ref, Refers: "schema", Optional: true}, {Name: "comment", Kind: hclext.String}},
		Children:   []hclext.Child{{Type: "field", Attributes: []hclext.Attribute{{Name: "type", Kind: hclext.TypeName}}}}}
}

func typeHCLObject(claim hclext.Claim) (schemaext.Object, error) {
	switch claim.Block {
	case "range":
		return rangeHCLObject(claim)
	case "domain":
		attrs := claim.Attributes
		value := &pgtype.DesiredDomain{BaseType: strings.TrimSpace(attrs["type"].Text), Check: strings.TrimSpace(attrs["check"].Text), Comment: strings.TrimSpace(attrs["comment"].Text)}
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
		return pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, claim.Schema, claim.Name), value)
	case "composite":
		value := &pgtype.DesiredComposite{Comment: strings.TrimSpace(claim.Attributes["comment"].Text)}
		for _, child := range claim.Children {
			if child.Type != "field" {
				return schemaext.Object{}, fmt.Errorf("unsupported composite child %q", child.Type)
			}
			typ, written := child.Attributes["type"]
			if !written || strings.TrimSpace(typ.Text) == "" {
				return schemaext.Object{}, fmt.Errorf("%w: composite field %q requires type", schemaext.ErrInvalidValue, child.Name)
			}
			value.Attributes = append(value.Attributes, pgtype.Attribute{Name: child.Name, Type: strings.TrimSpace(typ.Text)})
		}
		if len(value.Attributes) == 0 {
			return schemaext.Object{}, fmt.Errorf("%w: composite %q requires at least one field", schemaext.ErrInvalidValue, claim.Name)
		}
		return pgtype.DesiredObject(pgtype.Ref(pgtype.CompositeKind, claim.Schema, claim.Name), value)
	default:
		return schemaext.Object{}, fmt.Errorf("%w: unexpected type block %q", schemaext.ErrInvalidValue, claim.Block)
	}
}
