package pgeffects

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/internal/deporder"
	"ptah.run/internal/tableref"
	"ptah.run/internal/typeorder"
)

// DefinedType returns the type a common creation defines. Enums have no
// user-type references and occupy their own phase.
func DefinedType(builder objectidentity.Builder, node *ast.CreateTypeNode) (schemaext.DefinedType, bool) {
	var kind objectidentity.Kind
	var refs []string
	switch def := node.TypeDef.(type) {
	case *ast.DomainTypeDef:
		kind, refs = objectidentity.KindDomain, []string{def.BaseType}
	case *ast.CompositeTypeDef:
		kind = objectidentity.KindComposite
		for _, field := range def.Fields {
			refs = append(refs, field.Type)
		}
	default:
		return schemaext.DefinedType{}, false
	}
	parsed, ok := tableref.Parse(node.Name)
	if !ok {
		return schemaext.DefinedType{}, false
	}
	id := builder.TablePartsVerbatim(parsed.Schema, parsed.Name)
	id.Kind = kind
	return schemaext.DefinedType{ID: id, References: refs}, true
}

// TypeSequence adds effects for common type changes whose identity is known
// from the supplied vocabulary. A DROP TYPE alone does not tell whether its
// subject is an enum or a composite, so an unknown subject stays unknown.
func TypeSequence(builder objectidentity.Builder, nodes []ast.Node, types []schemaext.DefinedType) [][]plangraph.Effect {
	result := Sequence(builder, nodes)
	byName := make(map[string]objectidentity.ID)
	for _, typ := range types {
		byName[deporder.NormalizeTypeReference(typeorder.Name(typ.ID))] = typ.ID
	}
	for i, node := range nodes {
		var name string
		var action plangraph.Action
		switch typed := node.(type) {
		case *ast.DropTypeNode:
			name, action = typed.Name, plangraph.Drop
		case *ast.AlterTypeNode:
			name, action = typed.Name, plangraph.Alter
		default:
			continue
		}
		if id, ok := byName[deporder.NormalizeTypeReference(name)]; ok {
			result[i] = slices.Clone(result[i])
			result[i] = append(result[i], plangraph.Effect{Subject: id, Action: action})
		}
	}
	return result
}
