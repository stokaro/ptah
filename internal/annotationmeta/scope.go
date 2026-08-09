package annotationmeta

import "go/ast"

// Placement describes the Go AST attachment of an annotation comment.
type Placement struct {
	Scope            Scope
	StructName       string
	FieldNames       []string
	NamedField       bool
	EmbeddedTypeName string
}

// CommentPlacements classifies every comment in file using the attachment
// scopes shared by the Go parser and destructive annotation cleanup.
func CommentPlacements(file *ast.File) map[*ast.Comment]Placement {
	placements := make(map[*ast.Comment]Placement)
	for _, group := range file.Comments {
		setCommentGroupPlacement(group, Placement{Scope: ScopeFile}, placements)
	}
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, specification := range general.Specs {
			typeSpec, ok := specification.(*ast.TypeSpec)
			if !ok {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}
			structName := typeSpec.Name.Name
			setCommentGroupPlacement(general.Doc, Placement{
				Scope:      ScopeStruct,
				StructName: structName,
			}, placements)
			for _, field := range structType.Fields.List {
				setCommentGroupPlacement(field.Doc, Placement{
					Scope:            ScopeField,
					StructName:       structName,
					FieldNames:       fieldNames(field),
					NamedField:       len(field.Names) > 0,
					EmbeddedTypeName: embeddedFieldTypeName(field),
				}, placements)
			}
		}
	}
	return placements
}

func fieldNames(field *ast.Field) []string {
	if len(field.Names) == 0 {
		return nil
	}
	names := make([]string, len(field.Names))
	for i, name := range field.Names {
		names[i] = name.Name
	}
	return names
}

func embeddedFieldTypeName(field *ast.Field) string {
	if len(field.Names) > 0 {
		return ""
	}
	return embeddedTypeName(field.Type)
}

func embeddedTypeName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.StarExpr:
		return embeddedTypeName(typed.X)
	default:
		return ""
	}
}

func setCommentGroupPlacement(
	group *ast.CommentGroup,
	placement Placement,
	placements map[*ast.Comment]Placement,
) {
	if group == nil {
		return
	}
	for _, comment := range group.List {
		placements[comment] = placement
	}
}
