package annotationmeta

import "go/ast"

// Placement describes the Go AST attachment of an annotation comment.
type Placement struct {
	Scope      Scope
	NamedField bool
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
			setCommentGroupPlacement(general.Doc, Placement{Scope: ScopeStruct}, placements)
			for _, field := range structType.Fields.List {
				setCommentGroupPlacement(field.Doc, Placement{
					Scope:      ScopeField,
					NamedField: len(field.Names) > 0,
				}, placements)
			}
		}
	}
	return placements
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
