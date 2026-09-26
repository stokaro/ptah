package postgres

import (
	"ptah.run/core/ast"
	"ptah.run/migration/schemadiff/difftypes"
)

// objectCommentStatements maps the kind a comparison reports to the kind
// COMMENT ON spells. A composite and a range are both a TYPE to the statement.
var objectCommentStatements = map[difftypes.CommentedObjectKind]ast.CommentedObject{
	difftypes.CommentedView:          ast.CommentedView,
	difftypes.CommentedSequence:      ast.CommentedSequence,
	difftypes.CommentedDomain:        ast.CommentedDomain,
	difftypes.CommentedCompositeType: ast.CommentedType,
	difftypes.CommentedRangeType:     ast.CommentedType,
	difftypes.CommentedExtension:     ast.CommentedExtension,
}

// changeObjectComments sets the comment of each existing object whose
// comment the comparison found changed.
//
// An object this plan also creates again does not need the statement, and
// writing it would say the same thing twice. Its create node already carries
// the comment the declaration asks for, and the renderer writes that one after
// the CREATE; a comment the declaration removed needs nothing when the
// object was dropped first, because the new object has none. What remains is
// the view replaced in place, which keeps the comment it had: a comment
// removed from it is removed here.
func (p *Planner) changeObjectComments(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if len(diff.ObjectCommentsChanged) == 0 {
		return result
	}
	written := createdComments(result)
	for _, change := range diff.ObjectCommentsChanged {
		object, known := objectCommentStatements[change.Kind]
		if !known {
			continue
		}
		if created, found := written[createdObject{object: object, name: change.Name}]; found &&
			(!created.replaced || (change.Desired != "" && created.comment == change.Desired)) {
			continue
		}
		result = append(result, ast.NewObjectComment(object, change.Name, change.Desired))
	}
	return result
}

// createdObject names an object a plan creates, by the kind COMMENT ON uses.
type createdObject struct {
	object ast.CommentedObject
	name   string
}

// createdComment is what a create node in the plan says about its object: the
// comment the renderer writes after it, and whether the statement replaces
// an object that keeps the comment it had.
type createdComment struct {
	comment  string
	replaced bool
}

// createdComments collects the views and user types the plan so far creates or
// replaces. Sequences and extensions are altered in place and never recreated,
// so a comment change on one always needs its own statement.
func createdComments(nodes []ast.Node) map[createdObject]createdComment {
	written := make(map[createdObject]createdComment)
	for _, node := range nodes {
		switch created := node.(type) {
		case *ast.CreateViewNode:
			written[createdObject{object: ast.CommentedView, name: created.Name}] = createdComment{
				comment: created.Comment, replaced: created.Replace,
			}
		case *ast.CreateTypeNode:
			object, commented := createdTypeObject(created.TypeDef)
			if commented {
				written[createdObject{object: object, name: created.Name}] = createdComment{comment: created.Comment}
			}
		}
	}
	return written
}

// createdTypeObject answers the COMMENT ON kind of a created user type, and
// false for an enum, whose comment the model does not keep.
func createdTypeObject(definition ast.TypeDefinition) (ast.CommentedObject, bool) {
	switch definition.(type) {
	case *ast.DomainTypeDef:
		return ast.CommentedDomain, true
	case *ast.CompositeTypeDef, *ast.RangeTypeDef:
		return ast.CommentedType, true
	default:
		return "", false
	}
}
