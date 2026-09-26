package postgres

import (
	"ptah.run/core/ast"
	"ptah.run/internal/routineargs"
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
	difftypes.CommentedEnumType:      ast.CommentedType,
	difftypes.CommentedFunction:      ast.CommentedFunction,
	difftypes.CommentedProcedure:     ast.CommentedProcedure,
	difftypes.CommentedMatView:       ast.CommentedMaterializedView,
	difftypes.CommentedTrigger:       ast.CommentedTrigger,
	difftypes.CommentedPolicy:        ast.CommentedPolicy,
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
// removed from it is removed here. An enum rebuilt to remove a value is a new
// type too, written with the declared comment by the rebuild.
func (p *Planner) changeObjectComments(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if len(diff.ObjectCommentsChanged) == 0 {
		return result
	}
	written := createdComments(result)
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, enumDiff := range diff.EnumsModified {
		if declared, rebuilt := enumRebuild(diff.DeclaredUserTypes, enumDiff, semantics); rebuilt {
			written[createdObject{object: ast.CommentedType, name: declared.QualifiedName()}] = createdComment{
				comment: declared.Comment,
			}
		}
	}
	for _, change := range diff.ObjectCommentsChanged {
		object, known := objectCommentStatements[change.Kind]
		if !known {
			continue
		}
		key := createdObject{object: object, name: change.Name, table: change.Table}
		if change.Arguments != nil {
			key.arguments = routineargs.InputTypes(*change.Arguments)
		}
		if created, found := written[key]; found &&
			(!created.replaced || (change.Desired != "" && created.comment == change.Desired)) {
			continue
		}
		node := ast.NewObjectComment(object, change.Name, change.Desired).SetTable(change.Table)
		node.Arguments = change.Arguments
		result = append(result, node)
	}
	return result
}

// createdObject names an object a plan creates, by the kind COMMENT ON uses.
// A trigger and a policy are named within their table, and a routine by its
// input argument types as well, because a name may carry several overloads.
type createdObject struct {
	object    ast.CommentedObject
	name      string
	table     string
	arguments string
}

// createdComment is what a create node in the plan says about its object: the
// comment the renderer writes after it, and whether the statement replaces
// an object that keeps the comment it had.
type createdComment struct {
	comment  string
	replaced bool
}

// createdComments collects the objects the plan so far creates or replaces.
// Sequences and extensions are altered in place and never recreated, so a
// comment change on one always needs its own statement.
//
// A routine is always written with CREATE OR REPLACE, and a trigger the plan
// replaces may keep the comment it had, so both count as replaced: at worst a
// comment is set that the object already carries. A materialized view, a
// policy and an enum are dropped before they are written again, so each is a
// new object with no comment of its own.
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
		case *ast.EnumNode:
			written[createdObject{object: ast.CommentedType, name: created.Name}] = createdComment{comment: created.Comment}
		case *ast.CreateFunctionNode:
			object := ast.CommentedFunction
			if created.IsProcedure() {
				object = ast.CommentedProcedure
			}
			key := createdObject{object: object, name: created.Name, arguments: routineargs.InputTypes(created.Parameters)}
			written[key] = createdComment{comment: created.Comment, replaced: true}
		case *ast.CreateMaterializedViewNode:
			written[createdObject{object: ast.CommentedMaterializedView, name: created.Name}] = createdComment{
				comment: created.Comment,
			}
		case *ast.CreateTriggerNode:
			written[createdObject{object: ast.CommentedTrigger, name: created.Name, table: created.Table}] = createdComment{
				comment: created.Comment, replaced: created.Replace,
			}
		case *ast.CreatePolicyNode:
			written[createdObject{object: ast.CommentedPolicy, name: created.Name, table: created.Table}] = createdComment{
				comment: created.Comment,
			}
		}
	}
	return written
}

// createdTypeObject answers the COMMENT ON kind of a created user type.
func createdTypeObject(definition ast.TypeDefinition) (ast.CommentedObject, bool) {
	switch definition.(type) {
	case *ast.DomainTypeDef:
		return ast.CommentedDomain, true
	case *ast.CompositeTypeDef, *ast.RangeTypeDef, *ast.EnumTypeDef:
		return ast.CommentedType, true
	default:
		return "", false
	}
}
