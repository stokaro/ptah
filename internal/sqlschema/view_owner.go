package sqlschema

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/objectowner"
)

// applyAlterOwner declares the owner of the view or the materialized view an
// `ALTER [MATERIALIZED] VIEW name OWNER TO role` names: the parser makes the
// node only for a kind whose owner the dialect manages. A schema file is a
// script the server runs in order, so the statement names a view an earlier
// statement, or an earlier document, declares, and the last statement for a
// view is the owner it declares, as it is the owner the server leaves.
//
// The role is held as the server stores its name: an unquoted name folds as
// the dialect folds role names, and a quoted one keeps its spelling. Declaring
// an owner declares no role. A view the schema does not declare, a statement
// of the other kind of view, and a role [objectowner.ValidateRole] refuses,
// such as CURRENT_USER, are refused.
func applyAlterOwner(database *schemamodel.Database, document *Document, node *ast.AlterOwnerNode, sourcePlatform string) error {
	kind := objectowner.Kind(node.Kind)
	statement := fmt.Sprintf("ALTER %s %s OWNER TO %s", node.Kind, node.Name, node.Owner)
	owner := roleName(sourcePlatform, node.Owner)
	if err := objectowner.ValidateRole(owner); err != nil {
		return fmt.Errorf("%s: %w", statement, err)
	}
	databases := declaredIn(database, document)
	name := unqualified(normalizeSQLTableReference(sourcePlatform, node.Name), sourcePlatform)
	for _, declared := range databases {
		switch kind {
		case objectowner.View:
			for i := range declared.Views {
				if sameDeclaredName(declared.Views[i].Name, name, sourcePlatform) {
					declared.Views[i].Owner = owner
					return nil
				}
			}
		case objectowner.MaterializedView:
			for i := range declared.MaterializedViews {
				if sameDeclaredName(declared.MaterializedViews[i].Name, name, sourcePlatform) {
					declared.MaterializedViews[i].Owner = owner
					return nil
				}
			}
		}
	}
	declaredViews := declaredViews(databases, sourcePlatform)
	if index := resolveDeclaredName(sourcePlatform, name, declaredViews.names); index >= 0 {
		return fmt.Errorf("%w: %s: the schema declares %s as the other kind of view", ErrUnmodeledStatement, statement, node.Name)
	}
	return undeclared(fmt.Errorf("%w: %s names a %s this schema does not declare", ErrUnmodeledStatement, statement, kind.Label()))
}

// sameDeclaredName reports whether declared, a view's name as the schema
// holds it, is the view name names, unqualified as [unqualified] leaves it.
func sameDeclaredName(declared, name, sourcePlatform string) bool {
	return resolveDeclaredName(sourcePlatform, name, []string{unqualified(declared, sourcePlatform)}) == 0
}
