package postgres

import (
	"ptah.run/core/ast"
	"ptah.run/migration/schemadiff/difftypes"
)

// releaseRemovedForeignKeys separates dependency release from recreation.
// Adding a key before recreating its foreign keys is insufficient: an existing
// foreign key must also be gone before that key can be removed or replaced.
func (p *Planner) releaseRemovedForeignKeys(nodes []ast.Node, diff *difftypes.SchemaDiff) ([]ast.Node, map[constraintHostKey]struct{}) {
	state := constraintPlanState{
		semantics:        diff.EffectiveIdentifierSemantics(p.targetDialect()),
		droppedForModify: make(map[constraintHostKey]struct{}),
	}
	for _, removal := range diff.ConstraintsRemoved {
		if removal.Type == "FOREIGN KEY" && removal.TableName != "" && removal.Name != "" {
			nodes = p.appendScopedDrop(nodes, removal.TableName, removal.Name, removal.Identity, state)
		}
	}
	return nodes, state.droppedForModify
}
