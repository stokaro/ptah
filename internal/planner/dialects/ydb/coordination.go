package ydb

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbcoordination"
	"ptah.run/migration/schemadiff/difftypes"
)

// refuseCoordinationNodes refuses, before anything is emitted, a coordination
// node change this target cannot make: any of them without
// [capability.CoordinationNodes], a node Ptah never writes, and a change that
// leaves a node with a configuration it would not run with as written.
func (p *Planner) refuseCoordinationNodes(diff *difftypes.SchemaDiff) error {
	if !p.caps.Has(capability.CoordinationNodes) {
		switch {
		case len(diff.CoordinationNodesAdded) > 0:
			return refuseKey(capability.CoordinationNodes,
				"creating coordination node "+diff.CoordinationNodesAdded[0].QualifiedName())
		case len(diff.CoordinationNodesModified) > 0:
			return refuseKey(capability.CoordinationNodes,
				"changing coordination node "+diff.CoordinationNodesModified[0].QualifiedName())
		case len(diff.CoordinationNodesRemoved) > 0:
			return refuseKey(capability.CoordinationNodes,
				"dropping coordination node "+diff.CoordinationNodesRemoved[0].QualifiedName())
		}
		return nil
	}
	for _, nodes := range [][]schemamodel.CoordinationNode{diff.CoordinationNodesAdded, diff.CoordinationNodesRemoved} {
		for _, node := range nodes {
			if err := ydbcoordination.RefuseName(node.Schema, node.Name); err != nil {
				return refuseFact("coordination node "+node.QualifiedName(), err.Error())
			}
		}
	}
	for _, node := range diff.CoordinationNodesAdded {
		if err := ydbcoordination.Validate(node.Spec); err != nil {
			return refuseFact("coordination node "+node.QualifiedName(), err.Error())
		}
	}
	for _, change := range diff.CoordinationNodesModified {
		subject := "coordination node " + change.QualifiedName()
		if err := ydbcoordination.RefuseName(change.Schema, change.Name); err != nil {
			return refuseFact(subject, err.Error())
		}
		if err := ydbcoordination.Validate(ydbcoordination.Merge(change.Previous, change.Changes)); err != nil {
			return refuseFact(subject, err.Error())
		}
	}
	return nil
}

// coordinationNodes creates each added node, changes each changed one and
// drops each removed one. A node depends on no table and no table on it, so
// where the statements stand among the others does not matter to YDB; the
// creations and changes come before the drops, as everywhere in a plan.
func coordinationNodes(diff *difftypes.SchemaDiff) (creations, drops []ast.Node) {
	for _, node := range diff.CoordinationNodesAdded {
		creations = append(creations, &ast.CreateCoordinationNodeNode{Name: node.QualifiedName(), Spec: node.Spec})
	}
	for _, change := range diff.CoordinationNodesModified {
		creations = append(creations, &ast.AlterCoordinationNodeNode{Name: change.QualifiedName(), Spec: change.Changes})
	}
	for _, node := range diff.CoordinationNodesRemoved {
		drops = append(drops, &ast.DropCoordinationNodeNode{Name: node.QualifiedName()})
	}
	return creations, drops
}
