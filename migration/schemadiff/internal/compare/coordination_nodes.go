package compare

import (
	"sort"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbcoordination"
	"ptah.run/migration/schemadiff/difftypes"
)

// CoordinationNodes compares the YDB coordination nodes the desired state
// declares against the ones the database holds.
//
// A node is matched by its directory and name, which YDB compares case for
// case. A matched node is changed when the configuration it runs with differs:
// both sides are read through [ydbcoordination.Effective], because YDB stores
// a setting only when one was sent, so a declaration naming a setting at its
// default and a node that never had it set describe the same node.
//
// An addition is withheld, and a removal not planned, where the read or the
// declaration did not describe coordination nodes; see [Coverage].
func CoordinationNodes(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	declared := make(map[string]schemamodel.CoordinationNode, len(desired.CoordinationNodes))
	for _, node := range desired.CoordinationNodes {
		declared[node.QualifiedName()] = node
	}
	held := make(map[string]catalog.CoordinationNode, len(database.CoordinationNodes))
	for _, node := range database.CoordinationNodes {
		held[node.QualifiedName()] = node
	}

	var added []schemamodel.CoordinationNode
	for name, node := range declared {
		current, exists := held[name]
		if !exists {
			added = append(added, node)
			continue
		}
		changes := ydbcoordination.Changes(node.Spec, current.Spec)
		if changes.IsZero() {
			continue
		}
		diff.CoordinationNodesModified = append(diff.CoordinationNodesModified, difftypes.CoordinationNodeChange{
			Schema:   current.Schema,
			Name:     current.Name,
			Changes:  changes,
			Previous: current.Spec,
		})
	}
	kept, withheld := keepPlannedAdditions(cov, coverage.CoordinationNode, added,
		func(node schemamodel.CoordinationNode) (string, []string) {
			return node.Schema, []string{node.QualifiedName(), node.Name}
		},
		func(node schemamodel.CoordinationNode) string { return node.QualifiedName() },
		unguardedCreations(),
	)
	cov.recordUndecidedAdditions(withheld)
	diff.CoordinationNodesAdded = kept

	for name, node := range held {
		if _, ok := declared[name]; ok {
			continue
		}
		if !cov.PlansRemoval(coverage.CoordinationNode, node.Schema, name, node.Name) {
			continue
		}
		diff.CoordinationNodesRemoved = append(diff.CoordinationNodesRemoved, schemamodel.CoordinationNode{
			Schema: node.Schema,
			Name:   node.Name,
			Spec:   node.Spec,
		})
	}

	byName := func(nodes []schemamodel.CoordinationNode) {
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].QualifiedName() < nodes[j].QualifiedName() })
	}
	byName(diff.CoordinationNodesAdded)
	byName(diff.CoordinationNodesRemoved)
	sort.Slice(diff.CoordinationNodesModified, func(i, j int) bool {
		return diff.CoordinationNodesModified[i].QualifiedName() < diff.CoordinationNodesModified[j].QualifiedName()
	})
}
