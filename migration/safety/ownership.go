package safety

import (
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
)

// An owner operation's verdict belongs to the statements that operation
// rendered, and to no other. The planners build every owner operation as a
// node of its own ([ast.IsolatedExtension]), and planning refuses a plan in
// which one shares a node, so the statements a node renders are exactly the
// statements its operation rendered. A node that still carries an owner
// operation beside other work ([ast.MixedExtension]) cannot say which of its
// statements the operation wrote; each of them takes the node's verdict raised
// to the fail-closed one rather than a guess.

// mixedReason is the verdict a statement carries when the node that rendered
// it holds an owner operation beside other operations.
const mixedReason = "an owner operation shares its statement with other operations; manual review is required"

// failClosed is the verdict for a statement no owner verdict can be
// attributed to. It carries an unknown access effect when an owner operation
// among involved makes an access claim, and none otherwise.
func failClosed(reason string, involved ...ast.Node) StatementAssessment {
	verdict := StatementAssessment{Severity: Destructive, Reason: reason}
	if slices.ContainsFunc(involved, declaresAccess) {
		verdict.Access, verdict.AccessReason = schemaext.AccessUnknown, reason
	}
	return verdict
}

// mixedVerdict is the verdict of a node holding an owner operation beside
// other work: the node's own verdict raised to the fail-closed one. Raising
// keeps what the node established, so a widening stays a widening rather than
// becoming unknown.
func mixedVerdict(node ast.Node) StatementAssessment {
	verdict := assessNode(node)
	raiseAssessment(&verdict, failClosed(mixedReason, node))
	return verdict
}

// declaresAccess reports whether an owner operation in node makes an access
// claim, read from its type alone so a malformed payload still counts.
func declaresAccess(node ast.Node) bool {
	return slices.ContainsFunc(ast.OwnerOperations(node), func(payload ast.ExtensionPayload) bool {
		_, ok := payload.(schemaext.AccessEffectSource)
		return ok
	})
}

// OwnerVerdicts returns, for each statement of a plan, the verdict of the
// owner operation that rendered it, and a zero assessment for a statement no
// owner operation rendered.
//
// nodes are the planned nodes; statementNodes holds, position for position
// with the plan's statements, the index of the node whose fragment rendered
// each one, or -1 for a statement no node rendered (see
// [ptah.run/migration/planner.RenderedPlan.PlannedStatements]). Attribution is
// by that provenance alone: no statement text is read or compared.
//
// A statement rendered by a node that is one owner operation gets that node's
// verdict, which includes the operation's lifecycle and access effects. A
// statement rendered by a node holding an owner operation beside other work
// gets the node's verdict raised to Destructive, with an unknown access effect
// when the operation makes an access claim and none was established. An index
// outside nodes is refused with [renderer.ErrInvalidResult].
func OwnerVerdicts(nodes []ast.Node, statementNodes []int) ([]StatementAssessment, error) {
	for _, index := range statementNodes {
		if index < -1 || index >= len(nodes) {
			return nil, fmt.Errorf("%w: statement provenance %d is outside the %d planned nodes", renderer.ErrInvalidResult, index, len(nodes))
		}
	}
	verdicts := make([]StatementAssessment, len(statementNodes))
	nodeVerdicts := make(map[int]StatementAssessment)
	for i, node := range nodes {
		switch ast.PlacementOf(node) {
		case ast.IsolatedExtension:
			nodeVerdicts[i] = assessNode(node)
		case ast.MixedExtension:
			nodeVerdicts[i] = mixedVerdict(node)
		}
	}
	if len(nodeVerdicts) == 0 {
		return verdicts, nil
	}
	for i, index := range statementNodes {
		if verdict, owned := nodeVerdicts[index]; owned {
			verdicts[i] = verdict
		}
	}
	return verdicts, nil
}
