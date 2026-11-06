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

// ColumnVerdicts returns, for each statement of a plan, what a column modification
// establishes that the statement's words cannot say, and a zero assessment for any
// other statement.
//
// A statement's words do not name the type it replaces, so ALTER COLUMN ... TYPE
// timestamp(0) reads the same over a timestamp(6) as over a date. They do not say what
// a restated column gives up either: MySQL's MODIFY COLUMN c bigint reads the same
// whether c was NOT NULL, or held a default, or neither. The node knows. An assessment
// of planned statements by their text folds these in with [Fold] and loses nothing
// [AssessRenderedWithCapabilities] reports as destructive.
//
// A verdict is a column modification the classifier rates Destructive, or a dropped
// default. A warning that only says a column changes stays out, since a statement reports
// that by being there, and so does every other operation an ALTER TABLE holds.
// A node that renders several statements gives its verdict only to one that changes the
// column's type or restates its definition, as [AssessRenderedWithCapabilities] does. A
// node holding an owner operation is left to [OwnerVerdicts].
//
// nodes are the planned nodes; statementNodes holds, position for position with
// statements, the index of the node that rendered each one, or -1 for a statement no
// node rendered. Provenance outside nodes, or lists of different lengths, are refused
// with [renderer.ErrInvalidResult].
func ColumnVerdicts(nodes []ast.Node, statementNodes []int, statements []string) ([]StatementAssessment, error) {
	if len(statementNodes) != len(statements) {
		return nil, fmt.Errorf("%w: %d statements have provenance for %d", renderer.ErrInvalidResult, len(statements), len(statementNodes))
	}
	rendered := make(map[int]int, len(nodes))
	for _, index := range statementNodes {
		if index < -1 || index >= len(nodes) {
			return nil, fmt.Errorf("%w: statement provenance %d is outside the %d planned nodes", renderer.ErrInvalidResult, index, len(nodes))
		}
		rendered[index]++
	}
	verdicts := make([]StatementAssessment, len(statements))
	for i, index := range statementNodes {
		if index == -1 {
			continue
		}
		verdict, established := columnVerdict(nodes[index])
		if established && (rendered[index] == 1 || isTypeChangeSQL(statements[i])) {
			verdicts[i] = verdict
		}
	}
	return verdicts, nil
}

// columnVerdict is what [ColumnVerdicts] takes from one node: the strongest of what its
// column modifications establish.
func columnVerdict(node ast.Node) (StatementAssessment, bool) {
	alter, altersTable := node.(*ast.AlterTableNode)
	if !altersTable || alter == nil || ast.PlacementOf(node) != ast.NoExtension {
		return StatementAssessment{}, false
	}
	var verdict StatementAssessment
	for _, operation := range alter.Operations {
		modify, modifies := operation.(*ast.ModifyColumnOperation)
		if !modifies {
			continue
		}
		severity, reason := classifyModifyColumn(modify)
		if severity == Destructive || reason == dropDefaultReason {
			raiseAssessment(&verdict, StatementAssessment{Severity: severity, Reason: reason})
		}
	}
	return verdict, verdict.Severity != ""
}
