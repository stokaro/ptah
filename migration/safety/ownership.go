package safety

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlutil"
)

// An owner operation's verdict belongs to the statements that operation
// rendered, and to no other. The planners build every owner operation as a
// node of its own ([ast.IsolatedExtension]), and planning refuses a plan in
// which one shares a node, so the statements a node renders are exactly the
// statements its operation rendered. A node that still carries an owner
// operation beside other work ([ast.MixedExtension]) cannot say which of its
// statements the operation wrote; each of them is given the fail-closed
// verdict rather than a guess.

// mixedReason is the verdict a statement carries when the node that rendered
// it holds an owner operation beside other operations.
const mixedReason = "an owner operation shares its statement with other operations; manual review is required"

// unattributedReason is the verdict a plan carries when the statements its
// nodes rendered cannot be matched to it by position.
const unattributedReason = "the statements of an owner operation could not be identified in the plan; manual review is required"

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

// declaresAccess reports whether an owner operation in node makes an access
// claim, read from its type alone so a malformed payload still counts.
func declaresAccess(node ast.Node) bool {
	switch typed := node.(type) {
	case *ast.ExtensionStatement:
		return typed != nil && implementsAccess(typed.Payload)
	case *ast.ExtensionAlterOperation:
		return typed != nil && implementsAccess(typed.Payload)
	case *ast.StatementList:
		return typed != nil && slices.ContainsFunc(typed.Statements, declaresAccess)
	case *ast.AlterTableNode:
		if typed == nil {
			return false
		}
		for _, operation := range typed.Operations {
			if extension, ok := operation.(*ast.ExtensionAlterOperation); ok && declaresAccess(extension) {
				return true
			}
		}
	}
	return false
}

func implementsAccess(payload ast.ExtensionPayload) bool {
	_, ok := payload.(schemaext.AccessEffectSource)
	return ok
}

// renderedStatements splits one rendered fragment into its statements. A
// fragment the splitter cannot divide is one statement.
func renderedStatements(fragment, dialect string) []string {
	statements := sqlutil.SplitSQLStatementsForDialect(fragment, dialect)
	if len(statements) == 0 && strings.TrimSpace(fragment) != "" {
		statements = []string{strings.TrimSpace(fragment)}
	}
	return statements
}

// executable reports whether a statement holds anything besides comments. A
// comment-only piece of one fragment is joined to the next statement when the
// plan is split as a whole, so only executable statements are counted when the
// two splits are lined up.
func executable(statement, dialect string) bool {
	return strings.TrimSpace(sqlutil.StripCommentsForDialect(statement, dialect)) != ""
}

// OwnerVerdicts returns, for each statement of a plan rendered once, the
// verdict of the owner operation that rendered it, and a zero assessment for a
// statement no owner operation rendered.
//
// request and result are the plan's own rendering: the nodes it planned and the
// fragment each one rendered. statements is the plan as it will execute, split
// from the joined fragments. Attribution is by position: the executable
// statements of the fragments, in node order, are matched one for one with the
// executable statements of the plan. No statement text is compared and nothing
// is rendered again.
//
// A statement rendered by a node that is one owner operation gets that node's
// verdict, which includes the operation's lifecycle and access effects. A
// statement rendered by a node holding an owner operation beside other work is
// Destructive, because which of its statements the operation wrote is not
// known. When the counts do not line up, as when a fragment does not terminate
// its last statement, every statement of a plan carrying an owner operation is
// Destructive. Both fail-closed verdicts carry an unknown access effect when an
// owner operation involved makes an access claim. A plan without owner
// operations returns zero assessments.
func OwnerVerdicts(
	ctx context.Context,
	request renderer.Request,
	result renderer.Result,
	statements []string,
	dialect string,
) ([]StatementAssessment, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: owner verdicts require a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(result.Fragments) != len(request.Nodes) {
		return nil, fmt.Errorf("%w: %d fragments for %d planned nodes", renderer.ErrInvalidResult, len(result.Fragments), len(request.Nodes))
	}
	verdicts := make([]StatementAssessment, len(statements))
	owned := false
	nodeVerdicts := make([]StatementAssessment, len(request.Nodes))
	var sequence []int
	for i, node := range request.Nodes {
		switch ast.PlacementOf(node) {
		case ast.IsolatedExtension:
			owned = true
			nodeVerdicts[i] = assessNode(node)
		case ast.MixedExtension:
			owned = true
			nodeVerdicts[i] = failClosed(mixedReason, node)
		}
		for _, statement := range renderedStatements(result.Fragments[i], dialect) {
			if executable(statement, dialect) {
				sequence = append(sequence, i)
			}
		}
	}
	if !owned {
		return verdicts, nil
	}
	next := 0
	for i, statement := range statements {
		if !executable(statement, dialect) {
			continue
		}
		if next >= len(sequence) {
			next++
			break
		}
		verdicts[i] = nodeVerdicts[sequence[next]]
		next++
	}
	if next != len(sequence) {
		for i := range verdicts {
			verdicts[i] = failClosed(unattributedReason, request.Nodes...)
		}
	}
	return verdicts, nil
}
