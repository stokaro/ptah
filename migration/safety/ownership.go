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

// An owned operation's verdict belongs to the statements the operation
// rendered, and to no other. A node can carry an owned operation beside common
// ones: an ALTER TABLE adding a column and changing an owner's setting, or a
// statement list creating a table and then a policy on it. A dialect may render
// such a node as one statement, which then executes the owned operation, or as
// one statement per operation, where the ADD COLUMN executes nothing owned.
//
// The statements an owned operation rendered are found by rendering the node a
// second time without its owned operations, in the same batch: what the full
// rendering has and the common-only rendering lacks is the owned operation's.
// A node that is owned and nothing else renders owned statements only.

// ownedPart classifies a node for attribution. pure reports a node every
// statement of which an owned operation renders; common is the node without its
// owned operations when it keeps a common part. A node carrying no owned
// operation is neither.
func ownedPart(node ast.Node) (common ast.Node, pure bool) {
	if !hasExtensionEffect(node) {
		return nil, false
	}
	switch typed := node.(type) {
	case *ast.StatementList:
		kept := make([]ast.Node, 0, len(typed.Statements))
		for _, child := range typed.Statements {
			if !hasExtensionEffect(child) {
				kept = append(kept, child)
				continue
			}
			if part, childPure := ownedPart(child); !childPure {
				kept = append(kept, part)
			}
		}
		if len(kept) == 0 {
			return nil, true
		}
		return &ast.StatementList{Statements: kept}, false
	case *ast.AlterTableNode:
		operations := make([]ast.AlterOperation, 0, len(typed.Operations))
		for _, operation := range typed.Operations {
			if _, owned := operation.(*ast.ExtensionAlterOperation); !owned {
				operations = append(operations, operation)
			}
		}
		if len(operations) == 0 {
			return nil, true
		}
		common := *typed
		common.Operations = operations
		return &common, false
	default:
		return nil, true
	}
}

// commonParts appends the common part of every node that has both an owned and
// a common part to batch, and returns where each landed: -1 for a node with no
// common part to render.
func commonParts(nodes, batch []ast.Node) ([]ast.Node, []int) {
	positions := make([]int, len(nodes))
	for i, node := range nodes {
		positions[i] = -1
		if common, pure := ownedPart(node); !pure && common != nil {
			positions[i] = len(batch)
			batch = append(batch, common)
		}
	}
	return batch, positions
}

// ownedStatements reports, for each statement a node rendered, whether an owned
// operation rendered it. commonStatements is what the node's common part
// rendered alone; it is read only for a node with both parts.
func ownedStatements(node ast.Node, statements, commonStatements []string, dialect string) []bool {
	owned := make([]bool, len(statements))
	common, pure := ownedPart(node)
	switch {
	case pure:
		for i := range owned {
			owned[i] = true
		}
	case common != nil:
		remaining := make(map[string]int, len(commonStatements))
		for _, statement := range commonStatements {
			remaining[statementKey(statement, dialect)]++
		}
		for i, statement := range statements {
			key := statementKey(statement, dialect)
			if remaining[key] > 0 {
				remaining[key]--
				continue
			}
			owned[i] = true
		}
	}
	return owned
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

// statementKey identifies a statement independently of its comments and
// white space, the way a saved plan matches statements across an edit.
func statementKey(statement, dialect string) string {
	body := sqlutil.StripCommentsForDialect(statement, dialect)
	return strings.Join(strings.Fields(strings.TrimSuffix(strings.TrimSpace(body), ";")), " ")
}

// declaresAccess reports whether an owned operation in node makes an access
// claim, read from its type alone so a malformed payload still counts.
func declaresAccess(node ast.Node) bool {
	switch typed := node.(type) {
	case *ast.ExtensionStatement:
		return typed != nil && implementsAccess(typed.Payload)
	case *ast.ExtensionAlterOperation:
		return typed != nil && implementsAccess(typed.Payload)
	case *ast.StatementList:
		return slices.ContainsFunc(typed.Statements, declaresAccess)
	case *ast.AlterTableNode:
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

// unattributedReason is the verdict a plan carries when the statements its
// owned operations rendered cannot be identified in it.
const unattributedReason = "the statements of an owned operation could not be identified in the plan; manual review is required"

// OwnerVerdicts returns, for each statement of a plan rendered once, the
// verdict of the owned operation that rendered it, and a zero assessment for a
// statement no owned operation rendered.
//
// request and result are the plan's own rendering: the nodes it planned and the
// fragment each one rendered. statements is the plan as it will execute, split
// from the joined fragments. The owner verdict of a statement is its node's
// assessment, which includes every owned operation's lifecycle and access
// verdicts; a common statement beside an owned one in the same node gets none.
// Attribution is positional, by node, and never by matching text across two
// renderings of the plan.
//
// It fails closed. When the statements split from each fragment do not line up
// with statements, which happens when a fragment does not terminate its last
// statement, and some node carries an owned operation, every statement gets a
// Destructive verdict saying so, with an unknown access effect when an owned
// operation makes an access claim. A plan without owned operations returns zero
// assessments and calls no service; a plan with an owned operation beside a
// common one renders the common parts once more, in one batch.
func OwnerVerdicts(
	ctx context.Context,
	service renderer.Service,
	request renderer.Request,
	result renderer.Result,
	statements []string,
	dialect string,
) ([]StatementAssessment, error) {
	if err := schemaext.RequireRuntime(ctx, service); err != nil {
		return nil, err
	}
	if len(result.Fragments) != len(request.Nodes) {
		return nil, fmt.Errorf("%w: %d fragments for %d planned nodes", renderer.ErrInvalidResult, len(result.Fragments), len(request.Nodes))
	}
	verdicts := make([]StatementAssessment, len(statements))
	owners, access := 0, false
	for _, node := range request.Nodes {
		if hasExtensionEffect(node) {
			owners++
			access = access || declaresAccess(node)
		}
	}
	if owners == 0 {
		return verdicts, nil
	}
	sequence, err := attributeStatements(ctx, service, request, result, dialect)
	if err != nil {
		return nil, err
	}
	if !assignVerdicts(verdicts, sequence, request.Nodes, statements, dialect) {
		failed := StatementAssessment{Severity: Destructive, Reason: unattributedReason}
		if access {
			failed.Access, failed.AccessReason = schemaext.AccessUnknown, unattributedReason
		}
		for i := range verdicts {
			verdicts[i] = failed
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return verdicts, nil
}

// attributed is one statement a planned node rendered, in plan order: its
// comparison key, whether an owned operation rendered it, and the node.
type attributed struct {
	key   string
	owned bool
	node  int
}

// attributeStatements lists the statements every planned node rendered, in
// order, with their ownership. Statements with no executable body are left
// out, as the plan's own statement list leaves them out.
func attributeStatements(
	ctx context.Context,
	service renderer.Service,
	request renderer.Request,
	result renderer.Result,
	dialect string,
) ([]attributed, error) {
	batch, positions := commonParts(request.Nodes, nil)
	var common renderer.Result
	if len(batch) > 0 {
		commonRequest := request
		commonRequest.Nodes = batch
		rendered, err := renderer.Render(ctx, service, commonRequest)
		if err != nil {
			return nil, err
		}
		common = rendered
	}
	var sequence []attributed
	for i, node := range request.Nodes {
		rendered := renderedStatements(result.Fragments[i], dialect)
		var commonStatements []string
		if positions[i] >= 0 {
			commonStatements = renderedStatements(common.Fragments[positions[i]], dialect)
		}
		owned := ownedStatements(node, rendered, commonStatements, dialect)
		for j, statement := range rendered {
			if key := statementKey(statement, dialect); key != "" {
				sequence = append(sequence, attributed{key: key, owned: owned[j], node: i})
			}
		}
	}
	return sequence, nil
}

// assignVerdicts walks the plan's statements beside the attributed sequence
// and gives each owned statement its node's verdict. It reports false when the
// two do not line up, and then the verdicts it wrote mean nothing.
func assignVerdicts(verdicts []StatementAssessment, sequence []attributed, nodes []ast.Node, statements []string, dialect string) bool {
	next := 0
	for i, statement := range statements {
		key := statementKey(statement, dialect)
		if key == "" {
			continue
		}
		if next >= len(sequence) || sequence[next].key != key {
			return false
		}
		if sequence[next].owned {
			verdicts[i] = assessNode(nodes[sequence[next].node])
		}
		next++
	}
	return next == len(sequence)
}
