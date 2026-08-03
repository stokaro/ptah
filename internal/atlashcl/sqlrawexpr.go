package atlashcl

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// sqlFuncName is Atlas HCL's raw-expression escape hatch. `sql("X")` marks X as
// SQL text that is handed to the engine unchanged.
const sqlFuncName = "sql"

// Ptah reduces `sql("X")` to X in every position it reads a value from, instead
// of refusing the call the way the pinned Atlas community binary v1.3.0 does in
// the positions Atlas declares as plain strings. Issue #1106 offered both
// answers; this is the reasoning for the one taken, and both sides were
// measured against that binary before choosing.
//
// Refusing cannot be applied uniformly. The community binary ACCEPTS sql() in
// several of the same positions Ptah leaked it from -- `type = sql("varchar(10)")`
// plans as `varchar`, and files whose view.as, materialized.as or composite
// field type carry a sql() call are planned without complaint. A blanket
// refusal would make ptah-compat exit 1 on files that binary plans fine, which
// is a drop-in break in the direction that costs a user their working schema.
// Refusing only where that binary refuses means carrying a hand-copied table of
// Atlas's per-attribute hclspec types, re-measured attribute by attribute, and
// a wrong row in that table breaks drop-in silently.
//
// Reducing cannot smuggle anything past the renderer. `sql("X")` reduces to the
// string X, which is exactly the value the literal spelling `= "X"` already
// produces in the same position -- a spelling Ptah and that binary both accept
// today. After the reduction the two spellings are indistinguishable everywhere
// downstream, so the renderer is handed nothing it could not already be handed.
//
// What the choice costs: ptah-compat still exits 0 for check.expr and
// index.where where that binary exits 1. That is the same richer-surface
// direction this project already takes for the sequence, function, domain,
// policy and trigger blocks. The defect issue #1106 named is the DDL -- a plan
// that reports success and cannot run -- and the DDL is valid again.
// rejectMalformedSQLRawExprs is what keeps the remaining looseness honest: no
// spelling of sql() reaches the renderer unreduced, valid or not.

// sqlRawExprValue reduces a sql() call to the SQL text it carries.
//
// ok is false when expr is not a sql() call at all, and also for a call that
// cannot be reduced -- but no parse path can observe the second case, because
// rejectMalformedSQLRawExprs refuses the whole file first.
func (p *parser) sqlRawExprValue(expr hclsyntax.Expression) (string, bool) {
	call, ok := expr.(*hclsyntax.FunctionCallExpr)
	if !ok || call.Name != sqlFuncName {
		return "", false
	}
	value, err := p.sqlCallText(call)
	if err != nil {
		return "", false
	}
	return value, true
}

// sqlCallText returns the SQL text a sql() call carries, or the reason the call
// cannot be reduced to text.
func (p *parser) sqlCallText(call *hclsyntax.FunctionCallExpr) (string, error) {
	if call.ExpandFinal {
		return "", fmt.Errorf("sql() does not take an expanded argument list")
	}
	if len(call.Args) != 1 {
		return "", fmt.Errorf("sql() takes exactly one string argument, got %d", len(call.Args))
	}

	arg := call.Args[0]
	value, diags := arg.Value(nil)
	if !diags.HasErrors() && value.IsKnown() && !value.IsNull() && value.Type() == cty.String {
		return value.AsString(), nil
	}

	// A quoted string Ptah cannot evaluate is a string carrying an
	// interpolation, and Ptah has no schema-file variable evaluation yet
	// (issue #926). Handing back the template source keeps that gap exactly
	// where it already is -- `default = "${var.x}"` renders its own source text
	// today -- instead of turning #926 into a refusal in this one spot.
	//
	// The distinction is not cosmetic: measured against the pinned community
	// binary, `default = sql("${var.floor} + 1")` is planned successfully, so
	// refusing it here would be a NEW stricter break on a file that binary
	// supports. The DDL Ptah renders for it stays wrong until #926 lands, but
	// it no longer carries the literal token sql( into the plan.
	if _, ok := arg.(*hclsyntax.TemplateExpr); ok {
		return unquotedSource(p.rawExprNode(arg)), nil
	}

	return "", fmt.Errorf("sql() takes a string argument")
}

// unquotedSource strips the quoting from an expression's source text. A heredoc
// is not quoted that way and is returned unchanged.
func unquotedSource(raw string) string {
	if unquoted, err := strconv.Unquote(raw); err == nil {
		return unquoted
	}
	return raw
}

// sqlRawExprRefusal is one malformed sql() call and why it was refused.
type sqlRawExprRefusal struct {
	rng      hcl.Range
	attrName string
	reason   string
}

// rejectMalformedSQLRawExprs refuses every sql() call in the file that Ptah
// cannot reduce to SQL text, before the body walk can put one in front of the
// renderer.
//
// This is the half of the #1106 decision that keeps reducing sql() honest.
// Every value helper in this package falls back to the attribute's SOURCE TEXT
// when an expression will not evaluate, which is how `CHECK (sql("n > 0"))`
// reached a plan in the first place. The same fallback is worse for a call that
// is only part of an expression: `default = sql("1") + sql("2")` planned as
// `DEFAULT "1") + sql("2"` before this guard existed.
//
// Two shapes are refused:
//
//   - a sql() call Ptah cannot reduce: wrong arity, an expanded argument list,
//     or an argument that is not a string
//   - a sql() call that is not the whole attribute value
//
// The pinned Atlas community binary v1.3.0 refuses every one of these too, so
// the guard opens no position where ptah-compat is stricter than that binary.
func (p *parser) rejectMalformedSQLRawExprs(body *hclsyntax.Body) error {
	var refusals []sqlRawExprRefusal
	p.collectSQLRawExprRefusals(body, &refusals)
	if len(refusals) == 0 {
		return nil
	}
	// Attributes hang off a map, so report the one that comes first in the
	// file rather than whichever the map iteration reached first.
	first := slices.MinFunc(refusals, func(a, b sqlRawExprRefusal) int {
		return a.rng.Start.Byte - b.rng.Start.Byte
	})
	return fmt.Errorf(
		"parse HCL schema at %s: attribute %q: %s",
		first.rng.String(), first.attrName, first.reason,
	)
}

func (p *parser) collectSQLRawExprRefusals(body *hclsyntax.Body, out *[]sqlRawExprRefusal) {
	for _, attr := range body.Attributes {
		p.collectAttrSQLRawExprRefusals(attr, out)
	}
	for _, block := range body.Blocks {
		p.collectSQLRawExprRefusals(block.Body, out)
	}
}

func (p *parser) collectAttrSQLRawExprRefusals(attr *hclsyntax.Attribute, out *[]sqlRawExprRefusal) {
	if call, ok := attr.Expr.(*hclsyntax.FunctionCallExpr); ok && call.Name == sqlFuncName {
		if _, err := p.sqlCallText(call); err != nil {
			*out = append(*out, sqlRawExprRefusal{rng: call.Range(), attrName: attr.Name, reason: err.Error()})
		}
		// A nested sql() inside a call Ptah already refused is the same
		// mistake reported twice; the outer refusal names the position.
		return
	}
	for _, call := range nestedSQLCalls(attr.Expr) {
		*out = append(*out, sqlRawExprRefusal{
			rng:      call.Range(),
			attrName: attr.Name,
			reason:   "sql() must be the whole attribute value, not part of a larger expression",
		})
	}
}

// nestedSQLCalls returns every sql() call inside expr. Callers reach it only
// when expr itself is not a sql() call, so everything it finds is nested.
func nestedSQLCalls(expr hclsyntax.Expression) []*hclsyntax.FunctionCallExpr {
	var collector sqlCallCollector
	hclsyntax.Walk(expr, &collector) //nolint:errcheck // the walker never reports diagnostics
	return collector.calls
}

type sqlCallCollector struct {
	calls []*hclsyntax.FunctionCallExpr
}

func (c *sqlCallCollector) Enter(node hclsyntax.Node) hcl.Diagnostics {
	if call, ok := node.(*hclsyntax.FunctionCallExpr); ok && call.Name == sqlFuncName {
		c.calls = append(c.calls, call)
	}
	return nil
}

func (c *sqlCallCollector) Exit(hclsyntax.Node) hcl.Diagnostics { return nil }
