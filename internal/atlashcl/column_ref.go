package atlashcl

import (
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"go.5x5.cz/ptah/internal/tableref"
)

// maxColumnRefDepth bounds how many wrappers a column reference may be buried
// under. HCL's own parser already refuses deeply nested source, so no realistic
// schema reaches this; it exists so a hand-built expression tree cannot turn the
// unwrapping below into unbounded recursion.
const maxColumnRefDepth = 64

// tableColumnFromExpr reads a `column` value as a column reference by asking the
// PARSED expression, and returns ("", "") when the expression cannot name a
// column at all.
//
// This is issue #1182. The refusal added for #1106 keyed on the ATTRIBUTE'S
// SOURCE TEXT: it sliced the attribute's bytes and re-parsed them, and
// [hcl.AbsTraversalForExpr] answers no to anything that is not a bare traversal.
// So five spellings that name a column perfectly well were refused while the
// pinned Atlas community binary v1.3.0 planned them at exit 0 -- a parenthesised
// reference in `index.on`, the same across newlines, the same in
// `primary_key.on`, and both conditional forms. Measured before this function
// existed, all five exited 1 with `column contains unsupported reference`.
//
// Two wrappers stand between the attribute and the traversal, and each needs its
// own answer:
//
//   - Parentheses. [hclsyntax.ParenthesesExpr] embeds the hclsyntax.Expression
//     INTERFACE, so it promotes only what that interface declares -- and
//     `AsTraversal` is declared on the concrete traversal expressions, not on the
//     interface. It has no `UnwrapExpression` either, so
//     [hcl.UnwrapExpressionUntil] cannot see through it. Handing `attr.Expr`
//     straight to [hcl.AbsTraversalForExpr] therefore fails on `(column.n)`
//     exactly as the sliced text did; the unwrap has to be explicit.
//
//   - A conditional. Only one branch is taken, and which one is a question about
//     VALUES, so the condition is evaluated. See conditionalBranch.
//
// What the refusal was for survives untouched: a value that resolves to no
// column name still returns "" and the caller still refuses. `column = sql("n")`
// is a function call, which is neither wrapper and is not a traversal, and
// `column = 42` is a number that unquotes to nothing.
func (p *parser) tableColumnFromExpr(expr hclsyntax.Expression) (table, column string) {
	return p.tableColumnFromExprAt(expr, 0)
}

func (p *parser) tableColumnFromExprAt(expr hclsyntax.Expression, depth int) (table, column string) {
	if expr == nil || depth > maxColumnRefDepth {
		return "", ""
	}
	switch expr := expr.(type) {
	case *hclsyntax.ParenthesesExpr:
		return p.tableColumnFromExprAt(expr.Expression, depth+1)
	case *hclsyntax.ConditionalExpr:
		branch, ok := p.conditionalBranch(expr)
		if !ok {
			return "", ""
		}
		return p.tableColumnFromExprAt(branch, depth+1)
	}
	if traversal, diags := hcl.AbsTraversalForExpr(expr); !diags.HasErrors() {
		// An iteration bound by an enclosing dynamic block is a VALUE to
		// evaluate, not a table.column pair to read out of source text. The two
		// are the same shape -- `index.value` and `users.id` both parse as a
		// two-step traversal -- so the root name is what tells them apart
		// (stokaro/ptah#1636).
		if p.dynamicIteratorRoot(traversal.RootName()) {
			return "", p.evaluatedColumnName(expr)
		}
		return tableColumnFromTraversal(traversal)
	}
	// The quoted-string spelling, `column = "n"`. It is read from source text
	// rather than evaluated so that this change is about WHICH expressions can be
	// read and not about HOW a string is read: the accepted set for a literal is
	// byte-for-byte what it was before.
	return "", quotedColumnName(p.rawExprNode(expr))
}

// conditionalBranch picks the branch a conditional takes, or reports that the
// condition cannot be decided.
//
// Refusing to guess is the whole point. When the condition does not evaluate to
// a known boolean, this returns false and the caller refuses the attribute --
// which is what the pinned binary does too, by failing the file outright on a
// variable it cannot resolve. Taking a branch anyway would put a column name
// into DDL that the schema never asked for, the same class of silent damage
// #1106 was filed about.
func (p *parser) conditionalBranch(expr *hclsyntax.ConditionalExpr) (hclsyntax.Expression, bool) {
	value, diags := expr.Condition.Value(p.refContext)
	if diags.HasErrors() || !value.IsKnown() || value.IsNull() || value.Type() != cty.Bool {
		return nil, false
	}
	if value.True() {
		return expr.TrueResult, true
	}
	return expr.FalseResult, true
}

// schemaVariableTypes maps an Atlas `variable` block's declared type to the cty
// type its default has to carry.
//
// These three are the bare type keywords already measured to resolve on the
// pinned community binary (see droppedBodyScope). A variable declared as
// anything else stays out of scope, so a conditional that reads it is refused --
// stricter than that binary, but not a new stricter position: every conditional
// was refused before this file existed.
var schemaVariableTypes = map[string]cty.Type{
	"bool":   cty.Bool,
	"int":    cty.Number,
	"string": cty.String,
}

// columnRefContext is the evaluation context a conditional's condition is
// decided in.
//
// It carries the file's own `variable` defaults under `var`, plus the same
// function table a dropped body evaluates against. Ptah has no general
// schema-file variable evaluation -- that is issue #926 -- and this does not add
// one: nothing here reaches DDL. It decides which of two column references a
// conditional selects, and both were written by the schema author.
//
// The admission rule for a variable is measured against the pinned binary, whose
// exit code for the r4 fixture below moves with each part of it. All four
// rejected shapes exit 1 there, so keeping them out of scope keeps Ptah from
// planning a file that binary refuses:
//
//	variable "by_a" { type = bool, default = true }   -> 0, index on "a"
//	variable "pick" { type = string, default = "a" }  -> 0, index on "a"
//	variable "n"    { type = int, default = 1 }       -> 0, index on "a"
//	variable "n"    { type = int, default = 1.5 }     -> 0, index on "b"
//	variable "by_a" { default = true }                -> 1, `The argument "type" is required`
//	variable "by_a" { type = bool }                   -> 1, `missing value for required variable`
//	variable "by_a" { type = nonsense, default = true } -> 1, `There is no variable named "nonsense"`
//	variable "by_a" { type = bool, default = "yes" }  -> 1, `a bool is required`
//
// The int rows are the pair that pins strict type EQUALITY as the rule rather
// than an integer check: that binary accepts a fractional default under
// `type = int` and lets `var.n == 1` come out false.
func columnRefContext(body *hclsyntax.Body) *hcl.EvalContext {
	return &hcl.EvalContext{
		Variables: map[string]cty.Value{"var": cty.ObjectVal(schemaVariableDefaults(body))},
		Functions: droppedBodyFunctions,
	}
}

func schemaVariableDefaults(body *hclsyntax.Body) map[string]cty.Value {
	values := make(map[string]cty.Value)
	for _, block := range body.Blocks {
		name, value, ok := schemaVariableDefault(block)
		if !ok {
			continue
		}
		values[name] = value
	}
	return values
}

func schemaVariableDefault(block *hclsyntax.Block) (name string, value cty.Value, ok bool) {
	if block.Type != "variable" || len(block.Labels) != 1 || block.Body == nil {
		return "", cty.NilVal, false
	}
	typeAttr := block.Body.Attributes["type"]
	defaultAttr := block.Body.Attributes["default"]
	if typeAttr == nil || defaultAttr == nil {
		return "", cty.NilVal, false
	}
	want, known := declaredVariableType(typeAttr.Expr)
	if !known {
		return "", cty.NilVal, false
	}
	value, diags := defaultAttr.Expr.Value(nil)
	if diags.HasErrors() || !value.IsKnown() || value.IsNull() || !value.Type().Equals(want) {
		return "", cty.NilVal, false
	}
	return block.Labels[0], value, true
}

func declaredVariableType(expr hclsyntax.Expression) (cty.Type, bool) {
	traversal, diags := hcl.AbsTraversalForExpr(expr)
	if diags.HasErrors() || len(traversal) != 1 {
		return cty.NilType, false
	}
	root, isRoot := traversal[0].(hcl.TraverseRoot)
	if !isRoot {
		return cty.NilType, false
	}
	want, known := schemaVariableTypes[root.Name]
	return want, known
}

func quotedColumnName(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimSuffix(raw, ",")
	unquoted, err := strconv.Unquote(raw)
	if err != nil {
		return ""
	}
	return unquoted
}

func tableColumnFromTraversal(traversal hcl.Traversal) (table, column string) {
	if len(traversal) < 2 {
		return "", ""
	}
	root, isRoot := traversal[0].(hcl.TraverseRoot)
	if !isRoot {
		return "", ""
	}
	parts := make([]string, 0, len(traversal)-1)
	for _, step := range traversal[1:] {
		part, named := traversalPart(step)
		if !named {
			return "", ""
		}
		parts = append(parts, part)
	}
	if root.Name == "column" && len(parts) == 1 {
		return "", parts[0]
	}
	if root.Name != "table" || len(parts) < 3 || parts[len(parts)-2] != "column" {
		return "", ""
	}
	return qualifiedTableColumn(parts)
}

func qualifiedTableColumn(parts []string) (table, column string) {
	tableParts := parts[:len(parts)-2]
	if len(tableParts) == 1 {
		return tableref.Canonical("", tableParts[0]), parts[len(parts)-1]
	}
	if len(tableParts) == 2 {
		return tableref.Canonical(tableParts[0], tableParts[1]), parts[len(parts)-1]
	}
	return "", ""
}

// evaluatedColumnName evaluates an expression rooted at a dynamic iterator and
// returns the column name it produced, or "" when it produced anything other
// than a known non-empty string. The caller turns "" into the same refusal an
// unreadable reference gets, so a for_each over the wrong kind of collection
// fails rather than putting a rendered cty value into DDL.
func (p *parser) evaluatedColumnName(expr hclsyntax.Expression) string {
	value, diags := expr.Value(p.ctx)
	if diags.HasErrors() || value.IsNull() || !value.IsKnown() || value.Type() != cty.String {
		return ""
	}
	return value.AsString()
}
