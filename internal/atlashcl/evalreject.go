package atlashcl

import (
	"fmt"
	"slices"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

// typeAttrNames are the attributes whose value is a TYPE rather than a value.
// Types are read from source text -- `varchar(255)` is a call Ptah must not
// evaluate, `int` is a traversal that names no variable -- so the checks below
// skip them. A var. reference is still refused there: measured, the pinned
// Atlas community binary v1.3.0 refuses `type = var.x` too ("set field
// \"type\": unexpected type string"), and resolving it would exit 0 where that
// binary exits 1.
//
// The list is three names, not one. `column.type` is the obvious member;
// `function.return` and `range.subtype` hold a type in exactly the same way and
// are read through the same source-text path, so leaving them out would refuse
// `return = varchar(255)` as a call to an unknown function.
var typeAttrNames = []string{"return", "subtype", "type"}

// rejectUnresolvedExprs refuses every expression the evaluation context was
// supposed to resolve and could not.
//
// It runs before the body walk for the same reason rejectMalformedSQLRawExprs
// does: every value helper in this package falls back to an attribute's SOURCE
// TEXT when the expression will not evaluate, so an unresolved `var.status`
// silently becomes the DDL literal 'var.status' -- issue #926, and the reason
// docs/site/src/content/docs/reference/hcl-schema.md's "unsupported HCL
// constructs return errors rather than partial output" was not true.
//
// Only two shapes are checked, and the narrowness is the point. A schema file
// is full of traversals that are references rather than values -- schema.main,
// column.c, enum.status, the bare keyword `text` -- and every one of them
// still resolves from source text exactly as before.
func (p *parser) rejectUnresolvedExprs(body *hclsyntax.Body) error {
	refusals := p.collectUnresolvedExprs(body, nil)
	if len(refusals) == 0 {
		return nil
	}
	// Attributes hang off a map, so report the one that comes first in the
	// file rather than whichever the map iteration reached first.
	return slices.MinFunc(refusals, func(a, b unresolvedExpr) int {
		return a.start - b.start
	}).err
}

// unresolvedExpr is one expression that had to evaluate and did not, with the
// source offset that orders it against the others.
type unresolvedExpr struct {
	start int
	err   error
}

func (p *parser) collectUnresolvedExprs(body *hclsyntax.Body, out []unresolvedExpr) []unresolvedExpr {
	for _, name := range sortedAttrNames(body.Attributes) {
		out = p.appendUnresolvedExpr(name, body.Attributes[name], out)
	}
	for _, block := range body.Blocks {
		// variable and locals bodies built the evaluation context; re-checking
		// them here would evaluate a type constraint as if it were a value.
		if block.Type == variableBlockType || block.Type == localsBlockType {
			continue
		}
		out = p.collectUnresolvedExprs(block.Body, out)
	}
	return out
}

func (p *parser) appendUnresolvedExpr(name string, attr *hclsyntax.Attribute, out []unresolvedExpr) []unresolvedExpr {
	target, wrapped := sqlCallArgument(attr.Expr)
	if slices.Contains(typeAttrNames, name) {
		if wrapped || !usesEvalNamespace(attr.Expr) {
			return out
		}
		rng := attr.Expr.Range()
		return append(out, unresolvedExpr{
			start: rng.Start.Byte,
			err: fmt.Errorf(
				"parse HCL schema at %s: a variable reference is not supported in a type",
				rng.String(),
			),
		})
	}
	if !mustEvaluate(target) {
		return out
	}
	err := p.unresolvedExprError(target)
	if err == nil {
		return out
	}
	return append(out, unresolvedExpr{start: target.Range().Start.Byte, err: err})
}

// unresolvedExprError evaluates one expression and reports why it would not
// resolve, or nil when it does.
//
// A namespace that is not in the context at all is reported by name and at the
// root's own range, the way checkDroppedExpr reports one, because that is what
// the community binary underlines -- `var.status` points at `var`. Everything
// else is the HCL diagnostic, which already names the member or the operand.
//
// The one exception is a `schema.` reference the document itself declares: the
// HCL diagnostic calls that name unknown, and it is not.
func (p *parser) unresolvedExprError(expr hclsyntax.Expression) error {
	for _, traversal := range hclsyntax.Variables(expr) {
		root, ok := traversal[0].(hcl.TraverseRoot)
		if !ok {
			continue
		}
		if !isEvalNamespace(root.Name) {
			if p.declaresSchemaRoot(root.Name) {
				return p.schemaReferenceNotAValueError(root.Name, root.SrcRange)
			}
			continue
		}
		if _, bound := p.ctx.Variables[root.Name]; !bound {
			return p.unknownVariableError(root.Name, root.SrcRange)
		}
	}
	if _, diags := expr.Value(p.ctx); diags.HasErrors() {
		return p.evaluationFailed(diags)
	}
	return nil
}

// declaresSchemaRoot reports whether name is the `schema.` reference root and
// this document's own top-level `schema` blocks bind it.
//
// Both halves matter. A document with no `schema` block does not bind the root,
// and "there is no variable named schema" is then the accurate thing to say.
func (p *parser) declaresSchemaRoot(name string) bool {
	return name == droppedSchemaNamespace && len(p.declaredSchemas) > 0
}

// schemaReferenceNotAValueError reports a `schema.` reference used where a value
// was required.
//
// It replaces the HCL diagnostic for this one shape because that diagnostic is
// wrong about the cause. Measured on a document declaring `schema "main"`:
// `procedure "p" { names = jsonencode(schema.main) }` was refused with
// `unknown variable: There is no variable named "schema"` -- for a name bound
// two lines away, which `schema = schema.main` resolves on the same file, and
// which a dropped body's own scope binds (see droppedSchemaRoot). A reader sent
// after a missing block finds one already there.
//
// The refusal stays. Only a function call spelled as the whole value reaches
// here (see mustEvaluate), every value helper in this package falls back to an
// attribute's SOURCE TEXT when an expression will not evaluate, and a call let
// through would reach the database as the literal `jsonencode(schema.main)`.
// The pinned Atlas community binary v1.3.0 loads that document at exit 0, so
// this is Ptah refusing what that binary accepts -- the same asymmetry the
// closed dropped-body scope takes, and the one that costs a message rather than
// a wrong schema.
func (p *parser) schemaReferenceNotAValueError(name string, rng hcl.Range) error {
	return fmt.Errorf(
		"parse HCL schema at %s: schema reference %q has no value to pass to a function call: only var. and local. references are evaluated here",
		rng.String(), name,
	)
}

// sqlCallArgument unwraps `sql(X)` to X, reporting whether it unwrapped
// anything. The wrapper itself is never evaluated -- `sql` is not a function in
// the evaluation context -- but its argument is, so `sql("${var.floor} + 1")`
// is checked on the template it carries.
func sqlCallArgument(expr hclsyntax.Expression) (hclsyntax.Expression, bool) {
	call, ok := expr.(*hclsyntax.FunctionCallExpr)
	if !ok || call.Name != sqlFuncName || len(call.Args) != 1 {
		return expr, false
	}
	return call.Args[0], true
}

// mustEvaluate reports whether an expression is one the evaluation context is
// responsible for resolving, so that failing to resolve it is an error rather
// than a fall-through to source text.
//
// Two shapes qualify:
//
//   - anything reaching the var. or local. namespace, at any depth, including
//     inside a template: `var.status`, `"prefix_${var.n}"`, `local.d`
//   - a function call spelled as the whole value: `upper("abc")`, `now()`
//
// Everything else keeps the source-text path: a bare traversal names a schema
// object or a type keyword, and a tuple of traversals (`columns = [column.c]`)
// names columns.
func mustEvaluate(expr hclsyntax.Expression) bool {
	if usesEvalNamespace(expr) {
		// ...unless the same expression also reaches a namespace this context
		// does not bind. `column = var.by_a ? column.a : column.b` mixes the two
		// resolvers: `var` comes from here, `column` from the schema-object
		// resolver in column_ref.go, which picks the branch and reads the
		// column off it (stokaro/ptah#1182). Evaluating the whole expression
		// here would fail on `column` and report `unknown variable: There is no
		// variable named "column"` for a spelling the pinned community binary
		// plans correctly.
		//
		// Ownership, not tolerance: an expression naming a schema object is the
		// object resolver's, and it still refuses one that names nothing.
		return !usesNonEvalTraversal(expr)
	}
	call, ok := expr.(*hclsyntax.FunctionCallExpr)
	return ok && call.Name != sqlFuncName
}

// usesEvalNamespace reports whether expr reads var. or local. anywhere.
func usesEvalNamespace(expr hclsyntax.Expression) bool {
	for _, traversal := range hclsyntax.Variables(expr) {
		if isEvalNamespace(traversal.RootName()) {
			return true
		}
	}
	return false
}

func isEvalNamespace(name string) bool {
	return name == varNamespace || name == localNamespace
}

// usesNonEvalTraversal reports whether expr reads a traversal root the
// evaluation context does not bind -- `column`, `table`, `enum`, `schema` and
// the other schema-object namespaces.
func usesNonEvalTraversal(expr hclsyntax.Expression) bool {
	for _, traversal := range hclsyntax.Variables(expr) {
		if !isEvalNamespace(traversal.RootName()) {
			return true
		}
	}
	return false
}
