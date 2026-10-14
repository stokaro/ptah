package atlashcl

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"ptah.run/core/hclext"
)

// ownerRoleReference reads one global role without interpreting dots in a
// literal role name as a schema. Evaluation stays in the frontend.
func (p *parser) ownerRoleReference(block *hclsyntax.Block, attr *hclsyntax.Attribute, name string) (hclext.Value, error) {
	if attr == nil {
		return hclext.Value{}, p.blockError(block, "%s requires %s", block.Type, name)
	}
	reference, _, ok := p.referenceElement(attr.Expr, "role")
	if !ok || reference.Name == "" || reference.Schema != "" {
		return hclext.Value{}, p.attributeError(attr, "%s attribute %q must name one global role", block.Type, name)
	}
	return hclext.Value{Kind: hclext.Ref, Reference: &reference}, nil
}

// ownerTextList keeps list order and refuses non-string or unknown elements.
// A bare identifier is a literal keyword, not an expression sent to the owner.
func (p *parser) ownerTextList(block *hclsyntax.Block, attr *hclsyntax.Attribute) ([]string, error) {
	if tuple, ok := attr.Expr.(*hclsyntax.TupleConsExpr); ok {
		values := make([]string, 0, len(tuple.Exprs))
		for _, expr := range tuple.Exprs {
			text, ok := p.ownerTextElement(expr)
			if !ok {
				return nil, p.attributeError(attr, "%s attribute %q must contain only known strings or identifiers", block.Type, attr.Name)
			}
			values = append(values, text)
		}
		return values, nil
	}
	if text, ok := bareTextElement(attr.Expr); ok {
		return []string{text}, nil
	}
	value, diags := attr.Expr.Value(p.ctx)
	if !diags.HasErrors() {
		if values, ok := textListValue(value); ok {
			return values, nil
		}
	}
	return nil, p.attributeError(attr, "%s attribute %q must be a known string or list of strings", block.Type, attr.Name)
}

func (p *parser) ownerTextElement(expr hclsyntax.Expression) (string, bool) {
	if text, ok := bareTextElement(expr); ok {
		return text, true
	}
	value, diags := expr.Value(p.ctx)
	if diags.HasErrors() || !value.IsKnown() || value.IsNull() || value.Type() != cty.String {
		return "", false
	}
	return value.AsString(), true
}

func bareTextElement(expr hclsyntax.Expression) (string, bool) {
	traversal, diags := hcl.AbsTraversalForExpr(expr)
	if diags.HasErrors() || len(traversal) != 1 || traversal.RootName() == "null" || traversal.RootName() == "true" || traversal.RootName() == "false" {
		return "", false
	}
	return traversal.RootName(), true
}

func textListValue(value cty.Value) ([]string, bool) {
	if !value.IsKnown() || value.IsNull() {
		return nil, false
	}
	if value.Type() == cty.String {
		return []string{value.AsString()}, true
	}
	if !value.Type().IsListType() && !value.Type().IsTupleType() {
		return nil, false
	}
	var values []string
	for iterator := value.ElementIterator(); iterator.Next(); {
		_, element := iterator.Element()
		if !element.IsKnown() || element.IsNull() || element.Type() != cty.String {
			return nil, false
		}
		values = append(values, element.AsString())
	}
	return values, true
}
