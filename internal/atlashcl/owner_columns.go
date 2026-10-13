package atlashcl

import (
	"github.com/hashicorp/hcl/v2/hclsyntax"

	"ptah.run/core/hclext"
)

// ownerChildName validates the label count selected by the owner's shape.
func (p *parser) ownerChildName(block *hclsyntax.Block, child hclext.Child, label string) (string, error) {
	if child.Unlabeled {
		if len(block.Labels) != 0 {
			return "", p.blockError(block, "%s does not accept labels", label)
		}
		return "", nil
	}
	if len(block.Labels) != 1 {
		return "", p.blockError(block, "%s requires exactly one name label", label)
	}
	return block.Labels[0], nil
}

// ownerColumnValue reads parsed local column references without passing
// expressions or evaluation callbacks across the owner boundary.
func (p *parser) ownerColumnValue(block *hclsyntax.Block, attribute hclext.Attribute) (hclext.Value, bool, error) {
	attr := block.Body.Attributes[attribute.Name]
	if attr == nil {
		return hclext.Value{}, false, p.blockError(block, "%s requires %s", block.Type, attribute.Name)
	}
	exprs := []hclsyntax.Expression{attr.Expr}
	if tuple, ok := attr.Expr.(*hclsyntax.TupleConsExpr); ok && attribute.Kind == hclext.Refs {
		exprs = tuple.Exprs
	}
	refs := make([]hclext.Reference, 0, len(exprs))
	for _, expr := range exprs {
		table, name := p.tableColumnFromExpr(expr)
		if table != "" || name == "" {
			return hclext.Value{}, false, p.attributeError(attr, "%s attribute %q requires a local column reference", block.Type, attribute.Name)
		}
		refs = append(refs, hclext.Reference{Kind: "column", Name: name})
	}
	if attribute.Kind == hclext.Ref {
		return hclext.Value{Kind: hclext.Ref, Reference: &refs[0]}, true, nil
	}
	return hclext.Value{Kind: hclext.Refs, References: refs}, true, nil
}
