package atlasscript

import (
	"fmt"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/convert"
)

// parseMaskBlocks reads the reusable `mask "<name>"` blocks at the top level.
//
// They are read before any script, because a script's `use = [mask.<name>]`
// refers to them by name and a forward reference is the normal way to write
// one: masks are policy and scripts are the work, so an author puts the policy
// where they can find it rather than where the parser needs it.
func parseMaskBlocks(body *hclsyntax.Body) (map[string]Mask, error) {
	masks := make(map[string]Mask)
	for _, block := range body.Blocks {
		if block.Type != "mask" {
			continue
		}
		if len(block.Labels) != 1 {
			return nil, &ParseError{
				Range:   block.DefRange(),
				Message: `a mask block takes one label: mask "<name>"`,
			}
		}
		name := block.Labels[0]
		if _, taken := masks[name]; taken {
			return nil, &ParseError{
				Range:   block.DefRange(),
				Message: fmt.Sprintf("mask %q is declared twice; a name selects one rule, so two cannot share it", name),
			}
		}
		mask, err := parseMaskBody(block)
		if err != nil {
			return nil, err
		}
		mask.Name = name
		if err := mask.Compile(); err != nil {
			return nil, &ParseError{Range: block.DefRange(), Message: err.Error()}
		}
		masks[name] = mask
	}
	return masks, nil
}

// parseMaskBody reads the attributes a mask carries, whatever block it is in.
func parseMaskBody(block *hclsyntax.Block) (Mask, error) {
	method, err := stringAttr(block, "method")
	if err != nil {
		return Mask{}, err
	}
	if method == "" {
		return Mask{}, &ParseError{
			Range:   block.DefRange(),
			Message: "mask has no method; the methods are REDACT, PARTIAL, HASH and REPLACE",
		}
	}
	mask := Mask{Method: MaskMethod(method)}

	if mask.Token, err = stringAttr(block, "token"); err != nil {
		return Mask{}, err
	}
	if mask.Salt, err = stringAttr(block, "salt"); err != nil {
		return Mask{}, err
	}
	if mask.Match, err = stringAttr(block, "match"); err != nil {
		return Mask{}, err
	}
	if mask.With, err = stringAttr(block, "with"); err != nil {
		return Mask{}, err
	}
	if attr := block.Body.Attributes["keep_right"]; attr != nil {
		value, diags := attr.Expr.Value(nil)
		if diags.HasErrors() || value.IsNull() {
			return Mask{}, &ParseError{Range: block.DefRange(), Message: "keep_right must be a number"}
		}
		converted, convErr := convert.Convert(value, cty.Number)
		if convErr != nil {
			return Mask{}, &ParseError{Range: block.DefRange(), Message: "keep_right must be a number"}
		}
		keep, _ := converted.AsBigFloat().Int64()
		mask.KeepRight = int(keep)
	}
	if attr := block.Body.Attributes["columns"]; attr != nil {
		columns, listErr := rawList(attr)
		if listErr != nil {
			return Mask{}, listErr
		}
		mask.Columns = columns
	}
	return mask, nil
}

// parseStepMasks collects the masks a step applies, in declaration order.
//
// Inline `mask` blocks and `use = [mask.<name>]` references are both accepted,
// and the order is the order they appear -- which the first-match rule makes
// load-bearing rather than cosmetic. A reference to a mask nobody declared is
// refused rather than skipped: skipping it would run the query with one fewer
// mask than the author wrote, which is a leak that looks like a working script.
func parseStepMasks(block *hclsyntax.Block, declared map[string]Mask) (MaskSet, error) {
	set := make(MaskSet, 0)

	if attr := block.Body.Attributes["use"]; attr != nil {
		names, err := maskReferences(attr)
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			mask, known := declared[name]
			if !known {
				return nil, &ParseError{
					Range:   block.DefRange(),
					Message: fmt.Sprintf("mask %q is used but never declared", name),
				}
			}
			set = append(set, mask)
		}
	}

	for _, nested := range block.Body.Blocks {
		if nested.Type != "mask" {
			continue
		}
		mask, err := parseMaskBody(nested)
		if err != nil {
			return nil, err
		}
		if len(nested.Labels) == 1 {
			mask.Name = nested.Labels[0]
		}
		set = append(set, mask)
	}
	return set, nil
}

// maskReferences reads `use = [mask.a, mask.b]` into the names it points at.
func maskReferences(attr *hclsyntax.Attribute) ([]string, error) {
	list, ok := attr.Expr.(*hclsyntax.TupleConsExpr)
	if !ok {
		return nil, &ParseError{Range: attr.SrcRange, Message: "use must be a list of mask references"}
	}
	names := make([]string, 0, len(list.Exprs))
	for _, expr := range list.Exprs {
		traversal, diags := hcl.AbsTraversalForExpr(expr)
		if diags.HasErrors() || len(traversal) != 2 || traversal.RootName() != "mask" {
			return nil, &ParseError{
				Range:   attr.SrcRange,
				Message: "use takes mask references, written mask.<name>",
			}
		}
		step, isAttr := traversal[1].(hcl.TraverseAttr)
		if !isAttr {
			return nil, &ParseError{
				Range:   attr.SrcRange,
				Message: "use takes mask references, written mask.<name>",
			}
		}
		names = append(names, step.Name)
	}
	return names, nil
}
