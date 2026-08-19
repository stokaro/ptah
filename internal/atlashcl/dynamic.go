package atlashcl

import (
	"fmt"
	"slices"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// dynamicBlockType is the block a schema file writes to generate repeated
// blocks of one type from a collection.
const dynamicBlockType = "dynamic"

// dynamicIteratorAttr overrides the name a generated body reads its iteration
// from. Absent, that name is the block's label, which is HCL's own rule for
// dynamic blocks and the one the pinned community binary's error message
// implies: it answers `This object does not have an attribute named "value"`
// for `dynamic.value`, so `dynamic` is not the iterator there either.
const dynamicIteratorAttr = "iterator"

// expandDynamic turns one `dynamic "<type>"` block into the blocks it stands
// for and hands each to emit, with the iteration bound as `<iterator>.value`
// and `<iterator>.key` for the duration of that one call.
//
// Ptah expands these; the pinned community Atlas binary v1.3.0 does not. That
// is a measured divergence in the direction of doing more: on that binary a
// `dynamic "index"` block with a `for_each` exits 0 and plans no index at all,
// with no diagnostic -- the operator's declared intent disappears between the
// file and the plan (stokaro/ptah#1636). The exit status is unchanged by
// expanding, and a file that carries no `dynamic` block cannot notice this code
// exists.
//
// The `content` body is parsed once per element rather than materialized into a
// synthetic document, because a generated block has to be evaluated with its
// own iteration bound and the parser evaluates attributes lazily against
// [parser.ctx].
func (p *parser) expandDynamic(block *hclsyntax.Block, emit func(*hclsyntax.Block) error) error {
	generated, err := p.dynamicGeneratedType(block)
	if err != nil {
		return err
	}
	content, err := p.dynamicContentBlock(block)
	if err != nil {
		return err
	}
	iterator, err := p.dynamicIteratorName(block, generated)
	if err != nil {
		return err
	}
	elements, err := p.dynamicForEach(block)
	if err != nil {
		return err
	}

	outer := p.ctx
	outerIterators := p.dynamicIterators
	defer func() {
		p.ctx = outer
		p.dynamicIterators = outerIterators
	}()
	p.dynamicIterators = append(append([]string(nil), outerIterators...), iterator)
	for _, element := range elements {
		p.ctx = dynamicChildContext(outer, iterator, element)
		labels, err := p.dynamicLabels(block, content)
		if err != nil {
			return err
		}
		if err := emit(&hclsyntax.Block{
			Type:            generated,
			Labels:          labels,
			Body:            content.Body,
			TypeRange:       block.TypeRange,
			LabelRanges:     block.LabelRanges,
			OpenBraceRange:  content.OpenBraceRange,
			CloseBraceRange: content.CloseBraceRange,
		}); err != nil {
			return err
		}
	}
	return nil
}

// dynamicIteratorName answers which root name the generated body reads its
// iteration from: the `iterator` attribute when written, and the block's label
// otherwise.
func (p *parser) dynamicIteratorName(block *hclsyntax.Block, generated string) (string, error) {
	attr, ok := block.Body.Attributes[dynamicIteratorAttr]
	if !ok {
		return generated, nil
	}
	traversal, diags := hcl.AbsTraversalForExpr(attr.Expr)
	if diags.HasErrors() || len(traversal) != 1 {
		return "", fmt.Errorf("dynamic iterator at %s must be a bare name", attr.SrcRange.String())
	}
	return traversal.RootName(), nil
}

// dynamicLabels evaluates the labels the generated block carries.
//
// A block type that takes labels -- `index "name"`, `column "name"` -- needs one
// per iteration, and HCL spells that as a `labels` attribute on the dynamic
// block, evaluated with the iteration bound. A content block that already
// carries literal labels keeps them, which is the shape for a generated block
// whose label does not vary.
func (p *parser) dynamicLabels(block, content *hclsyntax.Block) ([]string, error) {
	attr, ok := block.Body.Attributes["labels"]
	if !ok {
		return content.Labels, nil
	}
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return nil, fmt.Errorf("evaluate dynamic labels at %s: %s", attr.SrcRange.String(), diags.Error())
	}
	if value.IsNull() || !value.IsKnown() || (!value.Type().IsTupleType() && !value.Type().IsListType()) {
		return nil, fmt.Errorf("dynamic labels at %s must be a known list of strings", attr.SrcRange.String())
	}
	labels := make([]string, 0, value.LengthInt())
	for iterator := value.ElementIterator(); iterator.Next(); {
		_, element := iterator.Element()
		if element.Type() != cty.String || element.IsNull() {
			return nil, fmt.Errorf("dynamic labels at %s must be a known list of strings", attr.SrcRange.String())
		}
		labels = append(labels, element.AsString())
	}
	return labels, nil
}

// dynamicGeneratedType reads the one label naming the block type to generate.
func (p *parser) dynamicGeneratedType(block *hclsyntax.Block) (string, error) {
	if len(block.Labels) != 1 {
		return "", p.blockError(block, "dynamic block requires exactly one label naming the block type to generate")
	}
	if block.Labels[0] == dynamicBlockType {
		return "", p.blockError(block, "dynamic block cannot generate another dynamic block")
	}
	return block.Labels[0], nil
}

// dynamicContentBlock reads the single `content` block holding the body to
// repeat. A dynamic block with no content generates nothing and is refused
// rather than silently dropped, which is the shape this whole construct exists
// to stop.
func (p *parser) dynamicContentBlock(block *hclsyntax.Block) (*hclsyntax.Block, error) {
	var content *hclsyntax.Block
	for _, nested := range block.Body.Blocks {
		if nested.Type != "content" {
			return nil, p.blockError(nested, "unsupported dynamic block %q: a dynamic block holds one content block", nested.Type)
		}
		if content != nil {
			return nil, p.blockError(nested, "dynamic block declares more than one content block")
		}
		content = nested
	}
	if content == nil {
		return nil, p.blockError(block, "dynamic block requires a content block")
	}
	return content, nil
}

// dynamicForEach evaluates `for_each` into the elements to iterate.
//
// A list yields its elements with an integer key; a map or object yields its
// entries sorted by key, so a generated document is byte-stable across runs.
func (p *parser) dynamicForEach(block *hclsyntax.Block) ([]dynamicElement, error) {
	attr, ok := block.Body.Attributes["for_each"]
	if !ok {
		return nil, p.blockError(block, "dynamic block requires a for_each attribute")
	}
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return nil, fmt.Errorf("evaluate dynamic for_each at %s: %s", attr.SrcRange.String(), diags.Error())
	}
	if value.IsNull() || !value.IsKnown() {
		return nil, fmt.Errorf("dynamic for_each at %s is not a known collection", attr.SrcRange.String())
	}
	valueType := value.Type()
	if !valueType.IsTupleType() && !valueType.IsListType() && !valueType.IsSetType() &&
		!valueType.IsMapType() && !valueType.IsObjectType() {
		return nil, fmt.Errorf("dynamic for_each at %s must be a list, set, map or object", attr.SrcRange.String())
	}

	elements := make([]dynamicElement, 0, value.LengthInt())
	for iterator := value.ElementIterator(); iterator.Next(); {
		key, element := iterator.Element()
		elements = append(elements, dynamicElement{key: key, value: element})
	}
	return elements, nil
}

// dynamicElement is one iteration of a for_each collection.
type dynamicElement struct {
	key   cty.Value
	value cty.Value
}

// dynamicChildContext binds one iteration under the iterator's root name,
// leaving the parent's var., local. and function namespaces reachable.
func dynamicChildContext(parent *hcl.EvalContext, iterator string, element dynamicElement) *hcl.EvalContext {
	child := parent.NewChild()
	child.Variables = map[string]cty.Value{
		iterator: cty.ObjectVal(map[string]cty.Value{
			"key":   element.key,
			"value": element.value,
		}),
	}
	return child
}

// dynamicIteratorRoot reports whether name is an iterator bound by an enclosing
// dynamic block.
//
// It exists so a column reference can tell `index.value` -- an iteration to
// evaluate -- from `users.id`, which is a table and column read out of source
// text. Without it the two are the same shape and the first is refused as an
// unsupported reference (stokaro/ptah#1636).
func (p *parser) dynamicIteratorRoot(name string) bool {
	return slices.Contains(p.dynamicIterators, name)
}
