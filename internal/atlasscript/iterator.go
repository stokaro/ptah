package atlasscript

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/hashicorp/hcl/v2/hclsyntax"
)

// parseIterator reads the one `iterator "keyset"` block a script may carry.
//
// One, not many: two iterators on one script have no defined meaning -- which
// walk does the body run over, and in what order -- and accepting them would
// pick one silently.
func parseIterator(block *hclsyntax.Block) (*Iterator, error) {
	var found *hclsyntax.Block
	for _, nested := range block.Body.Blocks {
		if nested.Type != "iterator" {
			continue
		}
		if found != nil {
			return nil, &ParseError{
				Range:   nested.DefRange(),
				Message: "a script declares one iterator; two have no defined order",
			}
		}
		found = nested
	}
	if found == nil {
		return nil, nil
	}

	if len(found.Labels) != 1 || found.Labels[0] != "keyset" {
		return nil, &ParseError{
			Range:   found.DefRange(),
			Message: `the only iterator is keyset: iterator "keyset" { … }`,
		}
	}

	iterator := &Iterator{Range: found.DefRange()}
	for _, nested := range found.Body.Blocks {
		switch nested.Type {
		case "cursor":
			// The cursor's attributes name the carried columns. Their values
			// are type hints in the documented grammar, and this reads the
			// names only -- the database decides the types, and a hint that
			// disagreed with it would be a second opinion nobody consults.
			//
			// Read in SOURCE order rather than map order. hclsyntax keeps
			// attributes in a map, and Go randomizes that iteration, so taking
			// the names as they come would order the cursor differently on
			// every run -- and the cursor's order is what the next batch's
			// arguments are positioned against.
			iterator.Cursor = attributeNamesInSourceOrder(nested)
		case "init":
			sql, err := stringAttr(nested, "sql")
			if err != nil {
				return nil, err
			}
			iterator.InitSQL = sql
		case "next":
			sql, err := stringAttr(nested, "sql")
			if err != nil {
				return nil, err
			}
			iterator.NextSQL = sql
			if attr := nested.Body.Attributes["args"]; attr != nil {
				args, err := rawList(attr)
				if err != nil {
					return nil, err
				}
				iterator.NextArgs = args
			}
		default:
			return nil, &ParseError{
				Range:   nested.DefRange(),
				Message: fmt.Sprintf("unsupported block %q inside an iterator", nested.Type),
			}
		}
	}

	if iterator.InitSQL == "" {
		return nil, &ParseError{Range: found.DefRange(), Message: "the iterator has no init sql"}
	}
	if iterator.NextSQL == "" {
		// Without `next` the walk has one page and the loop would run its body
		// once, which is the batching silently not happening rather than a
		// smaller batch size.
		return nil, &ParseError{
			Range:   found.DefRange(),
			Message: "the iterator has no next sql, so the walk would stop after one batch",
		}
	}
	if len(iterator.Cursor) == 0 {
		return nil, &ParseError{
			Range:   found.DefRange(),
			Message: "the iterator has no cursor, so each batch could not resume after the last",
		}
	}
	return iterator, nil
}

// attributeNamesInSourceOrder returns a block's attribute names as written.
//
// hclsyntax stores them in a map, and Go randomizes map iteration, so a caller
// that ranged over it would get a different order on every run. Anything
// positional read that way is a bug that reproduces one run in N.
func attributeNamesInSourceOrder(block *hclsyntax.Block) []string {
	attributes := make([]*hclsyntax.Attribute, 0, len(block.Body.Attributes))
	for _, attr := range block.Body.Attributes {
		attributes = append(attributes, attr)
	}
	slices.SortFunc(attributes, func(a, b *hclsyntax.Attribute) int {
		return cmp.Compare(a.SrcRange.Start.Byte, b.SrcRange.Start.Byte)
	})
	names := make([]string, 0, len(attributes))
	for _, attr := range attributes {
		names = append(names, attr.Name)
	}
	return names
}
