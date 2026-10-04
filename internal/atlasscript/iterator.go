package atlasscript

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/hashicorp/hcl/v2"
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
	var initBlock, nextBlock *hclsyntax.Block
	for _, nested := range found.Body.Blocks {
		switch nested.Type {
		case "cursor":
			// The cursor's attributes name the carried columns and their
			// types, read in SOURCE order rather than map order: hclsyntax
			// keeps attributes in a map, and Go randomizes that iteration, so
			// taking them as they come would order the cursor differently on
			// every run. The values are matched to the result set by name, so
			// the order is what the report and a reader see.
			columns, err := parseColumns(nested)
			if err != nil {
				return nil, err
			}
			iterator.Cursor = columns
		case "batch":
			columns, err := parseColumns(nested)
			if err != nil {
				return nil, err
			}
			iterator.Batch = columns
		case "init":
			initBlock = nested
		case "next":
			nextBlock = nested
		default:
			return nil, &ParseError{
				Range:   nested.DefRange(),
				Message: fmt.Sprintf("unsupported block %q inside an iterator", nested.Type),
			}
		}
	}

	// The queries are read after the cursor, which may be written below
	// them: the next query's args are checked against its columns.
	shape := pageShape{cursor: iterator.Cursor, batch: iterator.Batch}
	if initBlock != nil {
		sql, args, err := parseIteratorQuery(initBlock, scopeConstant, shape)
		if err != nil {
			return nil, err
		}
		iterator.InitSQL, iterator.InitArgs = sql, args
	}
	if nextBlock != nil {
		sql, args, err := parseIteratorQuery(nextBlock, scopeNext, shape)
		if err != nil {
			return nil, err
		}
		iterator.NextSQL, iterator.NextArgs = sql, args
	}

	if err := requireIteratorParts(iterator); err != nil {
		return nil, err
	}
	return iterator, nil
}

// requireIteratorParts refuses an iterator that cannot walk: one with no
// first query, no query for the pages after it, or no cursor to resume from.
func requireIteratorParts(iterator *Iterator) error {
	if iterator.InitSQL == "" {
		return &ParseError{Range: iterator.Range, Message: "the iterator has no init sql"}
	}
	if iterator.NextSQL == "" {
		// Without `next` the walk has one page and the loop would run its body
		// once, which is the batching silently not happening rather than a
		// smaller batch size.
		return &ParseError{
			Range:   iterator.Range,
			Message: "the iterator has no next sql, so the walk would stop after one batch",
		}
	}
	if len(iterator.Cursor) == 0 {
		return &ParseError{
			Range:   iterator.Range,
			Message: "the iterator has no cursor, so each batch could not resume after the last",
		}
	}
	return nil
}

// parseIteratorQuery reads an init or next block: its sql, and its args
// checked against what the scope reads.
func parseIteratorQuery(
	block *hclsyntax.Block, scope argScope, shape pageShape,
) (string, []hcl.Expression, error) {
	sql, err := stringAttr(block, "sql")
	if err != nil {
		return "", nil, err
	}
	attr := block.Body.Attributes["args"]
	if attr == nil {
		return sql, nil, nil
	}
	args, err := parseArgs(attr, scope, shape)
	if err != nil {
		return "", nil, err
	}
	return sql, args, nil
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
