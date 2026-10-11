package atlashcl

import (
	"context"
	"errors"
	"strings"

	"github.com/hashicorp/hcl/v2/hclsyntax"

	"ptah.run/core/hclext"
)

// ownerClaim is a block an owner's shape matched, waiting for the owner's
// declaration, and the block a refusal is positioned at.
type ownerClaim struct {
	block *hclsyntax.Block
	match hclext.Match
}

// claimOwnerBlock reads a block an owner's shape matches with the refusals
// every block gets -- its labels, its nested blocks, its unknown attributes
// and the kind of each attribute it takes -- and records it for the owner to
// read once the document is walked.
//
// The attributes are read in the order the shape lists them, and each as its
// kind: a [hclext.String] through stringAttr, which refuses any other value,
// and a [hclext.Text] through exprString, which writes any value as text. The
// owner receives the values and never the expressions, so this package's
// evaluation context stays its own.
func (p *parser) claimOwnerBlock(block *hclsyntax.Block, shape hclext.Shape) error {
	schema, name, err := p.objectSchemaAndName(block, block.Type)
	if err != nil {
		return err
	}
	if err := p.rejectNestedBlocks(block, block.Type); err != nil {
		return err
	}
	supported := map[string]bool{"schema": true}
	for _, attribute := range shape.Block.Attributes {
		supported[attribute.Name] = true
	}
	if err := p.rejectUnsupportedAttrs(block, supported, block.Type); err != nil {
		return err
	}
	values := make(map[string]hclext.Value, len(shape.Block.Attributes))
	for _, attribute := range shape.Block.Attributes {
		attr := block.Body.Attributes[attribute.Name]
		if attr == nil {
			continue
		}
		// Each attribute is evaluated once: a print() call in it prints
		// once, as it does in a block of this package's own.
		var text string
		switch attribute.Kind {
		case hclext.String:
			if text, err = p.stringAttr(block, attribute.Name, block.Type); err != nil {
				return err
			}
		default:
			text = p.exprString(attr)
		}
		values[attribute.Name] = hclext.Value{Kind: attribute.Kind, Text: text}
	}
	p.ownerClaims = append(p.ownerClaims, ownerClaim{block: block, match: hclext.Match{Shape: shape,
		Claim: hclext.Claim{Block: block.Type, Schema: schema, Name: name, Attributes: values}}})
	return nil
}

// readOwnerClaims hands every claimed block to its owner, one call per owner,
// and adds the objects each declares in the document's order. A refusal is
// positioned at the refused block, as a refusal of this package's own is,
// and so is a second declaration of one object.
func (p *parser) readOwnerClaims() error {
	if len(p.ownerClaims) == 0 {
		return nil
	}
	matches := make([]hclext.Match, 0, len(p.ownerClaims))
	for _, claim := range p.ownerClaims {
		matches = append(matches, claim.match)
	}
	// The loaders take no context yet. The owners read their blocks in
	// process, so the walk bounds the read.
	declarations, err := p.owners.Decode(context.Background(), matches)
	if refused, ok := errors.AsType[*hclext.ClaimError](err); ok {
		return p.blockError(p.ownerClaims[refused.Claim].block, "%v", refused.Err)
	}
	if err != nil {
		return err
	}
	for i, declaration := range declarations {
		claim := p.ownerClaims[i]
		for _, object := range declaration.Objects {
			objects, err := p.db.FeatureObjects.With(object)
			if err != nil {
				return p.blockError(claim.block, "%s %s is declared twice: %v", claim.block.Type, qualifiedName(claim.match.Claim), err)
			}
			p.db.FeatureObjects = objects
		}
	}
	return nil
}

// qualifiedName names the object a claim declares as a refusal names it: its
// schema and name, or its name alone where its block names no schema.
func qualifiedName(claim hclext.Claim) string {
	if strings.TrimSpace(claim.Schema) == "" {
		return claim.Name
	}
	return claim.Schema + "." + claim.Name
}
