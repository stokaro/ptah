package atlashcl

import (
	"slices"
	"strings"

	"github.com/hashicorp/hcl/v2/hclsyntax"

	"ptah.run/core/hclext"
	"ptah.run/core/schemamodel"
)

// ownedPlatform is a table's platform group an owner reads at
// [hclext.TablePlatform]: the shape, the overrides every block of the group
// writes, and the group's first block, where its claim stands.
type ownedPlatform struct {
	shape  hclext.Shape
	block  *hclsyntax.Block
	values map[string]hclext.Value
}

// parseTablePlatforms reads a table's platform blocks. A block whose label an
// owner reads at [hclext.TablePlatform], as the shape's type or one of its
// aliases, joins that owner's group, and every other block the table's
// overrides, as parsePlatformOverrides reads them. A key written twice is
// refused across the blocks of one group, as across those of one label.
func (p *parser) parseTablePlatforms(block *hclsyntax.Block) (map[string]map[string]string, []ownedPlatform, error) {
	overrides := make(map[string]map[string]string)
	var owned []ownedPlatform
	for _, platformBlock := range block.Body.Blocks {
		if platformBlock.Type != "platform" {
			continue
		}
		if len(platformBlock.Labels) == 1 {
			if shape, claimed := p.owners.Lookup(hclext.TablePlatform, platformBlock.Labels[0]); claimed {
				index := slices.IndexFunc(owned, func(group ownedPlatform) bool { return group.shape.Block.Type == shape.Block.Type })
				if index < 0 {
					owned = append(owned, ownedPlatform{shape: shape, block: platformBlock, values: make(map[string]hclext.Value)})
					index = len(owned) - 1
				}
				if err := p.parseOwnedPlatform(&owned[index], platformBlock); err != nil {
					return nil, nil, err
				}
				continue
			}
		}
		if err := p.parsePlatformBlock(overrides, platformBlock, "table"); err != nil {
			return nil, nil, err
		}
	}
	if len(overrides) == 0 {
		return nil, owned, nil
	}
	return overrides, owned, nil
}

// parseOwnedPlatform reads one platform block of an owner's group into the
// group. An override the shape does not list is refused at its block, naming
// the overrides the shape lists, and one that differs from a listed name only
// in case names the listed one: the owner never sees a name it does not read.
func (p *parser) parseOwnedPlatform(group *ownedPlatform, block *hclsyntax.Block) error {
	label := block.Labels[0]
	for name := range block.Body.Attributes {
		return p.blockError(block, "unsupported table platform attribute %q", name)
	}
	names := make([]string, 0, len(group.shape.Block.Attributes))
	for _, attribute := range group.shape.Block.Attributes {
		names = append(names, attribute.Name)
	}
	for _, overrideBlock := range block.Body.Blocks {
		if overrideBlock.Type != "override" {
			if err := p.rejectUnsupportedBlock(overrideBlock, "table platform"); err != nil {
				return err
			}
			continue
		}
		override, err := p.parsePlatformOverride(overrideBlock, "table")
		if err != nil {
			return err
		}
		if !slices.Contains(names, override.key) {
			if lower := strings.ToLower(override.key); lower != override.key && slices.Contains(names, lower) {
				return p.blockError(overrideBlock, "unknown table platform %q override %q: override names are lower case, as %q", label, override.key, lower)
			}
			return p.blockError(overrideBlock, "unknown table platform %q override %q: the group takes %s", label, override.key, strings.Join(names, ", "))
		}
		if _, exists := group.values[override.key]; exists {
			return p.blockError(overrideBlock, "table platform override %q for dialect %q is duplicated", override.key, label)
		}
		group.values[override.key] = hclext.Value{Kind: hclext.String, Text: override.value}
	}
	return nil
}

// claimOwnerPlatforms records the platform groups of the table at position
// for the owners that read them, each standing at its first block.
func (p *parser) claimOwnerPlatforms(table schemamodel.Table, position int, groups []ownedPlatform) {
	for _, group := range groups {
		p.ownerClaims = append(p.ownerClaims, ownerClaim{block: group.block, index: -1, table: position,
			match: hclext.Match{Shape: group.shape, Claim: hclext.Claim{Block: group.shape.Block.Type, Attributes: group.values, At: blockAt(group.block),
				Range: blockRange(group.block), Table: &hclext.Table{Schema: table.Schema, Name: table.Name, Struct: table.StructName}}}})
	}
}
