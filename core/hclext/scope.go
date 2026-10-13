package hclext

import (
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/internal/targetscope"
)

// ForDialect returns a view that selects block shapes for dialect. It accepts
// aliases recognized by platform.NormalizeDialect and custom target names.
// Empty selects unscoped declarations. The original set remains unchanged;
// views share immutable shapes and are safe for concurrent use.
func (s Set) ForDialect(dialect string) Set {
	s.dialect = targetscope.Normalize(dialect)
	return s
}

// Dialect returns the normalized default selected by ForDialect, or empty
// for a set that selects unscoped declarations.
func (s Set) Dialect() string { return s.dialect }

// Scoped reports whether a block type selects its shape by dialect. The
// frontend reads its dialect attribute before looking up that shape.
func (s Set) Scoped(site Site, block string) bool {
	routes, found := s.blocks[blockKey{site: site, block: block}]
	return found && routes.universal < 0
}

func scopedBlock(block Block) bool {
	return len(block.Dialects) > 0 || block.Unscoped
}

func validateBlockScope(owner string, block Block) error {
	if scopedBlock(block) && block.Site != TopLevel {
		return fmt.Errorf("HCL %s block %q of %s scopes its dialect, which only a top-level block does", block.Site, block.Type, owner)
	}
	if block.Unscoped && len(block.Dialects) == 0 {
		return fmt.Errorf("HCL block %q of %s selects unscoped declarations without a dialect", block.Type, owner)
	}
	for _, dialect := range block.Dialects {
		if strings.TrimSpace(dialect) == "" {
			return fmt.Errorf("HCL block %q of %s declares an empty dialect", block.Type, owner)
		}
	}
	return nil
}

func (s *Set) claimBlock(index int, owner string, block Block) error {
	key := blockKey{site: block.Site, block: block.Type}
	routes, found := s.blocks[key]
	if !found {
		routes = &blockRoutes{shapes: make(map[int]Block), scopes: targetscope.New(), universal: -1}
		s.blocks[key] = routes
	}
	previous, conflict := routes.claim(index, block)
	if conflict {
		previousOwner := owner
		if previous < len(s.extensions) {
			previousOwner = s.extensions[previous].Owner
		}
		return fmt.Errorf("%w: HCL %s block %q is read by %s and %s", schemaext.ErrDuplicate, block.Site, block.Type, previousOwner, owner)
	}
	routes.shapes[index] = block
	return nil
}

func (r *blockRoutes) claim(index int, block Block) (int, bool) {
	if _, duplicate := r.shapes[index]; duplicate {
		return index, true
	}
	if r.universal >= 0 {
		return r.universal, true
	}
	if !scopedBlock(block) {
		first := index
		for previous := range r.shapes {
			first = min(first, previous)
		}
		if first < index {
			return first, true
		}
		r.universal = index
		return 0, false
	}
	conflict, taken := r.scopes.Claim(index, targetscope.Scope{Targets: block.Dialects, Unscoped: block.Unscoped})
	return conflict.Owner, taken
}

func (r *blockRoutes) owner(dialect string) (int, bool) {
	if r.universal >= 0 {
		return r.universal, true
	}
	var targets []string
	if dialect != "" {
		targets = []string{dialect}
	}
	index, found, _ := r.scopes.Owner(targets)
	return index, found
}

// DeclaresType reports whether any selected owner uses a top-level block to
// declare types. References may name a type from another file or from a block
// with an explicit dialect, independently of this view's default dialect.
func (s Set) DeclaresType(block string) bool {
	routes, found := s.blocks[blockKey{site: TopLevel, block: block}]
	if !found {
		return false
	}
	for _, shape := range routes.shapes {
		if shape.DeclaresType {
			return true
		}
	}
	return false
}

// bindEncodedDialects gives a scoped block a reader that survives export to
// a caller with another default dialect. Encoders cannot select another
// owner's shape or put a discriminator on an unscoped block.
func (s Set) bindEncodedDialects(index int, blocks []EncodedBlock) error {
	for i := range blocks {
		block := &blocks[i]
		if block.Unwritten {
			continue
		}
		routes, found := s.blocks[blockKey{site: TopLevel, block: block.Type}]
		if !found || routes.universal >= 0 || block.Table != nil {
			if block.Dialect != "" {
				return fmt.Errorf("%w: HCL block %q of %s writes a dialect without a scoped reader", schemaext.ErrInvalidValue, block.Type, s.extensions[index].Owner)
			}
			continue
		}
		for _, attribute := range block.Attributes {
			if attribute.Name == "dialect" {
				return fmt.Errorf("%w: HCL block %q of %s writes the reserved dialect attribute", schemaext.ErrInvalidValue, block.Type, s.extensions[index].Owner)
			}
		}
		shape, owned := routes.shapes[index]
		if !owned {
			return fmt.Errorf("%w: HCL encoder of %s writes scoped block %q without owning a reader", schemaext.ErrInvalidValue, s.extensions[index].Owner, block.Type)
		}
		if block.Dialect == "" {
			block.Dialect = shape.Dialects[0]
		}
		block.Dialect = targetscope.Normalize(block.Dialect)
		reader, selected := routes.owner(block.Dialect)
		if !selected || reader != index {
			return fmt.Errorf("%w: HCL block %q of %s selects another reader with dialect %q", schemaext.ErrInvalidValue, block.Type, s.extensions[index].Owner, block.Dialect)
		}
	}
	return nil
}
