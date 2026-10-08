package generator

import (
	"fmt"
	"slices"

	"ptah.run/migration/schemadiff/difftypes"
)

func (p *indexProjection) comments(changes []difftypes.IndexCommentChange, removals []difftypes.IndexRef) error {
	for _, change := range changes {
		if !p.owns(change.TableName) {
			continue
		}
		position := p.position(change.Name)
		if position < 0 {
			// Some targets retain an index's comment after dropping the
			// index. Clearing it is part of removal, not a missing sibling.
			removed := slices.ContainsFunc(removals, func(ref difftypes.IndexRef) bool {
				return p.owns(ref.TableName) && p.semantics.IndexIdentityKey(ref.Name) == p.semantics.IndexIdentityKey(change.Name)
			})
			if removed && change.From == "" && change.Desired == "" {
				continue
			}
			return fmt.Errorf("cannot project comment of missing index %q on %q", change.Name, p.table)
		}
		p.indexes[position].Comment = change.Desired
		p.indexes[position].Definition = ""
	}
	return nil
}
