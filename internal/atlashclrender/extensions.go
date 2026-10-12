package atlashclrender

import "ptah.run/core/hclext"

// renderBeforeTypes writes the blocks the rest of the document may depend
// on, which are the extensions the owner writes, ordered by name, since an
// extension's schema is only where it is installed.
func (r *renderer) renderBeforeTypes() {
	r.renderOwnerBlocks(hclext.BeforeTypes, nil)
}
