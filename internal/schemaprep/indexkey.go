package schemaprep

import "ptah.run/core/schemamodel"

// IndexKeyParts returns the key parts a declared index is built from, in
// order: each structured part's expression, or its column when it has none, and
// the field list when the index declares no structured parts. The renderer's
// lowering and a planner that rebuilds the index both read the key through it,
// so the index a plan adds is the index the declaration renders.
func IndexKeyParts(index schemamodel.Index) []string {
	if len(index.Parts) == 0 {
		return index.Fields
	}
	parts := make([]string, 0, len(index.Parts))
	for _, part := range index.Parts {
		if part.Expr != "" {
			parts = append(parts, part.Expr)
			continue
		}
		parts = append(parts, part.Name)
	}
	return parts
}
