package schemaprep

import "ptah.run/core/schemamodel"

// IndexKey returns the parts a declared index is built from, in order: its
// structured parts, or each field as a column part when it declares none. A
// part keeps whether it names a column or holds an expression, which a target
// that quotes a column name and writes an expression as it is needs to know.
func IndexKey(index schemamodel.Index) []schemamodel.IndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]schemamodel.IndexPart, len(index.Fields))
	for position, field := range index.Fields {
		parts[position] = schemamodel.IndexPart{Name: field}
	}
	return parts
}

// IndexKeyParts returns the key parts a declared index is built from, in
// order: each part of [IndexKey] as its expression, or its column when it has
// none. The renderer's lowering and a planner that rebuilds the index both read
// the key through it, so the index a plan adds is the index the declaration
// renders.
func IndexKeyParts(index schemamodel.Index) []string {
	var parts []string
	for _, part := range IndexKey(index) {
		if part.Expr != "" {
			parts = append(parts, part.Expr)
			continue
		}
		parts = append(parts, part.Name)
	}
	return parts
}
