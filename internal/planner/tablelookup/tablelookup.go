// Package tablelookup resolves structural table references for migration planners.
package tablelookup

import (
	"strings"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/tableref"
)

// ResolveReference returns the canonical identity of a referenced table when
// the reference can be resolved without guessing.
func ResolveReference(tables []schemamodel.Table, current schemamodel.Table, reference string) string {
	ref, ok := tableref.Parse(reference)
	if !ok {
		return reference
	}
	if ref.Qualified {
		return schemamodel.QualifyTableName(ref.Schema, ref.Name)
	}

	currentSchema := strings.TrimSpace(current.Schema)
	for _, table := range tables {
		if strings.TrimSpace(table.Schema) == currentSchema && table.Name == ref.Name {
			return table.QualifiedName()
		}
	}

	match := ""
	for _, table := range tables {
		if table.Name != ref.Name {
			continue
		}
		if match != "" {
			return reference
		}
		match = table.QualifiedName()
	}
	if match != "" {
		return match
	}
	return reference
}
