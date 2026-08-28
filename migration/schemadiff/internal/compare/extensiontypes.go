package compare

import (
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/pgtypeext"
)

// extensionsDeclaredTypesNeed reports which extensions the desired schema's own
// column types require.
//
// A column declared `vector(384)` is a declaration that the vector extension is
// needed, the same way a foreign key is a declaration that another table is.
// Before stokaro/ptah#2389 nothing read it that way, so a schema holding such a
// column produced a plan that created the column and dropped the extension its
// type comes from -- one transaction contradicting itself, and on the orderings
// where the drop went first, an extension removed out from under an operator
// who never asked.
func extensionsDeclaredTypesNeed(desired *schemamodel.Database) map[string]bool {
	needed := make(map[string]bool)
	if desired == nil {
		return needed
	}
	for _, field := range desired.Fields {
		if extension, found := pgtypeext.ExtensionFor(field.Type); found {
			needed[extension] = true
		}
	}
	return needed
}
