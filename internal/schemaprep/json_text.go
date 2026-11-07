package schemaprep

import (
	"strings"

	"ptah.run/core/schemamodel"
)

// JSONHeldAsText reports whether declaredType is a JSON column on a dialect that holds one
// as a LONGTEXT with a CHECK of json_valid. Measured on MariaDB 12.3, a column built as
// `json` reads back as `longtext` with the CHECK under the column's own name.
//
// The renderer writes that shape for the same declaration, and the comparison has to read
// it as that declaration: taken as words, the plan rewrites the column and drops the CHECK
// on every run, and the server refuses the drop once the rewrite has replaced it (1091).
func JSONHeldAsText(declaredType, dialect string) bool {
	return describe(dialect).preparation.JSONHeldAsText && strings.EqualFold(strings.TrimSpace(declaredType), "json")
}

// NeedsJSONCheck reports whether a column declared with declaredType and declaredCheck
// takes the CHECK of json_valid its type implies, which it does unless it states one.
func NeedsJSONCheck(declaredType, declaredCheck, dialect string) bool {
	return JSONHeldAsText(declaredType, dialect) && !strings.Contains(strings.ToLower(declaredCheck), "json_valid(")
}

// HoldsJSONText reports whether a column of currentType already has the shape a JSON column
// is held in on dialect, so that only its CHECK is left to compare. A catalog reads
// `longtext`, and a second document says `json`. On any other type the statement that
// changes it writes the CHECK with the type.
func HoldsJSONText(currentType, dialect string) bool {
	current := strings.TrimSpace(currentType)
	return JSONHeldAsText(current, dialect) || describe(dialect).preparation.JSONHeldAsText && strings.EqualFold(current, "longtext")
}

// WithJSONCheck is field with the CHECK its type implies on dialect, as a read of the column
// the renderer builds reports it, and field as it stands otherwise.
func WithJSONCheck(field schemamodel.Field, dialect string) schemamodel.Field {
	if field.Check == "" && JSONHeldAsText(field.Type, dialect) {
		field.Check = "json_valid(`" + field.Name + "`)"
	}
	return field
}
