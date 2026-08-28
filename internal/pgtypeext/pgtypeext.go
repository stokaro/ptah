// Package pgtypeext answers which PostgreSQL extension provides a type.
//
// A column declared `vector(384)` is a declaration that pgvector is needed, the
// same way a foreign key is a declaration that another table is. Something has
// to know that, and the place that needs it most -- schema comparison -- runs
// against a description that may have come from a file, with no catalog to ask.
//
// So the answer is written down here, and a live test asks a real server the
// same question and fails when the two disagree. The package exists rather than
// a map inside the comparator because the test that can measure it lives in the
// integration tree, which cannot import a package under
// migration/schemadiff/internal.
package pgtypeext

import (
	"maps"
	"strings"
)

// byType names the extension that provides a type.
//
// Deliberately short. Every entry has to be installable where the control runs,
// because an entry nothing verifies is worse than an absent one: it reads as
// handled. Adding one means adding it to the control's fixture too.
var byType = map[string]string{
	"vector":    "vector",
	"halfvec":   "vector",
	"sparsevec": "vector",
	"hstore":    "hstore",
	"citext":    "citext",
	"ltree":     "ltree",
}

// ExtensionFor reports which extension provides a declared column type.
//
// The declaration is reduced to the name a catalog uses first, so a modifier or
// an array marker does not hide the answer.
func ExtensionFor(declared string) (string, bool) {
	extension, found := byType[BaseTypeName(declared)]
	return extension, found
}

// Types returns what this package claims, for the control that measures it.
func Types() map[string]string {
	return maps.Clone(byType)
}

// BaseTypeName reduces a declared type to the name a catalog would call it.
//
// `vector(384)` is the type `vector` with a modifier, and `VECTOR(384)[]` is an
// array of it. Neither changes which extension provides the type, and comparing
// the whole spelling would miss every column that carries one.
//
// A LONGER name is left alone. `vector_id` is a different type, and trimming
// toward a known one would keep an extension alive for a column that has
// nothing to do with it -- and make that extension undroppable with no way for
// the author to say otherwise.
func BaseTypeName(declared string) string {
	name := strings.TrimSpace(declared)
	name = strings.TrimSuffix(name, "[]")
	if open := strings.IndexByte(name, '('); open >= 0 {
		name = name[:open]
	}
	return strings.ToLower(strings.TrimSpace(name))
}
