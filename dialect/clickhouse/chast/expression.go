package chast

import "ptah.run/internal/chindexkey"

// SkippingIndexExpression joins an index's key parts into the expression
// ADD INDEX takes. Several parts form one tuple. A single part is used as
// written unless it is itself a comma-separated list, which is how
// system.data_skipping_indices reports the key of `INDEX i (a, b)`: ADD INDEX
// would read the bare list as the end of its expression, so the list becomes a
// tuple too. Commas inside parentheses, brackets, braces or string literals do
// not count. No parts yield an empty string.
//
// The comparison reads an index's key through the same package, so the key
// the renderer writes is the key it compares.
func SkippingIndexExpression(parts []string) string {
	return chindexkey.Expression(parts)
}
