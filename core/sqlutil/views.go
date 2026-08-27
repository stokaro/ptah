package sqlutil

import "strings"

// CheckOptionRequestsCheck reports whether a CHECK_OPTION a catalog reported
// asks for `WITH CHECK OPTION` on the view.
//
// The catalog reports a word where the model has a bool. The words are NONE,
// LOCAL and CASCADED, and a reader that has no column to read leaves it empty:
// Oracle's `all_views` carries the option as a constraint rather than a column,
// so its views arrive with none at all. Both of those mean the view has no
// check option, and every other word asks for one.
//
// It is stated as "not one of the two ways of saying no" rather than as a list
// of the words that say yes, because the catalog's own documentation admits a
// dialect equivalent and a list of yes-words would answer `false` for a word it
// had not been told about -- silently dropping a clause the view declares
// (stokaro/ptah#2315).
func CheckOptionRequestsCheck(checkOption string) bool {
	value := strings.TrimSpace(checkOption)
	return value != "" && !strings.EqualFold(value, "NONE")
}
