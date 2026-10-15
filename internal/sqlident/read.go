package sqlident

import (
	"strings"

	"ptah.run/internal/lexer"
)

// NamesArePaths reports whether dialect names an object by one path, as
// [Qualified] writes a YDB name, rather than by dot-separated parts. Such a
// name is one identifier, so a dot inside it is part of the name: `a.b/c` is
// the object c in the directory a.b. See [SplitPath].
func NamesArePaths(dialect string) bool {
	return spellingOf(dialect).paths
}

// ReadName reads written, one identifier of dialect, into the name it denotes
// by the dialect's own quoting, and reports whether it did. On YDB that is
// YQL's quoting: a backticked name with its backslash escapes decoded, or an
// unquoted name as written, so ReadName undoes [Quote] there. Any other dialect
// returns false, and so does text that is not one YQL identifier, such as an
// empty one or one with text after its closing backtick; a caller reads those
// by the SQL standard's quoting.
func ReadName(dialect, written string) (string, bool) {
	if !NamesArePaths(dialect) {
		return "", false
	}
	return lexer.YQLIdentifierValue(written)
}

// SplitPath splits a path name, as a dialect [NamesArePaths] reports writes
// one, into the directory that holds the object and the object's own name,
// at the last slash. A name with no slash is in no directory.
func SplitPath(name string) (directory, leaf string) {
	if slash := strings.LastIndex(name, "/"); slash >= 0 {
		return name[:slash], name[slash+1:]
	}
	return "", name
}
