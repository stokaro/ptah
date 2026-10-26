// Package viewcolumns rewrites a view's SELECT list so its output columns carry
// the names the declaration gives them.
//
// PostgreSQL accepts both spellings and stores one: `CREATE VIEW v (a, b) AS
// SELECT x, y` is kept as `SELECT x AS a, y AS b`, which is what
// pg_get_viewdef reports and what a reader hands the comparison. A declaration
// that rendered the alias list would therefore never match its own catalog row,
// and the view would be planned for replacement on every run
// (stokaro/ptah#3172).
//
// Rewriting the declaration into the spelling the catalog keeps costs nothing a
// caller can observe -- the two statements create the same view -- and leaves
// the body comparison exactly as it was.
package viewcolumns

import (
	"fmt"
	"strings"
	"unicode"
)

// ErrUnreadableSelectList marks a body whose SELECT list this package declines
// to rewrite.
//
// A view is refused rather than rewritten wrongly: a mis-split select list
// produces a view with the author's columns under the wrong names, which no
// diagnostic downstream could report.
var ErrUnreadableSelectList = fmt.Errorf("view body select list cannot be read")

// WithAliases returns body with each top-level select item aliased to the
// matching name.
//
// An item's explicit AS alias is replaced by the name from the view's column
// list. An unreadable alias, an empty name, or a count that does not match the
// select list is refused with [ErrUnreadableSelectList].
func WithAliases(body string, names []string) (string, error) {
	if len(names) == 0 {
		return body, nil
	}
	parts, err := splitSelectList(body)
	if err != nil {
		return "", err
	}
	items := splitTopLevel(parts.list)
	if len(items) != len(names) {
		return "", fmt.Errorf("%w: %d select items for %d declared columns",
			ErrUnreadableSelectList, len(items), len(names))
	}
	aliased := make([]string, 0, len(items))
	for i, item := range items {
		name := strings.TrimSpace(names[i])
		if name == "" {
			return "", fmt.Errorf("%w: column %d has no name", ErrUnreadableSelectList, i+1)
		}
		if isStarItem(item) {
			return "", fmt.Errorf("%w: column %d is a star, which expands to a number of columns the declaration cannot count",
				ErrUnreadableSelectList, i+1)
		}
		rewritten, err := aliasItem(item, name)
		if err != nil {
			return "", fmt.Errorf("column %d: %w", i+1, err)
		}
		aliased = append(aliased, rewritten)
	}
	return joinRewritten(parts, strings.Join(aliased, ", ")), nil
}

// joinRewritten puts the rewritten select list back between the keyword and
// whatever followed the list, with the one space each side needs.
//
// The pieces come back trimmed, so joining them as they are runs `SELECT` into
// the first item and the last item into `FROM`.
func joinRewritten(parts selectParts, list string) string {
	rewritten := strings.TrimSpace(parts.prefix) + " " + list
	if suffix := strings.TrimSpace(parts.suffix); suffix != "" {
		return rewritten + " " + suffix
	}
	return rewritten
}

// isStarItem reports whether item is `*` or `t.*`.
func isStarItem(item string) bool {
	return strings.HasSuffix(strings.TrimSpace(item), "*")
}

// aliasItem gives one select item the declared name. An explicit alias in the
// SELECT is replaced: the view's column list wins over that alias by position.
// Keeping both would produce an invalid `expression AS old AS new` statement.
func aliasItem(item, name string) (string, error) {
	trimmed := strings.TrimSpace(item)
	aliasAt := topLevelKeyword(trimmed, "AS")
	if aliasAt < 0 {
		return trimmed + " AS " + name, nil
	}
	expression := strings.TrimSpace(trimmed[:aliasAt])
	oldAlias := strings.TrimSpace(trimmed[aliasAt+len("AS"):])
	if expression == "" || !isAliasIdentifier(oldAlias) {
		return "", fmt.Errorf("%w: existing AS alias cannot be read", ErrUnreadableSelectList)
	}
	return expression + " AS " + name, nil
}

// isAliasIdentifier accepts one unquoted or quoted SQL identifier after AS.
// A suffix this reader cannot identify is refused instead of being discarded.
func isAliasIdentifier(alias string) bool {
	if alias == "" {
		return false
	}
	switch alias[0] {
	case '"', '`':
		return isQuotedAlias(alias, alias[0], alias[0])
	case '[':
		return isQuotedAlias(alias, '[', ']')
	}
	for i, r := range alias {
		if i == 0 {
			if r != '_' && !unicode.IsLetter(r) {
				return false
			}
			continue
		}
		if r != '_' && r != '$' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func isQuotedAlias(alias string, open, closing byte) bool {
	if len(alias) < 3 || alias[0] != open || alias[len(alias)-1] != closing {
		return false
	}
	for i := 1; i < len(alias)-1; i++ {
		if alias[i] != closing {
			continue
		}
		if i+1 >= len(alias)-1 || alias[i+1] != closing {
			return false
		}
		i++
	}
	return true
}

// selectParts is a body cut into the text before its select list, the list
// itself, and the text after it.
type selectParts struct {
	prefix string
	list   string
	suffix string
}

// splitSelectList cuts a body into its parts.
//
// The list ends at the first top-level FROM, or at the end of the body for a
// SELECT that reads no relation.
func splitSelectList(body string) (selectParts, error) {
	trimmed := strings.TrimSpace(body)
	start := selectKeywordEnd(trimmed)
	if start < 0 {
		return selectParts{}, fmt.Errorf("%w: no SELECT keyword", ErrUnreadableSelectList)
	}
	end := topLevelKeyword(trimmed[start:], "from")
	if end < 0 {
		return selectParts{prefix: trimmed[:start], list: trimmed[start:]}, nil
	}
	return selectParts{
		prefix: trimmed[:start],
		list:   trimmed[start : start+end],
		suffix: trimmed[start+end:],
	}, nil
}

// selectKeywordEnd returns the offset just past the leading SELECT, including
// a DISTINCT or ALL that follows it, or -1 when the body does not start with
// one.
func selectKeywordEnd(body string) int {
	fields := strings.Fields(body)
	if len(fields) == 0 || !strings.EqualFold(fields[0], "select") {
		return -1
	}
	end := len(fields[0])
	end += leadingQuantifierWidth(body[end:], fields)
	return end
}

// leadingQuantifierWidth measures the DISTINCT or ALL that may follow SELECT,
// with the space before it.
func leadingQuantifierWidth(rest string, fields []string) int {
	if len(fields) < 2 {
		return 0
	}
	if !strings.EqualFold(fields[1], "distinct") && !strings.EqualFold(fields[1], "all") {
		return 0
	}
	return strings.Index(rest, fields[1]) + len(fields[1])
}
