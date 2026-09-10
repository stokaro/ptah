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
// An item that already ends in the name it would be given is left alone, so a
// body an author wrote with its aliases is not rewritten into itself twice.
// A name that is empty, or a count that does not match the select list, is
// refused with [ErrUnreadableSelectList].
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
		aliased = append(aliased, aliasItem(item, name))
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

// aliasItem gives one select item its alias, leaving an item that already
// carries it unchanged.
func aliasItem(item, name string) string {
	trimmed := strings.TrimSpace(item)
	if strings.EqualFold(trailingAlias(trimmed), name) {
		return trimmed
	}
	return trimmed + " AS " + name
}

// trailingAlias returns the alias an item already ends with, or the empty
// string. Only the explicit `AS name` spelling counts: a bare trailing word is
// also a valid alias in SQL, but it is indistinguishable from the last token of
// an expression without parsing one.
func trailingAlias(item string) string {
	fields := strings.Fields(item)
	if len(fields) < 2 || !strings.EqualFold(fields[len(fields)-2], "as") {
		return ""
	}
	return strings.Trim(fields[len(fields)-1], `"`)
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
