package compare

import (
	"strings"

	"ptah.run/core/schemamodel"
)

// relationColumns answers the columns a relation declares, in declaration
// order, by the name a view body gives it. It is what a `*` in a declared body
// stands for.
type relationColumns func(relation string) ([]string, bool)

// declaredRelationColumns reads the desired tables' columns into a
// [relationColumns]. A relation is found by the name it is declared with, or by
// its bare name when exactly one declared table has that name.
//
// The desired schema is the right source, not the database. A `*` is expanded
// when the view is created, so a declaration reading `SELECT * FROM orders`
// asks for every column the desired `orders` has. A view created before
// `orders` gained a column holds one column fewer, and that is a real
// difference: re-creating the view would add the column.
func declaredRelationColumns(desired *schemamodel.Database) relationColumns {
	if desired == nil {
		return nil
	}
	byStruct := make(map[string][]string, len(desired.Tables))
	for _, field := range desired.Fields {
		byStruct[field.StructName] = append(byStruct[field.StructName], strings.ToLower(field.Name))
	}
	byName := make(map[string][]string, len(desired.Tables))
	bareCount := make(map[string]int, len(desired.Tables))
	byBare := make(map[string][]string, len(desired.Tables))
	for _, table := range desired.Tables {
		columns := byStruct[table.StructName]
		name := strings.ToLower(table.Name)
		byName[name] = columns
		if table.Schema != "" {
			byName[strings.ToLower(table.Schema)+"."+name] = columns
		}
		bare := name
		if _, after, qualified := strings.Cut(name, "."); qualified {
			bare = after
		}
		bareCount[bare]++
		byBare[bare] = columns
	}
	return func(relation string) ([]string, bool) {
		relation = strings.ToLower(strings.ReplaceAll(relation, `"`, ""))
		if columns, ok := byName[relation]; ok && len(columns) > 0 {
			return columns, true
		}
		bare := relation
		if _, after, qualified := strings.Cut(relation, "."); qualified {
			bare = after
		}
		if bareCount[bare] != 1 || len(byBare[bare]) == 0 {
			return nil, false
		}
		return byBare[bare], true
	}
}

// selectStarStopWords end the FROM item: after the relation and its alias,
// one of these starts the next clause. A word that is not one of them in the
// alias position is an alias.
var selectStarStopWords = map[string]bool{
	"where": true, "group": true, "having": true, "window": true, "order": true,
	"limit": true, "offset": true, "fetch": true, "for": true,
}

// selectStarRefusedWords continue the FROM clause with a second relation, which
// is where a `*` stops being the columns of one table. None of them can be an
// alias, and one in the alias position refuses the expansion.
var selectStarRefusedWords = map[string]bool{
	"join": true, "inner": true, "left": true, "right": true, "full": true,
	"cross": true, "natural": true, "lateral": true, "tablesample": true,
}

// expandSelectStar replaces each `*` item of a single-relation SELECT with the
// relation's declared columns, so a declared body can be compared with the one
// a server stores. Measured on PostgreSQL 18.6, `pg_get_viewdef` reports
// `SELECT * FROM orders WHERE total > 100` as `SELECT id, total FROM orders
// WHERE (total > 100)`, and `SELECT *, 1 AS one FROM orders` as
// `SELECT id, total, 1 AS one FROM orders`.
//
// body is a normalized body: lower case, whitespace collapsed. The answer is
// false for anything this does not recognize -- a join, a subquery in FROM, a
// set operation, a relation the desired schema does not declare -- so the
// comparison falls back to the text as written.
func expandSelectStar(body string, columns relationColumns) (string, bool) {
	if columns == nil {
		return "", false
	}
	rest, ok := strings.CutPrefix(body, "select ")
	if !ok {
		return "", false
	}
	prefix := "select "
	if after, distinct := strings.CutPrefix(rest, "distinct "); distinct {
		prefix, rest = "select distinct ", after
	}
	fromAt := topLevelIndex(rest, " from ")
	if fromAt < 0 {
		return "", false
	}
	items := splitTopLevel(rest[:fromAt], ',')
	fromClause := rest[fromAt+len(" from "):]
	relation, alias, ok := singleFromRelation(fromClause)
	if !ok {
		return "", false
	}
	relationColumns, ok := columns(relation)
	if !ok {
		return "", false
	}

	expanded := make([]string, 0, len(items))
	starFound := false
	for _, item := range items {
		item = strings.TrimSpace(item)
		if isStarItem(item, relation, alias) {
			expanded = append(expanded, relationColumns...)
			starFound = true
			continue
		}
		expanded = append(expanded, item)
	}
	if !starFound {
		return "", false
	}
	return prefix + strings.Join(expanded, ", ") + " from " + fromClause, true
}

// isStarItem reports whether a select item is `*`, or `<relation>.*` or
// `<alias>.*` for the one relation in FROM.
func isStarItem(item, relation, alias string) bool {
	if item == "*" {
		return true
	}
	qualifier, ok := strings.CutSuffix(item, ".*")
	if !ok {
		return false
	}
	qualifier = strings.ReplaceAll(qualifier, `"`, "")
	bare := relation
	if _, after, qualified := strings.Cut(relation, "."); qualified {
		bare = after
	}
	return qualifier == relation || qualifier == bare || (alias != "" && qualifier == alias)
}

// singleFromRelation reads a FROM clause that names exactly one relation, with
// an optional alias, followed by nothing or by one of the clauses in
// [selectStarStopWords]. What those clauses contain is not read: a join inside
// a subquery in WHERE does not change what the top-level `*` stands for.
func singleFromRelation(fromClause string) (relation, alias string, ok bool) {
	fields := strings.Fields(fromClause)
	if len(fields) == 0 || strings.HasPrefix(fields[0], "(") || strings.ContainsAny(fields[0], ",()") {
		return "", "", false
	}
	relation = strings.ReplaceAll(fields[0], `"`, "")
	next := 1
	if next < len(fields) && fields[next] == "as" {
		next++
	}
	if next < len(fields) && !selectStarStopWords[fields[next]] && !selectStarRefusedWords[fields[next]] {
		alias = strings.ReplaceAll(fields[next], `"`, "")
		next++
	}
	if strings.Contains(strings.Join(fields[:next], " "), ",") {
		return "", "", false
	}
	if next < len(fields) && !selectStarStopWords[fields[next]] {
		return "", "", false
	}
	return relation, alias, true
}

// topLevelIndex is the first index of needle in text outside parentheses and
// quotes, or -1.
func topLevelIndex(text, needle string) int {
	depth := 0
	var quote byte
	for i := 0; i < len(text); i++ {
		switch c := text[i]; {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && strings.HasPrefix(text[i:], needle):
			return i
		}
	}
	return -1
}

// splitTopLevel splits text at separator outside parentheses and quotes.
func splitTopLevel(text string, separator byte) []string {
	var parts []string
	depth, start := 0, 0
	var quote byte
	for i := 0; i < len(text); i++ {
		switch c := text[i]; {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && c == separator:
			parts = append(parts, text[start:i])
			start = i + 1
		}
	}
	return append(parts, text[start:])
}
