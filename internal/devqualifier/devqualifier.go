// Package devqualifier takes the name of a dev database off what a read of it
// reports, for a migration file written from that read. It sits between the
// readers, which record the database an object lives in, and `migrate diff`,
// which writes a file for some other database.
package devqualifier

import (
	"regexp"
	"strings"

	"ptah.run/catalog"
)

// Remove takes the name of the dev database off the objects a read of it
// qualifies with it.
//
// The MySQL-family and ClickHouse readers record the database a view, a routine
// or a sequence lives in as its schema, and the server prints a view's body with
// that database in front of every relation. A file written from that read names
// the dev database, as in DROP VIEW `dev`.`v`, so it cannot run on the database
// it is for: the statement stops there, or, with a guard, does nothing and leaves
// the view behind. A table carries no such name in a read of one database.
//
// A read of several schemas keeps every name, since there the qualifier is what
// tells the objects apart.
func Remove(schema *catalog.Database, database string, readNames []string) {
	if schema == nil || database == "" || len(readNames) != 1 || readNames[0] != database {
		return
	}
	unqualified := func(owner *string) bool {
		if *owner != database {
			return false
		}
		*owner = ""
		return true
	}
	for i := range schema.Views {
		if unqualified(&schema.Views[i].Schema) {
			schema.Views[i].Body = Body(schema.Views[i].Body, database)
		}
	}
	for i := range schema.MatViews {
		if unqualified(&schema.MatViews[i].Schema) {
			schema.MatViews[i].Body = Body(schema.MatViews[i].Body, database)
		}
	}
	for i := range schema.Functions {
		unqualified(&schema.Functions[i].Schema)
	}
	for i := range schema.Sequences {
		unqualified(&schema.Sequences[i].Schema)
	}
}

// Body removes database from in front of the names in body,
// outside string literals. A relation alias may be spelled like the database, as
// in `SELECT app.id FROM users AS app`, and removing that prefix would change
// what the body says, so such a body stays as it is.
func Body(body, database string) string {
	alias := regexp.MustCompile("(?i)\\s(AS\\s+)?[`\"]?" + regexp.QuoteMeta(database) + "[`\"]?(\\s|,|$)")
	if alias.MatchString(body) {
		return body
	}
	quoted := "`" + database + "`."
	bare := database + "."
	var out strings.Builder
	for i := 0; i < len(body); i++ {
		switch {
		case body[i] == '\'':
			end := literalEnd(body, i)
			out.WriteString(body[i:end])
			i = end - 1
		case strings.HasPrefix(body[i:], quoted):
			i += len(quoted) - 1
		case strings.HasPrefix(body[i:], bare) && (i == 0 || !isNameByte(body[i-1])):
			i += len(bare) - 1
		default:
			out.WriteByte(body[i])
		}
	}
	return out.String()
}

// literalEnd is the index just past the string literal that starts at start.
func literalEnd(text string, start int) int {
	for i := start + 1; i < len(text); i++ {
		switch text[i] {
		case '\\':
			i++
		case '\'':
			if i+1 < len(text) && text[i+1] == '\'' {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(text)
}

func isNameByte(b byte) bool {
	return b == '_' || b == '$' || b == '`' || b == '"' || b == '.' ||
		(b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}
