package compare

import (
	"strings"

	"go.5x5.cz/ptah/core/platform"
)

func isMySQLFamilyDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

func replaceSQLFunctionOutsideSingleQuotedSQL(value, old, replacement string) string {
	return replaceOutsideSingleQuotedSQL(value, old, replacement, func(value string, start int) bool {
		return start == 0 || !isSQLIdentifierByte(value[start-1])
	})
}

func replaceSQLLiteralOutsideSingleQuotedSQL(value, old, replacement string) string {
	return replaceOutsideSingleQuotedSQL(value, old, replacement, func(value string, start int) bool {
		end := start + len(old)
		return end == len(value) || !isSQLIdentifierByte(value[end])
	})
}

func replaceOutsideSingleQuotedSQL(
	value string,
	old string,
	replacement string,
	allowed func(value string, start int) bool,
) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] == '\'' || value[i] == '"' {
			i = copyQuotedSQL(&b, value, i)
			continue
		}
		if strings.HasPrefix(value[i:], old) && allowed(value, i) {
			b.WriteString(replacement)
			i += len(old) - 1
			continue
		}
		b.WriteByte(value[i])
	}
	return b.String()
}

func normalizeSQLCaseAndIdentifierQuotes(value, dialect string) string {
	var b strings.Builder
	mysqlFamily := isMySQLFamilyDialect(dialect)
	for i := 0; i < len(value); i++ {
		switch ch := value[i]; {
		case ch == '\'' || (mysqlFamily && ch == '"'):
			i = copyQuotedSQL(&b, value, i)
		case ch == '`' || ch == '"':
			i = copyIdentifierQuote(&b, value, i)
		default:
			b.WriteByte(lowerASCII(ch))
		}
	}
	return b.String()
}

func stripSQLQualifiers(value, schema string, authored map[string]struct{}) string {
	schema = strings.ToLower(strings.TrimSpace(schema))
	if schema != "" {
		value = stripMatchingSchemaQualifiers(value, schema)
	}
	return stripSinglePartQualifiers(value, authored)
}

// stripMatchingSchemaQualifiers removes the object's own schema from the places
// a catalog puts it: in front of a relation, and in front of the table half of a
// three-part column reference.
//
// It is deliberately not "every occurrence of this name followed by a dot". A
// relation alias may be spelled the same as the database it lives in --
// `SELECT analytics.id FROM users AS analytics` in database `analytics` is legal
// -- and removing that prefix here would take it off the readback while the
// declaration kept it, reporting a change on an object nobody edited. On
// ClickHouse that reads as a body change and is planned as a drop and a create,
// which discards the view's accumulated rows.
func stripMatchingSchemaQualifiers(value, schema string) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] == '\'' || value[i] == '"' {
			i = copyQuotedSQL(&b, value, i)
			continue
		}
		if strings.HasPrefix(value[i:], schema+".") &&
			startsIdentifierAt(value, i) &&
			qualifiesRelationAt(value, i, i+len(schema)) {
			i += len(schema)
			continue
		}
		b.WriteByte(value[i])
	}
	return b.String()
}

// walkSQLQualifiers rewrites value, calling keep for every identifier that is
// directly followed by a dot outside quoted text. Returning false drops that
// identifier and its dot; returning true copies both through.
//
// One traversal serves all three questions asked about qualifiers -- which ones
// a body carries, which ones a readback may lose, and whether a body qualifies a
// relation -- so the three answers cannot drift apart.
func walkSQLQualifiers(value string, keep func(start, dot int, name string) bool) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] == '\'' || value[i] == '"' {
			i = copyQuotedSQL(&b, value, i)
			continue
		}
		if !isSQLIdentifierByte(value[i]) {
			b.WriteByte(value[i])
			continue
		}
		start := i
		for i < len(value) && isSQLIdentifierByte(value[i]) {
			i++
		}
		if i < len(value) && value[i] == '.' && !keep(start, i, value[start:i]) {
			continue
		}
		b.WriteString(value[start:i])
		i--
	}
	return b.String()
}

// stripSinglePartQualifiers removes the one-part qualifiers a catalog adds to
// column references, keeping any name the authored body used as a qualifier
// itself.
//
// The strip exists because MySQL rewrites every column of a view body as
// `db`.`table`.`column`, which no declaration spells. It must not run on a
// qualifier the declaration does carry: measured on ClickHouse 26.7.3.19, a body
// authored as `SELECT u.id AS id FROM users AS u` reads back as
// `SELECT u.id AS id FROM wf9d_alias.users AS u`, so the alias is present on
// both sides and only the relation gained a qualifier. Stripping `u.` from the
// readback alone left the two sides unequal and reported a body change on a
// declaration nobody had touched.
//
// Keeping only the authored names is what separates that from a real change: a
// readback qualifier the declaration does not use is still stripped, so
// `SELECT a.id` against a readback of `SELECT b.id` stays a difference.
func stripSinglePartQualifiers(value string, authored map[string]struct{}) string {
	return walkSQLQualifiers(value, func(start, dot int, name string) bool {
		if !canStripSinglePartQualifier(value, start, dot) {
			return true
		}
		_, used := authored[name]
		return used
	})
}

// singlePartQualifierNames returns the one-part qualifiers value uses on
// columns -- aliases and table prefixes, not schemas.
func singlePartQualifierNames(value string) map[string]struct{} {
	names := make(map[string]struct{})
	walkSQLQualifiers(value, func(start, dot int, name string) bool {
		if canStripSinglePartQualifier(value, start, dot) {
			names[name] = struct{}{}
		}
		return true
	})
	return names
}

// bodyQualifiesRelation reports whether value names a relation with a qualifier
// of its own -- either directly after a relation introducer, or as the first
// part of a three-part name.
//
// This is the question "did the author write a schema qualifier", and it is the
// only reason to refuse the qualifier-stripping comparison: an authored
// qualifier is part of the declaration, so a readback that spells it differently
// is a real difference. A column prefix is not that. `SELECT u.id FROM users AS
// u` carries `u.`, and treating that as an authored schema made an unchanged
// materialized view read as drift -- which on ClickHouse plans a drop and a
// create and destroys the rows the view had accumulated.
func bodyQualifiesRelation(value string) bool {
	qualified := false
	walkSQLQualifiers(value, func(start, dot int, _ string) bool {
		if qualifiesRelationAt(value, start, dot) {
			qualified = true
		}
		return true
	})
	return qualified
}

func qualifiesRelationAt(value string, start, dot int) bool {
	if start > 0 && value[start-1] == '.' {
		return false
	}
	if previousSQLWordIsRelationIntroducer(value, start) {
		return true
	}
	return nextQualifierPartIsQualified(value, dot)
}

func canStripSinglePartQualifier(value string, start, dot int) bool {
	if start > 0 && value[start-1] == '.' {
		return false
	}
	if previousSQLWordIsRelationIntroducer(value, start) {
		return false
	}
	nextStart := dot + 1
	if nextStart >= len(value) || !isSQLIdentifierByte(value[nextStart]) {
		return false
	}
	return !nextQualifierPartIsQualified(value, dot)
}

// nextQualifierPartIsQualified reports whether the identifier after dot is
// itself followed by a dot, which makes the qualifier before dot the first part
// of a three-part name and therefore a schema.
func nextQualifierPartIsQualified(value string, dot int) bool {
	nextStart := dot + 1
	if nextStart >= len(value) || !isSQLIdentifierByte(value[nextStart]) {
		return false
	}
	nextEnd := nextStart
	for nextEnd < len(value) && isSQLIdentifierByte(value[nextEnd]) {
		nextEnd++
	}
	return nextEnd < len(value) && value[nextEnd] == '.'
}

func previousSQLWordIsRelationIntroducer(value string, start int) bool {
	end := start
	for end > 0 && isSQLWhitespace(value[end-1]) {
		end--
	}
	start = end
	for start > 0 && isSQLIdentifierByte(value[start-1]) {
		start--
	}
	switch value[start:end] {
	case "from", "join", "update", "into", "table":
		return true
	default:
		return false
	}
}

func startsIdentifierAt(value string, start int) bool {
	return (start == 0 || !isSQLIdentifierByte(value[start-1])) &&
		start+1 < len(value) && isSQLIdentifierByte(value[start])
}

func collapseWhitespaceOutsideQuotedSQL(value string) string {
	var b strings.Builder
	inWhitespace := false
	for i := 0; i < len(value); i++ {
		if value[i] == '\'' || value[i] == '"' {
			i = copyQuotedSQL(&b, value, i)
			inWhitespace = false
			continue
		}
		if isSQLWhitespace(value[i]) {
			inWhitespace = true
			continue
		}
		if inWhitespace && b.Len() > 0 {
			b.WriteByte(' ')
		}
		inWhitespace = false
		b.WriteByte(value[i])
	}
	return strings.TrimSpace(b.String())
}

func normalizeCommaSpacingOutsideQuotedSQL(value string) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] == '\'' || value[i] == '"' {
			i = copyQuotedSQL(&b, value, i)
			continue
		}
		if value[i] != ',' {
			b.WriteByte(value[i])
			continue
		}
		trimTrailingSpaces(&b)
		b.WriteByte(',')
		for i+1 < len(value) && isSQLWhitespace(value[i+1]) {
			i++
		}
	}
	return b.String()
}

func trimTrailingSpaces(b *strings.Builder) {
	value := strings.TrimRight(b.String(), " \t\n\r")
	b.Reset()
	b.WriteString(value)
}

func copyQuotedSQL(b *strings.Builder, value string, start int) int {
	quote := value[start]
	b.WriteByte(value[start])
	for i := start + 1; i < len(value); i++ {
		b.WriteByte(value[i])
		if value[i] == '\\' && i+1 < len(value) {
			i++
			b.WriteByte(value[i])
			continue
		}
		if value[i] != quote {
			continue
		}
		if i+1 < len(value) && value[i+1] == quote {
			i++
			b.WriteByte(value[i])
			continue
		}
		return i
	}
	return len(value) - 1
}

func copyIdentifierQuote(b *strings.Builder, value string, start int) int {
	quote := value[start]
	for i := start + 1; i < len(value); i++ {
		if value[i] == quote {
			if i+1 < len(value) && value[i+1] == quote {
				i++
				b.WriteByte(lowerASCII(value[i]))
				continue
			}
			return i
		}
		b.WriteByte(lowerASCII(value[i]))
	}
	return len(value) - 1
}

func isSQLIdentifierByte(ch byte) bool {
	return ch == '_' ||
		(ch >= '0' && ch <= '9') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z')
}

func isSQLWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func lowerASCII(ch byte) byte {
	if ch >= 'A' && ch <= 'Z' {
		return ch + ('a' - 'A')
	}
	return ch
}
