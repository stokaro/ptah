package compare

import (
	"strings"

	"github.com/stokaro/ptah/core/platform"
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

func stripSQLQualifiers(value, schema string) string {
	schema = strings.ToLower(strings.TrimSpace(schema))
	if schema != "" {
		value = stripMatchingSchemaQualifiers(value, schema)
	}
	return stripSinglePartQualifiers(value)
}

func stripMatchingSchemaQualifiers(value, schema string) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] == '\'' || value[i] == '"' {
			i = copyQuotedSQL(&b, value, i)
			continue
		}
		if strings.HasPrefix(value[i:], schema+".") && startsIdentifierAt(value, i) {
			i += len(schema)
			continue
		}
		b.WriteByte(value[i])
	}
	return b.String()
}

func stripSinglePartQualifiers(value string) string {
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
		if i < len(value) && value[i] == '.' && canStripSinglePartQualifier(value, start, i) {
			continue
		}
		b.WriteString(value[start:i])
		i--
	}
	return b.String()
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
	nextEnd := nextStart
	for nextEnd < len(value) && isSQLIdentifierByte(value[nextEnd]) {
		nextEnd++
	}
	return nextEnd >= len(value) || value[nextEnd] != '.'
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
