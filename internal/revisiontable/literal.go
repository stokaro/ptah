package revisiontable

import (
	"encoding/hex"
	"strconv"
	"strings"

	"ptah.run/core/platform"
)

// VersionLiteral renders an exact Atlas revision identity as a SQL literal
// whose bytes do not depend on a connection's string-escape mode.
func VersionLiteral(dialect, value string) string {
	normalizedDialect := platform.NormalizeDialect(dialect)
	switch normalizedDialect {
	case platform.MySQL, platform.MariaDB:
		// A hexadecimal binary literal is independent of sql_mode. Escaping a
		// backslash as `\\` works under MySQL's default mode but changes the
		// value when NO_BACKSLASH_ESCAPES is enabled.
		return "X'" + hex.EncodeToString([]byte(value)) + "'"
	case platform.ClickHouse:
		value = strings.ReplaceAll(value, `\`, `\\`)
	case platform.SQLServer:
		return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
	case platform.YDB:
		return yqlUtf8Literal(value)
	}
	if platform.IsPostgresFamily(normalizedDialect) && strings.Contains(value, `\`) {
		// Dollar-quoted content is literal regardless of PostgreSQL's
		// standard_conforming_strings setting. Pick a delimiter absent from the
		// revision token so the token cannot terminate its own SQL literal.
		return postgresDollarQuotedLiteral(value)
	}
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// yqlEscaper escapes what would end or change a YQL string literal. YQL's
// default lexer reads backslash escapes in every string, and a doubled quote
// is not an escape there: it ends one literal and opens the next.
var yqlEscaper = strings.NewReplacer(`\`, `\\`, "'", `\'`)

// yqlUtf8Literal renders value as a typed YQL Utf8 literal. The u suffix is
// the type: decision 11 of stokaro/ptah#4015 types every YQL data literal. Measured on YDB 26.2.1.14, '20260103_a\\b\'c'u reads back as
// 20260103_a\b'c of type Utf8.
func yqlUtf8Literal(value string) string {
	return "'" + yqlEscaper.Replace(value) + "'u"
}

func postgresDollarQuotedLiteral(value string) string {
	tag := "$ptah$"
	for suffix := 1; strings.Contains(value, tag); suffix++ {
		tag = "$ptah" + strconv.Itoa(suffix) + "$"
	}
	return tag + value + tag
}
