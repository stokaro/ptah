package compare

import (
	"encoding/hex"
	"strconv"
	"strings"

	"ptah.run/internal/defaultlit"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/normalize"
)

// storesBytes reports whether a MySQL-family column of typeName stores bytes
// rather than text.
func storesBytes(typeName string) bool {
	name := strings.ToLower(typeName)
	return strings.Contains(name, "binary") || strings.Contains(name, "blob")
}

// numericLiteralDefault writes a hexadecimal or bit literal on a MySQL-family
// integer or bit column the way the server reports it. Measured on MySQL 26.10
// and MariaDB 12.3, `DEFAULT 0x1F` on an int column reports 31, and
// `DEFAULT 0b110` on a bit column reports b'110'. Compared as written, either
// plans the column on every apply. Anything else is returned as it is.
func numericLiteralDefault(value, typeName string) string {
	name := strings.ToLower(strings.TrimSpace(typeName))
	trimmed := strings.ToLower(normalize.Expression(strings.TrimSpace(value)))
	switch {
	case strings.HasPrefix(name, "bit"):
		if bits, found := strings.CutPrefix(trimmed, "0b"); found && bits != "" && strings.Trim(bits, "01") == "" {
			return "b'" + bits + "'"
		}
	case strings.Contains(name, "int"):
		if canonical, ok := defaultlit.HexString(trimmed); ok {
			if number, err := strconv.ParseUint(canonical[2:len(canonical)-1], 16, 64); err == nil {
				return strconv.FormatUint(number, 10)
			}
		}
	}
	return value
}

// binaryDefault writes a default of a column that stores bytes as the
// hexadecimal string of those bytes, x'..' with lower-case digits. A string
// and the hexadecimal string of its bytes are one default there: measured on
// MariaDB 12.3, `DEFAULT ('xy')` on a varbinary reports `x'7879'`, and MySQL
// 26.10 reports `DEFAULT 'xy'` as `0x7879`. Compared as written, either plans
// the column on every apply. A character set introducer before the string,
// which MySQL writes into the expression it reports, is dropped. Anything
// else is returned as it is.
func binaryDefault(value, dialect string) string {
	trimmed := normalize.Expression(strings.TrimSpace(value))
	if canonical, ok := defaultlit.HexString(trimmed); ok {
		return canonical
	}
	if strings.HasPrefix(trimmed, "_") {
		quote := strings.IndexByte(trimmed, '\'')
		if quote < 0 || strings.Trim(strings.ToLower(trimmed[1:quote]), "abcdefghijklmnopqrstuvwxyz0123456789_") != "" {
			return value
		}
		trimmed = trimmed[quote:]
	}
	if !strings.HasPrefix(trimmed, "'") {
		return value
	}
	text, ok := lexer.StringValue(trimmed, dialectlexer.Options(dialect))
	if !ok {
		return value
	}
	return "x'" + hex.EncodeToString([]byte(text)) + "'"
}
