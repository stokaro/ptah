package compare

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"

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
// and MariaDB 12.3, `DEFAULT 0x1F` on an int column reports 31, `DEFAULT
// b'101'` and `DEFAULT 0b101` on an int column report 5, and `DEFAULT 0b110`
// on a bit column reports b'110'. Compared as written, each plans the column
// on every apply. Anything else is returned as it is.
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
		if bits, ok := bitDigits(trimmed); ok {
			if number, err := strconv.ParseUint(bits, 2, 64); err == nil {
				return strconv.FormatUint(number, 10)
			}
		}
	}
	return value
}

// bitDigits returns the binary digits of a bit literal, b'101' or 0b101, in
// lower case.
func bitDigits(value string) (string, bool) {
	if _, hexadecimal := defaultlit.HexString(value); hexadecimal || !defaultlit.IsBinaryString(value) {
		return "", false
	}
	bits := strings.ToLower(value)
	if rest, found := strings.CutPrefix(bits, "0b"); found {
		return rest, true
	}
	return bits[2 : len(bits)-1], true
}

// textDefault writes a hexadecimal or bit literal on a MySQL-family column
// that stores text as the string of its bytes, quoted the way the MySQL reader
// reports a string: each backslash and each quote doubled. Measured on MySQL
// 26.10 and MariaDB 12.3, `DEFAULT 0x41` on a varchar stores the string A, and
// so do x'41', b'1000001' and 0b1000001; `0x5C27` stores a backslash and a
// quote. Compared as written, each plans the column on every
// apply. Bytes that are no UTF-8 text, and anything else, are returned as they
// are.
func textDefault(value string) string {
	content, ok := literalBytes(normalize.Expression(strings.TrimSpace(value)))
	if !ok || !utf8.Valid(content) {
		return value
	}
	return "'" + strings.ReplaceAll(strings.ReplaceAll(string(content), `\`, `\\`), "'", "''") + "'"
}

// literalBytes returns the bytes a hexadecimal or bit literal spells. A bit
// literal whose length is not a whole number of bytes is padded on the left
// with zeros, as MySQL pads it.
func literalBytes(value string) ([]byte, bool) {
	if canonical, ok := defaultlit.HexString(value); ok {
		decoded, err := hex.DecodeString(canonical[2 : len(canonical)-1])
		return decoded, err == nil
	}
	bits, ok := bitDigits(value)
	if !ok {
		return nil, false
	}
	bits = strings.Repeat("0", (8-len(bits)%8)%8) + bits
	content := make([]byte, 0, len(bits)/8)
	for start := 0; start < len(bits); start += 8 {
		octet, err := strconv.ParseUint(bits[start:start+8], 2, 8)
		if err != nil {
			return nil, false
		}
		content = append(content, byte(octet))
	}
	return content, true
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
