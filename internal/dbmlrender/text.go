package dbmlrender

import "strings"

// qualified renders an object's identity, schema-qualified when it has one.
func qualified(schema, name string) string {
	if schema == "" {
		return quote(name)
	}
	return quote(schema) + "." + quote(name)
}

// quote writes an identifier in double quotes, always.
//
// Unconditionally, because the alternative is a rule about which names are safe
// bare, and a name that is safe today stops being safe when the format grows a
// keyword. An embedded quote is doubled, which is what the format reads back.
func quote(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

// quoteNote writes a note or a literal default in single quotes.
//
// A value with a newline takes the triple-quoted form, which is the only one
// that can hold one. A backslash and a single quote are escaped so the value
// reads back as itself.
func quoteNote(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	if strings.ContainsAny(value, "\n\r") {
		return "'''" + strings.ReplaceAll(escaped, "'''", `\'\'\'`) + "'''"
	}
	return "'" + strings.ReplaceAll(escaped, `'`, `\'`) + "'"
}
