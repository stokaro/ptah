package pgrole

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/internal/routinesetting"
)

// Setting is one configuration parameter a role's settings name.
//
// Name is the parameter's name without quotes, the parts of a dotted name
// joined by dots, as the server looks it up: statement_timeout, TimeZone,
// app.tenant. The server ignores case when it looks a name up, so two names
// that differ only in case are one setting.
//
// Value is the value in SQL, as `SET name = value` writes it: '30s', 30000,
// '$user', public. A declaration holds the spelling it was written in, its
// elements separated by a comma and a space. An observation holds the text
// pg_db_role_setting.setconfig keeps after the equals sign, unquoted: 30s,
// "$user", public. Reset is a declaration's explicit RESET, and holds no value.
type Setting struct {
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
	Reset bool   `json:"reset,omitempty"`
}

// ParseName reads a parameter name as SQL writes it: identifiers separated by
// dots, each bare or in double quotes. A bare part folds to lower case, as
// the server reads it, and a quoted part keeps its case and loses its quotes.
// It refuses an empty name, an empty part and text that is not a name.
func ParseName(written string) (string, error) {
	trimmed := strings.TrimSpace(written)
	rest := trimmed
	if rest == "" {
		return "", fmt.Errorf("%w: a role setting needs a parameter name", schemaext.ErrInvalidValue)
	}
	var parts []string
	for {
		part, remainder, err := namePart(rest)
		if err != nil {
			return "", fmt.Errorf("%w: role setting name %q: %s", schemaext.ErrInvalidValue, trimmed, err.Error())
		}
		parts = append(parts, part)
		remainder = strings.TrimSpace(remainder)
		if remainder == "" {
			return strings.Join(parts, "."), nil
		}
		if remainder[0] != '.' {
			return "", fmt.Errorf("%w: role setting name %q is not one name", schemaext.ErrInvalidValue, trimmed)
		}
		rest = strings.TrimSpace(remainder[1:])
	}
}

// namePart reads one identifier at the start of text and returns it and what
// follows it.
func namePart(text string) (part, rest string, err error) {
	if strings.HasPrefix(text, `"`) {
		contents, remainder, closed := cutDoubled(text, '"')
		if !closed {
			return "", "", fmt.Errorf("an unclosed quoted part")
		}
		if contents == "" {
			return "", "", fmt.Errorf("an empty quoted part")
		}
		return contents, remainder, nil
	}
	end := 0
	for end < len(text) && isWordByte(text[end], end == 0) {
		end++
	}
	if end == 0 {
		return "", "", fmt.Errorf("a part that is not an identifier")
	}
	return strings.ToLower(text[:end]), text[end:], nil
}

// QuoteName writes name, a name [ParseName] read, in SQL: each part in double
// quotes, so a part keeps its case and no part is read as a keyword. The server
// stores a known parameter under its own spelling either way.
func QuoteName(name string) string {
	parts := strings.Split(name, ".")
	for i, part := range parts {
		parts[i] = `"` + strings.ReplaceAll(part, `"`, `""`) + `"`
	}
	return strings.Join(parts, ".")
}

// Key is the name two settings are matched by: the server ignores case.
func Key(name string) string { return strings.ToLower(name) }

// ValueForm is what a value written in SQL asks for.
type ValueForm int

const (
	// ValueLiteral is a value to store.
	ValueLiteral ValueForm = iota
	// ValueDefault is the bare word DEFAULT, which SQL reads as RESET.
	ValueDefault
)

// ParseValue reads a value written in SQL, as `SET name = value` writes it
// after the equals sign: string literals in single quotes, identifiers bare or
// in double quotes, and numbers with an optional sign, separated by commas.
// It returns the value with its elements separated by a comma and a space,
// each as written except a number, which is written as the server stores it
// (+5000 is 5000, 002 is 2), and the form the value takes. The bare word
// DEFAULT alone is [ValueDefault].
//
// It refuses anything else rather than pass text it does not understand into
// a statement: an escape string (E'...'), a backslash in a string literal, a
// dollar-quoted string, an operator, a number in another radix or with digit
// separators, an empty value and an unclosed quote.
func ParseValue(written string) (string, ValueForm, error) {
	rest := strings.TrimSpace(written)
	if rest == "" {
		return "", ValueLiteral, fmt.Errorf("%w: a role setting needs a value", schemaext.ErrInvalidValue)
	}
	var elements []string
	for {
		element, remainder, err := valueElement(rest)
		if err != nil {
			return "", ValueLiteral, fmt.Errorf("%w: role setting value %q: %s", schemaext.ErrInvalidValue, written, err.Error())
		}
		elements = append(elements, element)
		remainder = strings.TrimSpace(remainder)
		if remainder == "" {
			break
		}
		if remainder[0] != ',' {
			return "", ValueLiteral, fmt.Errorf("%w: role setting value %q: expected a comma after %s", schemaext.ErrInvalidValue, written, element)
		}
		rest = strings.TrimSpace(remainder[1:])
		if rest == "" {
			return "", ValueLiteral, fmt.Errorf("%w: role setting value %q ends in a comma", schemaext.ErrInvalidValue, written)
		}
	}
	if slices.ContainsFunc(elements, func(element string) bool { return strings.EqualFold(element, "DEFAULT") }) {
		if len(elements) > 1 {
			return "", ValueLiteral, fmt.Errorf("%w: role setting value %q: DEFAULT stands alone, never in a list", schemaext.ErrInvalidValue, written)
		}
		return "", ValueDefault, nil
	}
	return strings.Join(elements, ", "), ValueLiteral, nil
}

// valueElement reads one element of a value at the start of text.
func valueElement(text string) (element, rest string, err error) {
	switch first := text[0]; {
	case first == '\'' || first == '"':
		contents, remainder, closed := cutDoubled(text, first)
		if !closed {
			return "", "", fmt.Errorf("an unclosed quote")
		}
		if first == '\'' && strings.Contains(contents, `\`) {
			return "", "", fmt.Errorf("a backslash in a string literal, which a server with standard_conforming_strings off reads as an escape; " +
				"write the value without one")
		}
		return text[:len(text)-len(remainder)], remainder, nil
	case first == '+' || first == '-' || first == '.' || (first >= '0' && first <= '9'):
		return number(text)
	case isWordByte(first, true):
		end := 1
		for end < len(text) && isWordByte(text[end], false) {
			end++
		}
		word := text[:end]
		if (word == "E" || word == "e") && end < len(text) && text[end] == '\'' {
			return "", "", fmt.Errorf("an escape string, which a setting does not take; write a plain literal")
		}
		return word, text[end:], nil
	default:
		return "", "", fmt.Errorf("unexpected %q", string(first))
	}
}

// number reads a numeric constant with an optional sign: digits with an
// optional fraction and exponent. It returns the number as the server stores
// it. A letter straight after it is refused, as the server
// refuses 30s with "trailing junk after numeric literal".
func number(text string) (element, rest string, err error) {
	i, sign := 0, ""
	if text[0] == '+' || text[0] == '-' {
		if text[0] == '-' {
			sign = "-"
		}
		i = 1
		for i < len(text) && (text[i] == ' ' || text[i] == '\t') {
			i++
		}
	}
	start := i
	i = digitsEnd(text, i)
	integer := true
	if i < len(text) && text[i] == '.' {
		integer = false
		i = digitsEnd(text, i+1)
	}
	if strings.Trim(text[start:i], ".") == "" {
		return "", "", fmt.Errorf("a sign or a point without a number")
	}
	if end := exponentEnd(text, i); end > i {
		integer, i = false, end
	}
	if i < len(text) && isWordByte(text[i], false) {
		return "", "", trailingWord(text, start, i)
	}
	// The spelling the server stores, measured on PostgreSQL 18.6: a number
	// keeps its digits as written, without a plus sign, so +1.5 is 1.5 while
	// 0.50, 1e3 and 1E+03 stay as they are; an integer is read by
	// [storedInteger]. A declaration is held in this spelling, so it compares
	// equal to what the server keeps from the statement Ptah writes for it.
	element = sign + text[start:i]
	if integer {
		element = storedInteger(sign, text[start:i])
	}
	return element, text[i:], nil
}

// storedInteger is the spelling the server stores for an integer: its value
// where it fits in 32 bits, which is how the server reads it, so 002 is 2,
// -005 is -5 and -0 is 0, and the digits as written otherwise, so 03000000000
// stays as it is.
func storedInteger(sign, digits string) string {
	value, err := strconv.ParseInt(sign+digits, 10, 64)
	if err != nil || value > math.MaxInt32 || value < -math.MaxInt32 {
		return sign + digits
	}
	return strconv.FormatInt(value, 10)
}

// trailingWord refuses the word that follows the number text[start:end]. A
// digit separator or a radix prefix is a spelling the server reads and stores
// as another number, so it is refused with that reason rather than as a unit.
func trailingWord(text string, start, end int) error {
	word := end
	for word < len(text) && isWordByte(text[word], false) {
		word++
	}
	written := text[start:word]
	if text[end] == '_' || (text[start:end] == "0" && strings.ContainsRune("xXoObB", rune(text[end]))) {
		return fmt.Errorf("%s is not a plain decimal number; write the number in decimal digits", written)
	}
	return fmt.Errorf("%s is a number followed by a word; a value with a unit is a string literal: '%s'", written, written)
}

// digitsEnd returns the index of the first byte at or after i that is not a
// decimal digit.
func digitsEnd(text string, i int) int {
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
	}
	return i
}

// exponentEnd returns where an exponent that starts at i ends, or i itself
// when no exponent with digits stands there.
func exponentEnd(text string, i int) int {
	if i >= len(text) || (text[i] != 'e' && text[i] != 'E') {
		return i
	}
	j := i + 1
	if j < len(text) && (text[j] == '+' || text[j] == '-') {
		j++
	}
	if k := digitsEnd(text, j); k > j {
		return k
	}
	return i
}

// isWordByte reports whether c may stand in an unquoted identifier, at its
// start when first is true. Bytes of a multibyte character may, as the server
// reads them.
func isWordByte(c byte, first bool) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_', c >= 0x80:
		return true
	case c >= '0' && c <= '9', c == '$':
		return !first
	default:
		return false
	}
}

// cutDoubled reads the part of text that quote opens, a doubled quote inside
// read as one, and returns its contents, what follows the closing quote and
// whether there was one.
func cutDoubled(text string, quote byte) (contents, rest string, closed bool) {
	var b strings.Builder
	for i := 1; i < len(text); i++ {
		if text[i] != quote {
			b.WriteByte(text[i])
			continue
		}
		if i+1 < len(text) && text[i+1] == quote {
			b.WriteByte(quote)
			i++
			continue
		}
		return b.String(), text[i+1:], true
	}
	return b.String(), "", false
}

// StoredValue is a declared value as pg_db_role_setting.setconfig keeps it,
// which is the text the server writes after the equals sign: '30s' is 30s, and
// search_path's '$user', 'public' is "$user", public. It is the spelling an
// observation holds, so a declaration and the server's row compare as text.
func StoredValue(name, value string) string {
	stored := routinesetting.ToCatalog(name + "=" + value)
	_, after, _ := strings.Cut(stored, "=")
	return after
}

// SQLValue is a stored value written in SQL, the spelling a statement that
// restores it writes: 30s is '30s', and search_path's "$user", public is
// '$user', 'public'. It refuses nothing: a stored value it cannot read as a
// list stays one literal.
func SQLValue(name, stored string) string {
	spelled := routinesetting.FromCatalog(name + "=" + stored)
	_, after, _ := strings.Cut(spelled, "=")
	return after
}

// Matches reports whether a declared value and a stored one are the value
// the server keeps: the declared value read down to the parts the server
// stores, compared with the stored one read the same way. '30s' matches 30s,
// and '$user', public matches "$user", public. 30s does not match 30000: the
// server keeps the spelling it was given, so a declaration converges after one
// statement and then compares equal.
func Matches(name, declared, stored string) bool {
	return routinesetting.Comparable(name+"="+declared) == routinesetting.Comparable(name+"="+SQLValue(name, stored))
}
