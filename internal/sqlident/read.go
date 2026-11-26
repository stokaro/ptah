package sqlident

import (
	"strconv"
	"strings"

	"ptah.run/internal/dialectlexer"
)

// NamesArePaths reports whether dialect names an object by one path, as
// [Qualified] writes a YDB name, rather than by dot-separated parts. Such a
// name is one identifier, so a dot inside it is part of the name: `a.b/c` is
// the object c in the directory a.b. See [SplitPath].
func NamesArePaths(dialect string) bool {
	return spellingOf(dialect).paths
}

// ReadName reads written, one identifier of dialect, into the name it denotes
// by the dialect's own quoting, and reports whether it did. A dialect that
// names an object by one path is read by the lexer of its own its descriptor
// names (lexer.Scanner.IdentifierValue): on YDB that is YQL's quoting, a
// backticked name with its backslash escapes decoded or an unquoted name as
// written, so ReadName undoes [Quote] there.
//
// A dialect whose descriptor states Lexical.IdentifierBackslashEscapes, as
// ClickHouse's does, has a backquoted or double-quoted name read with its
// escapes decoded, the way the server reads them; see [unescape]. A doubled
// quote is one quote. So ReadName undoes [Quote] there too, reads the spelling
// the server prints in SHOW CREATE TABLE, and agrees with the lexer, which
// steps over such an escape. A name that ends inside an escape or before its
// closing quote is not one identifier, and neither is one holding \x without
// two hexadecimal digits after it.
//
// Any other dialect returns false, and so does text that is not one
// identifier, such as an empty one or one with text after its closing quote;
// a caller reads those by the SQL standard's quoting.
func ReadName(dialect, written string) (string, bool) {
	if scanner := dialectlexer.Options(dialect).Scanner; NamesArePaths(dialect) && scanner != nil {
		return scanner.IdentifierValue(written)
	}
	if spellingOf(dialect).backslash && written != "" && (written[0] == '`' || written[0] == '"') {
		return readEscaped(written)
	}
	return "", false
}

// readEscaped reads a quoted name in which a backslash escapes the byte after
// it; see [ReadName].
func readEscaped(written string) (string, bool) {
	quote := written[0]
	if len(written) < 2 || written[len(written)-1] != quote {
		return "", false
	}
	body := written[1 : len(written)-1]
	var name strings.Builder
	for i := 0; i < len(body); i++ {
		switch char := body[i]; {
		case char == quote:
			if i+1 >= len(body) || body[i+1] != quote {
				return "", false
			}
			name.WriteByte(quote)
			i++
		case char != '\\':
			name.WriteByte(char)
		default:
			unescaped, next, ok := unescape(body, i+1)
			if !ok {
				return "", false
			}
			name.WriteString(unescaped)
			i = next - 1
		}
	}
	return name.String(), true
}

// unescape reads the escape that starts at body[at], the byte after a
// backslash, and returns the text it stands for and the index after it. It
// follows the server, measured on ClickHouse 24.10 and 26.10:
//
//   - \a, \b, \e, \f, \n, \r, \t, \v and \0 stand for their control bytes;
//   - \xHH stands for the byte HH;
//   - \N stands for nothing;
//   - a backslash before a backslash, a quote of any kind, a slash, an equals
//     sign or a control byte stands for that byte;
//   - a backslash before any other byte is kept, so `a\zb` names a\zb.
//
// It reports false for an escape body ends inside, and for \x followed by
// anything but two hexadecimal digits, which the server reads as a byte that
// differs between its versions.
func unescape(body string, at int) (string, int, bool) {
	if at >= len(body) {
		return "", 0, false
	}
	switch escaped := body[at]; escaped {
	case 'x':
		if at+2 >= len(body) {
			return "", 0, false
		}
		value, err := strconv.ParseUint(body[at+1:at+3], 16, 8)
		if err != nil {
			return "", 0, false
		}
		return string([]byte{byte(value)}), at + 3, true
	case 'N':
		return "", at + 1, true
	default:
		decoded := escapedByte(escaped)
		if decoded > 0x1f && strings.IndexByte(`\'"`+"`"+`/=`, decoded) < 0 {
			return string([]byte{'\\', decoded}), at + 1, true
		}
		return string([]byte{decoded}), at + 1, true
	}
}

// escapedByte is the byte a letter after a backslash stands for, or the
// letter itself.
func escapedByte(letter byte) byte {
	switch letter {
	case 'a':
		return '\a'
	case 'b':
		return '\b'
	case 'e':
		return 0x1b
	case 'f':
		return '\f'
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	case 'v':
		return '\v'
	case '0':
		return 0
	default:
		return letter
	}
}

// SplitPath splits a path name, as a dialect [NamesArePaths] reports writes
// one, into the directory that holds the object and the object's own name,
// at the last slash. A name with no slash is in no directory.
func SplitPath(name string) (directory, leaf string) {
	if slash := strings.LastIndex(name, "/"); slash >= 0 {
		return name[:slash], name[slash+1:]
	}
	return "", name
}
