package onlineddl

import (
	"strings"
)

// AlterTarget is the parsed target of an ALTER TABLE statement.
type AlterTarget struct {
	// Schema is the explicit schema qualifier, empty when the table
	// reference is unqualified.
	Schema string
	// Table is the bare table name, quoting stripped.
	Table string
	// Clause is everything after the table reference — the value passed to
	// the tool's --alter flag.
	Clause string
}

// ParseAlterTable splits an ALTER TABLE statement (comments already
// stripped) into its target table and alter clause. ok is false for any
// other statement, and for a bare ALTER TABLE with no clause. The optional
// MariaDB IF EXISTS modifier and backtick-quoted or schema-qualified table
// references are handled.
func ParseAlterTable(stmt string) (AlterTarget, bool) {
	rest, ok := eatKeyword(strings.TrimSpace(stmt), "ALTER")
	if !ok {
		return AlterTarget{}, false
	}
	// MariaDB accepts ALTER [ONLINE] [IGNORE] TABLE ...; skip those modifiers.
	if after, ok := eatKeyword(rest, "ONLINE"); ok {
		rest = after
	}
	if after, ok := eatKeyword(rest, "IGNORE"); ok {
		rest = after
	}
	rest, ok = eatKeyword(rest, "TABLE")
	if !ok {
		return AlterTarget{}, false
	}
	if afterIf, ok := eatKeyword(rest, "IF"); ok {
		if afterExists, ok := eatKeyword(afterIf, "EXISTS"); ok {
			rest = afterExists
		}
	}

	name, rest, ok := eatIdentifier(rest)
	if !ok {
		return AlterTarget{}, false
	}
	target := AlterTarget{Table: name}

	rest = strings.TrimLeft(rest, " \t\r\n")
	if strings.HasPrefix(rest, ".") {
		second, afterSecond, ok := eatIdentifier(strings.TrimLeft(rest[1:], " \t\r\n"))
		if !ok {
			return AlterTarget{}, false
		}
		target.Schema, target.Table = name, second
		rest = afterSecond
	}

	target.Clause = strings.TrimSpace(rest)
	if target.Clause == "" {
		return AlterTarget{}, false
	}
	return target, true
}

// eatKeyword consumes a case-insensitive keyword followed by whitespace and
// returns the remainder with leading whitespace trimmed.
func eatKeyword(s, keyword string) (string, bool) {
	if len(s) <= len(keyword) || !strings.EqualFold(s[:len(keyword)], keyword) {
		return s, false
	}
	rest := s[len(keyword):]
	if !isSpaceByte(rest[0]) {
		return s, false
	}
	return strings.TrimLeft(rest, " \t\r\n"), true
}

// eatIdentifier consumes one identifier, backtick-quoted (doubled backticks
// stay inside, quotes stripped) or bare, and returns it plus the remainder.
func eatIdentifier(s string) (name, rest string, ok bool) {
	if s == "" {
		return "", s, false
	}
	if s[0] == '`' {
		var b strings.Builder
		i := 1
		for i < len(s) {
			if s[i] != '`' {
				b.WriteByte(s[i])
				i++
				continue
			}
			if i+1 < len(s) && s[i+1] == '`' {
				b.WriteByte('`')
				i += 2
				continue
			}
			if b.Len() == 0 {
				return "", s, false // empty identifier ``
			}
			return b.String(), s[i+1:], true
		}
		return "", s, false // unterminated quote
	}

	i := 0
	for i < len(s) && !isSpaceByte(s[i]) && s[i] != '.' && s[i] != '(' && s[i] != ',' && s[i] != ';' {
		i++
	}
	if i == 0 {
		return "", s, false
	}
	return s[:i], s[i:], true
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}
