package parser

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
)

// gluedLiteral reads a literal default the lexer split after its first word,
// value, written upper in upper case: PostgreSQL's E'x', and on the MySQL
// family a prefixed string ([Parser.prefixedLiteral]) or a hexadecimal number
// ([Parser.hexadecimalNumber]). The other engines read those spellings each
// their own way: 0x1F is the number 31 on PostgreSQL, SQLite and ClickHouse,
// and x'1f' a bit string on PostgreSQL and a blob on SQLite, so read as the
// MySQL literal they would change type.
func (p *Parser) gluedLiteral(value, upper string) (*ast.DefaultValue, bool) {
	if upper == "E" && p.current.Type == lexer.TokenString {
		value += p.current.Value
		p.advance()
		return &ast.DefaultValue{Value: value, ValueSet: true}, true
	}
	if !p.target.mysqlStrings {
		return nil, false
	}
	if literal, ok := p.prefixedLiteral(upper); ok {
		return literal, true
	}
	return p.hexadecimalNumber(value)
}

// prefixedLiteral reads a default written as a letter or a character set
// glued to a quoted string, after the prefix upper has been read: the bit
// string B'101' and the hexadecimal string X'41', kept as SQL with the prefix
// in lower case, and N'x' and _utf8mb4'x', which store the string alone and
// read as it. A space between the two makes them two tokens,
// which no server reads as one literal.
func (p *Parser) prefixedLiteral(upper string) (*ast.DefaultValue, bool) {
	if p.current.Type != lexer.TokenString || p.current.Start != p.previous.End || isDoubleQuotedIdentifierToken(p.current) {
		return nil, false
	}
	switch {
	case upper == "B" || upper == "X":
		value := strings.ToLower(upper + p.current.Value)
		p.advance()
		return &ast.DefaultValue{Expression: value}, true
	case upper == "N" || (len(upper) > 1 && upper[0] == '_'):
		value := p.current.Value
		p.advance()
		return &ast.DefaultValue{Value: p.mysqlStringConstant(value), ValueSet: true}, true
	}
	return nil, false
}

// hexadecimalNumber reads the rest of 0x61 and 0b101 after the 0 has been
// read: MySQL and MariaDB write a hexadecimal or bit string this way, and the
// lexer splits it into the word 0 and the word after it. The literal keeps
// this spelling, in lower case. The X'61' form is not the same literal
// everywhere: measured on MariaDB 12.3, an int column takes DEFAULT 0x1F and
// answers ERROR 1067 to DEFAULT x'1f'.
func (p *Parser) hexadecimalNumber(number string) (*ast.DefaultValue, bool) {
	word := strings.ToLower(p.current.Value)
	if number != "0" || p.current.Type != lexer.TokenIdentifier || p.current.Start != p.previous.End || len(word) < 2 {
		return nil, false
	}
	digits := map[byte]string{'x': "0123456789abcdef", 'b': "01"}[word[0]]
	if digits == "" || strings.Trim(word[1:], digits) != "" {
		return nil, false
	}
	p.advance()
	return &ast.DefaultValue{Expression: "0" + word}, true
}

// mysqlStringConstant writes the string token just read, and every string
// token that follows it across whitespace, as one single-quoted MySQL string:
// MySQL reads `'a' 'b'` as 'ab', and "a" as 'a' under its default sql_mode.
// Escapes keep their spelling, so the constant means what the tokens did.
func (p *Parser) mysqlStringConstant(token string) string {
	body := mysqlStringBody(token)
	for {
		p.skipWhitespace()
		if p.current.Type != lexer.TokenString {
			return "'" + body + "'"
		}
		body += mysqlStringBody(p.current.Value)
		p.advance()
	}
}

// mysqlStringBody returns the inside of a MySQL string token as it reads
// between single quotes: a doubled double quote is one, and a single quote is
// doubled.
func mysqlStringBody(token string) string {
	if len(token) < 2 {
		return token
	}
	inner := token[1 : len(token)-1]
	if token[0] != '"' {
		return inner
	}
	var b strings.Builder
	for i := 0; i < len(inner); i++ {
		switch ch := inner[i]; {
		case ch == '\\' && i+1 < len(inner):
			b.WriteByte(ch)
			i++
			b.WriteByte(inner[i])
		case ch == '"' && i+1 < len(inner) && inner[i+1] == '"':
			b.WriteByte(ch)
			i++
		case ch == '\'':
			b.WriteString("''")
		default:
			b.WriteByte(ch)
		}
	}
	return b.String()
}
