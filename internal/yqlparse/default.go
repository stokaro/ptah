package yqlparse

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/ydbtype"
)

// defaultValue preserves the value in the model's SQL-literal spelling.
// Keeping quotes is essential: the string "NULL" and SQL NULL are different,
// and a string containing leading/trailing spaces must not be trimmed later.
func defaultValue(text, columnType string) (string, error) {
	if strings.EqualFold(text, "NULL") {
		return "NULL", nil
	}
	p := newParser(text)
	value, ok := literalValue(p, columnType)
	if !ok {
		return "", fmt.Errorf("default %q is not a supported literal of type %s", text, columnType)
	}
	return "'" + strings.ReplaceAll(value, "'", "''") + "'", nil
}

func literalValue(p *parser, columnType string) (string, bool) {
	tokens := p.tokens
	if len(tokens) == 0 {
		return "", false
	}
	if len(tokens) == 1 && tokens[0].Type == lexer.TokenString {
		return stringLiteral(tokens[0].Value, columnType)
	}
	if len(tokens) > 2 {
		return constructorLiteral(tokens, columnType)
	}
	if len(tokens) == 2 && tokens[0].Value != "-" {
		return "", false
	}
	value, ok := ydbtype.LiteralValue(p.text)
	if !ok {
		return "", false
	}
	if p.text == "true" || p.text == "false" {
		return value, columnType == ydbtype.Bool
	}
	suffix := strings.TrimLeft(p.text, "-0123456789")
	types := map[string]string{"": ydbtype.Int32, "t": ydbtype.Int8, "s": ydbtype.Int16, "l": ydbtype.Int64, "ut": ydbtype.Uint8, "us": ydbtype.Uint16, "u": ydbtype.Uint32, "ul": ydbtype.Uint64}
	return value, types[suffix] == columnType
}

func stringLiteral(raw, columnType string) (string, bool) {
	original := raw
	suffix := strings.HasSuffix(strings.ToLower(raw), "u")
	if suffix || strings.HasSuffix(strings.ToLower(raw), "s") {
		raw = raw[:len(raw)-1]
	}
	if !strings.HasSuffix(raw, "\"") && !strings.HasSuffix(raw, "'") && !strings.HasSuffix(raw, "@@") {
		return "", false
	}
	if columnType != ydbtype.String && columnType != ydbtype.Utf8 {
		return "", false
	}
	if suffix != (columnType == ydbtype.Utf8) {
		return "", false
	}
	return lexer.StringValue(original, dialectlexer.Options(platform.YDB))
}

func constructorLiteral(tokens []lexer.Token, columnType string) (string, bool) {
	if len(tokens) == 4 && tokens[1].Value == "(" && tokens[3].Value == ")" && tokens[0].Value == columnType {
		// LiteralValue owns the finite set of constructor types. Decode alternate
		// YQL quote forms before asking it, without permitting arbitrary functions.
		value, ok := lexer.StringValue(tokens[2].Value, dialectlexer.Options(platform.YDB))
		if !ok {
			return "", false
		}
		canonical := columnType + "('" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(value) + "')"
		return ydbtype.LiteralValue(canonical)
	}
	if len(tokens) == 8 && tokens[0].Value == "Decimal" && tokens[1].Value == "(" && tokens[3].Value == "," && tokens[5].Value == "," && tokens[7].Value == ")" {
		if columnType != "Decimal("+tokens[4].Value+","+tokens[6].Value+")" {
			return "", false
		}
		return lexer.StringValue(tokens[2].Value, dialectlexer.Options(platform.YDB))
	}
	return "", false
}
