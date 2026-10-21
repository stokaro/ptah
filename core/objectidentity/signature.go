package objectidentity

import (
	"strconv"
	"strings"

	"ptah.run/internal/lexer"
)

// SignatureText formats source components for diagnostics. Components containing
// separators, quotes, or parentheses are quoted so a single component cannot
// look like several. This text is neither SQL nor a comparison key.
func (id ID) SignatureText() string {
	var parts []string
	for _, part := range id.Signature.Values() {
		text := part.Source
		if text == "" || strings.ContainsAny(text, ",()\"\\\n\r\t") {
			text = strconv.Quote(text)
		}
		parts = append(parts, text)
	}
	return strings.Join(parts, ", ")
}

func signatureParts(signature string) Parts {
	text := strings.TrimSpace(signature)
	if strings.HasPrefix(text, "(") && strings.HasSuffix(text, ")") {
		text = text[1 : len(text)-1]
	}

	var parts []Part
	start, depth := 0, 0
	appendPart := func(end int) {
		argument := strings.TrimSpace(text[start:end])
		if argument != "" {
			parts = append(parts, Part{Source: argument, Normalized: normalizeSignature(argument)})
		}
	}
	scanner := signatureLexer(text)
	for {
		token := scanner.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			appendPart(len(text))
			return NewParts(parts...)
		case lexer.TokenOperator:
			switch token.Value {
			case "(":
				depth++
			case ")":
				depth--
			case ",":
				if depth == 0 {
					appendPart(token.Start)
					start = token.End
				}
			}
		}
	}
}

// normalizeSignature folds unquoted type text only. Quoted names retain their
// case, whitespace, and commas; changing any of those can select another type.
func normalizeSignature(argument string) string {
	scanner := signatureLexer(argument)
	var result strings.Builder
	space := false
	for {
		token := scanner.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return result.String()
		case lexer.TokenUnknown:
			return strings.TrimSpace(argument)
		case lexer.TokenWhitespace:
			space = result.Len() != 0
		default:
			if space && token.Value != "," {
				result.WriteByte(' ')
			}
			value := token.Value
			if token.Type == lexer.TokenIdentifier && !strings.HasPrefix(value, "`") && !strings.HasPrefix(value, "[") {
				value = strings.ToLower(value)
			}
			result.WriteString(value)
			space = value == ","
		}
	}
}

// Splitting and folding must recognize the same quoted tokens, or a comma in a
// type name can become a boundary in one pass and ordinary text in the other.
func signatureLexer(text string) *lexer.Lexer {
	return lexer.NewLexerWithOptions(text, lexer.Options{StandardStrings: true, BracketIdentifiers: true})
}
