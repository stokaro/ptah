package pgtypesource

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// rangeTokens reads one claimed statement with the target's lexical rules.
// Expressions keep their source text; names keep the spelling the server sees.
type rangeTokens struct {
	target  string
	text    string
	offset  int
	options lexer.Options
	tokens  []lexer.Token
	pos     int
}

func newRangeTokens(target string, claim sqlext.Claim) *rangeTokens {
	t := &rangeTokens{target: platform.NormalizeDialect(target), text: claim.Text, offset: claim.Offset}
	if t.target != "" {
		t.options = dialectlexer.Options(t.target)
	}
	scan := lexer.NewLexerWithOptions(t.text, t.options)
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			t.tokens = append(t.tokens, token)
		}
	}
	if len(t.tokens) > 0 && t.tokens[len(t.tokens)-1].Type == lexer.TokenSemicolon {
		t.tokens = t.tokens[:len(t.tokens)-1]
	}
	if t.target == "" {
		t.options.BackslashEscapes = true
	}
	return t
}

func (t *rangeTokens) current() lexer.Token {
	if t.pos >= len(t.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: len(t.text), End: len(t.text)}
	}
	return t.tokens[t.pos]
}

func (t *rangeTokens) keyword(word string) bool {
	token := t.current()
	return token.MatchIdentifierValue(word)
}

func (t *rangeTokens) operator(word string) bool {
	token := t.current()
	return token.MatchOperatorValue(word)
}

func (t *rangeTokens) words(words ...string) error {
	for _, word := range words {
		if !t.keyword(word) {
			return fmt.Errorf("expected %s at position %d", word, t.offset+t.current().Start)
		}
		t.pos++
	}
	return nil
}

func (t *rangeTokens) end() error {
	if t.current().Type != lexer.TokenEOF {
		return fmt.Errorf("unexpected %s at position %d", t.current().Value, t.offset+t.current().Start)
	}
	return nil
}

func (t *rangeTokens) name(label string) (string, error) {
	token := t.current()
	switch {
	case token.Type == lexer.TokenIdentifier:
		t.pos++
		switch t.target {
		case platform.Postgres, platform.YugabyteDB:
			return identifier.ComparisonASCIIInsensitive.IdentityKey(token.Value), nil
		case platform.CockroachDB:
			return identifier.ComparisonUnicodeInsensitive.IdentityKey(token.Value), nil
		default:
			return token.Value, nil
		}
	case token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`):
		if len(token.Value) < 2 || !strings.HasSuffix(token.Value, `"`) || strings.Contains(strings.ReplaceAll(token.Value[1:len(token.Value)-1], `""`, ""), `"`) {
			return "", fmt.Errorf("expected a closed quoted %s at position %d", label, t.offset+token.Start)
		}
		t.pos++
		return strings.ReplaceAll(token.Value[1:len(token.Value)-1], `""`, `"`), nil
	default:
		return "", fmt.Errorf("expected %s at position %d", label, t.offset+token.Start)
	}
}

func (t *rangeTokens) reference() (objectidentity.ID, error) {
	first, err := t.name("range type name")
	if err != nil {
		return objectidentity.ID{}, err
	}
	schema, name := "", first
	if t.operator(".") {
		t.pos++
		name, err = t.name("range name after schema")
		if err != nil {
			return objectidentity.ID{}, err
		}
		schema = first
	}
	return pgtype.Ref(pgtype.RangeKind, schema, name), nil
}

func (t *rangeTokens) punctuation(text string) error {
	if !t.operator(text) {
		return fmt.Errorf("expected %q at position %d", text, t.offset+t.current().Start)
	}
	t.pos++
	return nil
}

func (t *rangeTokens) optionValue(name string) (string, error) {
	start, end, depth := t.current().Start, t.current().Start, 0
	for t.current().Type != lexer.TokenEOF {
		if depth == 0 && (t.operator(",") || t.operator(")")) {
			break
		}
		if t.operator("(") {
			depth++
		}
		if t.operator(")") {
			depth--
		}
		end = t.current().End
		t.pos++
	}
	if depth != 0 || end == start {
		return "", fmt.Errorf("expected a value for range option %q at position %d", name, t.offset+start)
	}
	return strings.TrimSpace(t.text[start:end]), nil
}

func (t *rangeTokens) commentText() (string, error) {
	if t.keyword("NULL") {
		t.pos++
		return "", t.end()
	}
	token := t.current()
	if token.Type != lexer.TokenString || strings.HasPrefix(token.Value, `"`) {
		return "", fmt.Errorf("expected a string constant or NULL after IS at position %d", t.offset+token.Start)
	}
	value, ok := lexer.StringValue(token.Value, t.options)
	if !ok {
		return "", fmt.Errorf("invalid range comment string at position %d", t.offset+token.Start)
	}
	t.pos++
	return value, t.end()
}
