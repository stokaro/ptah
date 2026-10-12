package pgpolicysource

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/sqlext"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// policyTokens reads one claimed statement with the target's lexical rules.
// Expressions keep their source text; names keep the spelling the server sees.
type policyTokens struct {
	target  string
	text    string
	offset  int
	options lexer.Options
	tokens  []lexer.Token
	pos     int
}

func newPolicyTokens(target string, claim sqlext.Claim) *policyTokens {
	t := &policyTokens{target: platform.NormalizeDialect(target), text: claim.Text, offset: claim.Offset}
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

func (t *policyTokens) current() lexer.Token {
	if t.pos >= len(t.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: len(t.text), End: len(t.text)}
	}
	return t.tokens[t.pos]
}

func (t *policyTokens) keyword(word string) bool {
	token := t.current()
	return token.MatchIdentifierValue(word)
}

func (t *policyTokens) operator(word string) bool {
	token := t.current()
	return token.MatchOperatorValue(word)
}

func (t *policyTokens) words(words ...string) error {
	for _, word := range words {
		if !t.keyword(word) {
			return fmt.Errorf("expected %s at position %d", word, t.offset+t.current().Start)
		}
		t.pos++
	}
	return nil
}

func (t *policyTokens) end() error {
	if t.current().Type != lexer.TokenEOF {
		return fmt.Errorf("unexpected %s at position %d", t.current().Value, t.offset+t.current().Start)
	}
	return nil
}

func (t *policyTokens) name(label string) (string, error) {
	token := t.current()
	switch {
	case token.Type == lexer.TokenIdentifier:
		t.pos++
		switch t.target {
		case platform.Postgres, platform.YugabyteDB:
			return lowerASCII(token.Value), nil
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

// policyTable is the reference as written beside the names the server reads.
type policyTable struct{ schema, name, written string }

func (t *policyTokens) table() (policyTable, error) {
	start := t.current().Start
	first, err := t.name("policy table name")
	if err != nil {
		return policyTable{}, err
	}
	table := policyTable{name: first}
	if t.operator(".") {
		t.pos++
		second, err := t.name("table name after schema")
		if err != nil {
			return policyTable{}, err
		}
		table.schema, table.name = first, second
	}
	table.written = t.text[start:t.tokens[t.pos-1].End]
	return table, nil
}

func (t *policyTokens) expression(label string) (string, error) {
	if !t.operator("(") {
		return "", fmt.Errorf("expected '(' after %s at position %d", label, t.offset+t.current().Start)
	}
	start := t.current().End
	t.pos++
	depth := 1
	for t.current().Type != lexer.TokenEOF {
		token := t.current()
		t.pos++
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return strings.TrimSpace(t.text[start:token.Start]), nil
			}
		}
	}
	return "", fmt.Errorf("unterminated %s expression", label)
}

func (t *policyTokens) commentText() (string, error) {
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
		return "", fmt.Errorf("invalid policy comment string at position %d", t.offset+token.Start)
	}
	t.pos++
	return value, t.end()
}
