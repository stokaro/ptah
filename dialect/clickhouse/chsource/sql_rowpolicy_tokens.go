package chsource

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/sqlext"
	"ptah.run/internal/lexer"
)

// rowPolicyTokens retains expression bytes while reading clause boundaries.
type rowPolicyTokens struct{ *sqlTokens }

func newRowPolicyTokens(claim sqlext.Claim) *rowPolicyTokens {
	tokens := newSQLTokens(platform.ClickHouse, claim)
	tokens.tokens = slices.DeleteFunc(tokens.tokens, func(token lexer.Token) bool {
		return token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment
	})
	if n := len(tokens.tokens); n > 0 && tokens.tokens[n-1].Type == lexer.TokenSemicolon {
		tokens.tokens = tokens.tokens[:n-1]
	}
	return &rowPolicyTokens{tokens}
}

func (t *rowPolicyTokens) words(words ...string) error {
	for _, word := range words {
		if !t.keyword(word) {
			return t.fail("expected " + word)
		}
		t.pos++
	}
	return nil
}

func (t *rowPolicyTokens) fail(reason string) error {
	return fmt.Errorf("ClickHouse ROW POLICY: %s at position %d (got %q)", reason, t.position(t.current()), t.current().Value)
}

func (t *rowPolicyTokens) name() (string, error) {
	token := t.current()
	if token.Type != lexer.TokenIdentifier && token.Type != lexer.TokenString {
		return "", t.fail("expected an identifier")
	}
	name := token.Value
	if len(name) > 0 && (name[0] == '`' || name[0] == '"') {
		quote := name[:1]
		if len(name) < 2 || !strings.HasSuffix(name, quote) {
			return "", t.fail("expected a closed quoted identifier")
		}
		inner := name[1 : len(name)-1]
		if strings.Contains(inner, `\`) {
			return "", t.fail("backslash escapes in identifiers are not supported")
		}
		doubled := quote + quote
		if strings.Contains(strings.ReplaceAll(inner, doubled, ""), quote) {
			return "", t.fail("expected one quoted identifier")
		}
		name = strings.ReplaceAll(inner, doubled, quote)
	} else if token.Type != lexer.TokenIdentifier || name == "" || name[0] >= '0' && name[0] <= '9' {
		return "", t.fail("expected an identifier")
	}
	if name == "" {
		return "", t.fail("an identifier cannot be empty")
	}
	t.pos++
	return name, nil
}

func (t *rowPolicyTokens) table() (database, table string, err error) {
	first, err := t.name()
	if err != nil {
		return "", "", err
	}
	token := t.current()
	if !token.MatchOperatorValue(".") {
		return "", first, nil
	}
	t.pos++
	token = t.current()
	if token.MatchOperatorValue("*") {
		return "", "", t.fail("database-wide policies ON db.* are not supported")
	}
	second, err := t.name()
	return first, second, err
}

// clauseText accepts keywords in parentheses and quoted text. A top-level
// WITH starts the write-check clause, which the owner refuses by name.
func (t *rowPolicyTokens) clauseText(stop func() bool) (string, error) {
	start := t.pos
	var stack []string
	for t.pos < len(t.tokens) {
		token := t.current()
		if token.Type == lexer.TokenString || strings.HasPrefix(token.Value, "`") {
			if !closedPolicyQuote(token.Value) {
				return "", t.fail("expected closed quoted text")
			}
		}
		if len(stack) == 0 && stop() {
			break
		}
		switch token.Value {
		case "(", "[":
			stack = append(stack, token.Value)
		case ")", "]":
			want := "("
			if token.Value == "]" {
				want = "["
			}
			if len(stack) == 0 || stack[len(stack)-1] != want {
				return "", t.fail("unbalanced expression")
			}
			stack = stack[:len(stack)-1]
		}
		if token.Type == lexer.TokenUnknown || token.Type == lexer.TokenSemicolon {
			return "", t.fail("unsupported expression token")
		}
		t.pos++
	}
	if len(stack) != 0 {
		return "", t.fail("unbalanced expression")
	}
	if t.pos == start {
		return "", t.fail("expected a clause value")
	}
	return strings.TrimSpace(t.text[t.tokens[start].Start:t.tokens[t.pos-1].End]), nil
}

// filterText removes only parentheses that enclose the entire expression, so
// the renderer's wrapper does not grow on each SQL load/render cycle.
func (t *rowPolicyTokens) filterText(start int) string {
	tokens := t.tokens[start:t.pos]
	for len(tokens) >= 2 && tokens[0].Value == "(" && tokens[len(tokens)-1].Value == ")" {
		depth, encloses := 0, true
		for i, token := range tokens {
			switch token.Value {
			case "(":
				depth++
			case ")":
				depth--
			}
			if depth == 0 && i < len(tokens)-1 {
				encloses = false
				break
			}
		}
		if !encloses {
			break
		}
		tokens = tokens[1 : len(tokens)-1]
	}
	if len(tokens) == 0 {
		return ""
	}
	return strings.TrimSpace(t.text[tokens[0].Start:tokens[len(tokens)-1].End])
}

func closedPolicyQuote(text string) bool {
	if len(text) < 2 {
		return false
	}
	quote := text[0]
	for i := 1; i < len(text); i++ {
		if text[i] == '\\' {
			i++
			continue
		}
		if text[i] != quote {
			continue
		}
		if i+1 < len(text) && text[i+1] == quote {
			i++
			continue
		}
		return i == len(text)-1
	}
	return false
}
