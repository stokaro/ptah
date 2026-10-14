package pgdefaultprivilegesource

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/sqlext"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// privilegeTokens reads one owner claim with positions in its source document.
type privilegeTokens struct {
	target           string
	offset, end, pos int
	tokens           []lexer.Token
}

func newPrivilegeTokens(target string, claim sqlext.Claim) *privilegeTokens {
	t := &privilegeTokens{target: platform.NormalizeDialect(target), offset: claim.Offset, end: len(claim.Text)}
	options := lexer.Options{}
	if t.target != "" {
		options = dialectlexer.Options(t.target)
	}
	scan := lexer.NewLexerWithOptions(claim.Text, options)
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			t.tokens = append(t.tokens, token)
		}
	}
	if len(t.tokens) > 0 && t.tokens[len(t.tokens)-1].Type == lexer.TokenSemicolon {
		t.tokens = t.tokens[:len(t.tokens)-1]
	}
	return t
}

func (t *privilegeTokens) current() lexer.Token {
	if t.pos >= len(t.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: t.end, End: t.end}
	}
	return t.tokens[t.pos]
}

func (t *privilegeTokens) keyword(word string) bool {
	token := t.current()
	return token.MatchIdentifierValue(word)
}

func (t *privilegeTokens) comma() bool {
	token := t.current()
	return token.MatchOperatorValue(",")
}

func (t *privilegeTokens) words(words ...string) error {
	for _, word := range words {
		if !t.keyword(word) {
			return fmt.Errorf("expected %s in ALTER DEFAULT PRIVILEGES at position %d", word, t.offset+t.current().Start)
		}
		t.pos++
	}
	return nil
}

func (t *privilegeTokens) name(label string) (string, error) {
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
		return "", fmt.Errorf("expected %s in ALTER DEFAULT PRIVILEGES at position %d", label, t.offset+token.Start)
	}
}

func (t *privilegeTokens) singleName(label string) (string, error) {
	name, err := t.name(label)
	if err != nil {
		return "", err
	}
	if t.comma() {
		return "", fmt.Errorf("ALTER DEFAULT PRIVILEGES names one %s here, not a list, at position %d", label, t.offset+t.current().Start)
	}
	return name, nil
}

func (t *privilegeTokens) endOfStatement() error {
	if t.current().Type != lexer.TokenEOF {
		return fmt.Errorf("unsupported ALTER DEFAULT PRIVILEGES clause %q at position %d", t.current().Value, t.offset+t.current().Start)
	}
	return nil
}

// roleName folds quoted roles on CockroachDB too. Schema names still follow
// the ordinary identifier rules, so this normalization belongs at role sites.
func (t *privilegeTokens) roleName(label string) (string, error) {
	name, err := t.singleName(label)
	if err != nil {
		return "", err
	}
	if t.target == platform.CockroachDB {
		name = identifier.ComparisonUnicodeInsensitive.IdentityKey(name)
	}
	return name, nil
}
