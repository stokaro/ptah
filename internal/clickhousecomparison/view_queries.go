package clickhousecomparison

import (
	"context"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemacomparison"
	"ptah.run/internal/chcolumn"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlqualifier"
	"ptah.run/internal/sqlreach"
)

// ViewQueries compares a view's local catalog query with a declaration that
// may explicitly name the connection's database. It proves equality only
// after safe qualifier removal; other pairs keep the shared comparison.
type ViewQueries struct{}

// EquivalentViewQueries answers each pair in order, using the request's
// default schema as the current database. Equal token streams after local
// normalization are equivalent. An unknown scope, ambiguous expression or
// other difference is undecided. It changes no request data and needs no
// server; it returns no error.
func (ViewQueries) EquivalentViewQueries(_ context.Context, request schemacomparison.ViewQueriesRequest) ([]schemacomparison.ViewQueryAnswer, error) {
	answers := make([]schemacomparison.ViewQueryAnswer, len(request.Pairs))
	for i, pair := range request.Pairs {
		answers[i] = schemacomparison.ViewQueryUndecided
		if request.Identifiers.DefaultSchema == "" {
			continue
		}
		declared, err := sqlqualifier.LocalViewBody(pair.Declared, platform.ClickHouse, request.Identifiers.DefaultSchema, chcolumn.UnquoteIdentifier)
		if err != nil {
			continue
		}
		held, err := sqlqualifier.LocalViewBody(pair.Held, platform.ClickHouse, request.Identifiers.DefaultSchema, chcolumn.UnquoteIdentifier)
		if err != nil {
			continue
		}
		left, leftOK := viewQueryTokens(declared)
		right, rightOK := viewQueryTokens(held)
		if leftOK && rightOK && slices.Equal(left, right) {
			answers[i] = schemacomparison.ViewQueryEquivalent
		}
	}
	return answers, nil
}

// viewQueryTokens ignores formatting and source-relation quoting while
// retaining expression quoting, every name's case, literals, operators and
// grouping parentheses. A query this
// narrow reading cannot equate remains for the shared comparison to decide.
func viewQueryTokens(body string) ([]lexer.Token, bool) {
	tokens := sqlreach.SignificantTokens(body, true, platform.ClickHouse)
	if len(tokens) > 0 && tokens[len(tokens)-1].Type == lexer.TokenSemicolon {
		tokens = tokens[:len(tokens)-1]
	}
	relations := sqlqualifier.QueryRelations(tokens)
	for i, token := range tokens {
		tokens[i].Start, tokens[i].End = 0, 0
		if !relations[i] || (token.Type != lexer.TokenIdentifier && !(token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`))) {
			continue
		}
		name, ok := chcolumn.UnquoteIdentifier(token.Value)
		if !ok {
			return nil, false
		}
		tokens[i].Type, tokens[i].Value = lexer.TokenIdentifier, name
	}
	return tokens, true
}
