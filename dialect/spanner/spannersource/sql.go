package spannersource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// sqlRowDeletion names the owner's SQL pattern, which each claim carries back.
const sqlRowDeletion = "row-deletion-policy"

// SQL is the owner's contribution to the SQL frontend: a Spanner document's
// row deletion policy, `TTL INTERVAL '<interval>' ON <column>` after a CREATE
// TABLE's column list, read as the table's [spannerschema.DesiredRowDeletion].
// The pattern is targeted: a document of another target, or of none, reads
// TTL as ClickHouse's table clause, which takes a date expression.
//
// Its claim is [Coverage]: Spanner SQL spells a row deletion policy as a table
// clause, so a table without one requests none. A document that names no
// target makes no claim.
func SQL() sqlext.Extension {
	return sqlext.Extension{
		Owner:    spannerschema.Owner,
		Targets:  []string{platform.Spanner},
		Patterns: []sqlext.Pattern{{Name: sqlRowDeletion, Site: sqlext.TableOption, Words: []string{"TTL"}, Targeted: true}},
		Parser:   sqlParser{},
		Coverage: func([]coverage.Object) (schemaext.Coverage, error) { return Coverage() },
	}
}

// sqlParser reads the row deletion policies the owner's pattern claims.
type sqlParser struct{}

// ParseSQL reads every claim of one document, each to the policy of its
// table. A malformed clause, and one Spanner refuses, are refused with their
// position in the document.
func (sqlParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		if claim.Pattern != sqlRowDeletion {
			return sqlext.Result{}, fmt.Errorf("%w: Spanner SQL pattern %q", schemaext.ErrInvalidValue, claim.Pattern)
		}
		policy, err := newClauseTokens(request.Target, claim).rowDeletion()
		if err != nil {
			return sqlext.Result{}, err
		}
		facets, err := schemaext.Facets{}.With(policy)
		if err != nil {
			return sqlext.Result{}, err
		}
		values = append(values, sqlext.Value{Facets: facets})
	}
	return sqlext.Result{Values: values}, nil
}

// clauseTokens is a claimed clause as the frontend's lexer reads it for the
// document's target, positioned against the document.
type clauseTokens struct {
	offset int
	end    int
	tokens []lexer.Token
	pos    int
}

func newClauseTokens(target string, claim sqlext.Claim) *clauseTokens {
	scan := lexer.NewLexerWithOptions(claim.Text, dialectlexer.Options(target))
	tokens := &clauseTokens{offset: claim.Offset, end: len(claim.Text)}
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		tokens.tokens = append(tokens.tokens, token)
	}
	return tokens
}

// current is the token at the cursor, an EOF token at the end of the clause.
func (t *clauseTokens) current() lexer.Token {
	if t.pos >= len(t.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: t.end, End: t.end}
	}
	return t.tokens[t.pos]
}

// next steps past the token at the cursor and the whitespace, comments and
// version-guarded spans after it, as the frontend steps over them.
func (t *clauseTokens) next() {
	t.pos++
	for {
		switch t.current().Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenUnknown:
			t.pos++
		default:
			return
		}
	}
}

// position is where the token at the cursor stands in the document.
func (t *clauseTokens) position() int {
	return t.offset + t.current().Start
}

// word reports whether the token at the cursor is the bare word, whatever its
// letter case.
func (t *clauseTokens) word(word string) bool {
	current := t.current()
	return current.Type == lexer.TokenIdentifier && strings.EqualFold(current.Value, word)
}

// rowDeletion reads `TTL INTERVAL '<interval>' ON <column>` and nothing after
// it, and refuses a policy Spanner refuses.
func (t *clauseTokens) rowDeletion() (*spannerschema.DesiredRowDeletion, error) {
	t.next()
	if !t.word("INTERVAL") {
		return nil, fmt.Errorf("expected INTERVAL after TTL at position %d", t.position())
	}
	t.next()
	if t.current().Type != lexer.TokenString {
		return nil, fmt.Errorf("expected a quoted interval after TTL INTERVAL at position %d", t.position())
	}
	interval := unquote(t.current().Value, '\'')
	t.next()
	if !t.word("ON") {
		return nil, fmt.Errorf("expected ON after the TTL interval at position %d", t.position())
	}
	t.next()
	// A quoted column arrives as a String token: the lexer does not separate
	// a double-quoted identifier from a string literal, and `db read` quotes
	// every identifier it writes.
	column := t.current()
	if column.Type != lexer.TokenIdentifier && column.Type != lexer.TokenString {
		return nil, fmt.Errorf("expected a column name after TTL ... ON at position %d", t.position())
	}
	policy := &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: unquote(column.Value, '"'), Interval: interval}}
	if err := spannerschema.ValidateDesired(policy); err != nil {
		return nil, fmt.Errorf("TTL at position %d: %w", t.position(), err)
	}
	t.next()
	if rest := t.current(); rest.Type != lexer.TokenEOF {
		return nil, fmt.Errorf("unexpected %q after the row deletion policy at position %d", rest.Value, t.position())
	}
	return policy, nil
}

// unquote strips one pair of the given quote character and undoubles the
// escapes inside it, leaving a value carrying no quotes alone. A column named
// `a"b` is written `"a""b"`, and keeping the doubled pair would store `a""b`,
// which the renderer then escapes again into a column no database has.
func unquote(value string, quote byte) string {
	if len(value) < 2 || value[0] != quote || value[len(value)-1] != quote {
		return value
	}
	pair := string([]byte{quote, quote})
	return strings.ReplaceAll(value[1:len(value)-1], pair, string(quote))
}
