package chsource

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// The names of the owner's SQL patterns, which each claim carries back.
const (
	sqlSkippingIndex    = "skipping-index"
	sqlAddSkippingIndex = "add-skipping-index"
	sqlRefresh          = "refresh"
	sqlStorage          = "storage"
)

// SQL is the owner's contribution to the SQL frontend.
//
// It reads the data-skipping index a table declares inside its column list,
// `INDEX <name> <expression> TYPE <type> [GRANULARITY <n>]`, and the same
// index an ALTER TABLE adds. ClickHouse alone spells an index this way inside
// the column list, where MySQL's `INDEX name (columns)` stands, so that form
// is read only from a document that names ClickHouse. The ALTER form is read
// from a document that names no target too, as the frontend always read it;
// a document that names the MySQL family reads its own ADD INDEX there.
//
// It reads what a materialized view states between its name and AS: the
// refresh schedule, through the grammar the Go source uses, so a SQL file and
// an annotation declare the same schedule the same way, and the storage
// clause, which may be only the one the renderer writes, because a view's
// storage is not modeled.
//
// Its claim is [RefreshCoverage]: a ClickHouse materialized view states its
// schedule in its CREATE statement, so a view without one declares a plain
// view.
func SQL() sqlext.Extension {
	return sqlext.Extension{
		Owner:   chschema.Owner,
		Targets: []string{platform.ClickHouse},
		Facets:  []schemaext.Kind{chschema.RefreshKind},
		Patterns: []sqlext.Pattern{
			{Name: sqlSkippingIndex, Site: sqlext.TableElement, Words: []string{"INDEX"}, Targeted: true},
			{Name: sqlAddSkippingIndex, Site: sqlext.AlterOperation, Words: []string{"ADD", "INDEX"}},
			{Name: sqlRefresh, Site: sqlext.ViewClause, Words: []string{"REFRESH"},
				Stops: []string{"ENGINE", "EMPTY", "DEFINER", "SQL", "("}, Targeted: true},
			{Name: sqlStorage, Site: sqlext.ViewClause, Words: []string{"ENGINE"}, Targeted: true},
		},
		Parser:   sqlParser{},
		Coverage: sqlCoverage,
	}
}

// sqlCoverage is the owner's claim for a ClickHouse document. The owner reads
// no header entry, so it is handed none.
func sqlCoverage(sqlext.CoverageRequest) (schemaext.Coverage, error) {
	return RefreshCoverage()
}

// sqlParser reads the spans the owner's patterns claim.
type sqlParser struct{}

// ParseSQL reads every claim of one document.
func (sqlParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		value, err := readClaim(request.Target, claim)
		if err != nil {
			return sqlext.Result{}, err
		}
		values = append(values, value)
	}
	return sqlext.Result{Values: values}, nil
}

func readClaim(target string, claim sqlext.Claim) (sqlext.Value, error) {
	tokens := newSQLTokens(target, claim)
	switch claim.Pattern {
	case sqlSkippingIndex:
		index, err := tokens.skippingIndex()
		if err != nil {
			return sqlext.Value{}, err
		}
		// The type and granularity are the owner's settings, not the common
		// index type, so they travel as its declaration.
		facets, err := index.DeclaredFacets()
		if err != nil {
			return sqlext.Value{}, err
		}
		return sqlext.Value{Index: &sqlext.Index{Name: index.Name, Expression: index.Expression, Facets: facets}}, nil
	case sqlAddSkippingIndex:
		tokens.next() // ADD
		index, err := tokens.skippingIndex()
		if err != nil {
			return sqlext.Value{}, err
		}
		facets, err := index.DeclaredFacets()
		if err != nil {
			return sqlext.Value{}, err
		}
		return sqlext.Value{
			Operation: index,
			Index:     &sqlext.Index{Name: index.Name, Expression: index.Expression, Facets: facets},
		}, nil
	case sqlRefresh:
		tokens.next() // REFRESH
		facets, err := RefreshFacets(tokens.rest())
		if err != nil {
			return sqlext.Value{}, fmt.Errorf("materialized view %s: REFRESH: %w", claim.Subject, err)
		}
		return sqlext.Value{Facets: facets}, nil
	case sqlStorage:
		if err := tokens.viewStorage(claim.Subject); err != nil {
			return sqlext.Value{}, err
		}
		return sqlext.Value{}, nil
	default:
		return sqlext.Value{}, fmt.Errorf("%w: ClickHouse SQL pattern %q", schemaext.ErrInvalidValue, claim.Pattern)
	}
}

// sqlTokens is a claim's text as the frontend's lexer reads it for the
// document's target, positioned against the document.
type sqlTokens struct {
	text   string
	offset int
	tokens []lexer.Token
	pos    int
}

func newSQLTokens(target string, claim sqlext.Claim) *sqlTokens {
	options := lexer.Options{}
	if target != "" {
		options = dialectlexer.Options(target)
	}
	scan := lexer.NewLexerWithOptions(claim.Text, options)
	tokens := &sqlTokens{text: claim.Text, offset: claim.Offset}
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		tokens.tokens = append(tokens.tokens, token)
	}
	return tokens
}

// current is the token at the cursor, an EOF token at the end of the span.
func (t *sqlTokens) current() lexer.Token {
	if t.pos >= len(t.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: len(t.text), End: len(t.text)}
	}
	return t.tokens[t.pos]
}

// position is where token stands in the document.
func (t *sqlTokens) position(token lexer.Token) int {
	return t.offset + token.Start
}

func (t *sqlTokens) next() {
	t.skipWhitespace()
	if t.pos < len(t.tokens) {
		t.pos++
	}
}

func (t *sqlTokens) skipWhitespace() {
	for t.pos < len(t.tokens) {
		switch t.tokens[t.pos].Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenUnknown:
			t.pos++
		default:
			return
		}
	}
}

func (t *sqlTokens) keyword(word string) bool {
	token := t.current()
	return token.Type == lexer.TokenIdentifier && strings.EqualFold(token.Value, word)
}

// rest is the source text from the cursor to the end of the span.
func (t *sqlTokens) rest() string {
	if t.pos >= len(t.tokens) {
		return ""
	}
	return t.text[t.tokens[t.pos].Start:]
}

// identifier reads a name: a plain or double-quoted identifier, or a
// bracketed one.
func (t *sqlTokens) identifier() (string, error) {
	t.skipWhitespace()
	token := t.current()
	if token.MatchOperatorValue("[") {
		return t.bracketedIdentifier()
	}
	if token.Type != lexer.TokenIdentifier && (token.Type != lexer.TokenString || !strings.HasPrefix(token.Value, `"`)) {
		return "", fmt.Errorf("expected identifier, got %s at position %d", token.Type, t.position(token))
	}
	t.pos++
	return token.Value, nil
}

func (t *sqlTokens) bracketedIdentifier() (string, error) {
	start := t.position(t.current())
	var value strings.Builder
	for {
		token := t.current()
		if token.Type == lexer.TokenEOF {
			return "", fmt.Errorf("unterminated bracketed identifier at position %d", start)
		}
		value.WriteString(token.Value)
		t.pos++
		if token.MatchOperatorValue("]") {
			return value.String(), nil
		}
	}
}

// expression returns the source text from the cursor to the first top-level
// token stop accepts, or the end of the span, and the document position it
// started at. Parenthesised sub-expressions are stepped over, because
// `TYPE set(100)` carries the stop keywords' company inside brackets that are
// part of the value.
func (t *sqlTokens) expression(stop func() bool) (string, int) {
	t.skipWhitespace()
	first := t.current()
	start, end := first.Start, first.Start
	depth := 0
	for t.pos < len(t.tokens) {
		token := t.tokens[t.pos]
		if token.MatchOperatorValue("(") {
			depth++
		}
		if token.MatchOperatorValue(")") {
			depth--
			if depth < 0 {
				break
			}
		}
		if depth == 0 && stop() {
			break
		}
		end = token.End
		t.pos++
	}
	return strings.TrimSpace(t.text[start:end]), t.offset + start
}

// skippingIndex reads `INDEX <name> <expression> TYPE <type> [GRANULARITY
// <n>]`.
//
// The expression and the type are both kept as source text: an expression is
// arbitrary, and a type carries its own parameters -- `set(100)`,
// `bloom_filter(0.01)`, `tokenbf_v1(256, 2, 0)` -- so neither survives being
// rebuilt from tokens. GRANULARITY is optional and, when present, a positive
// unsigned 64-bit number; a missing one is zero here, which the operation
// reads as ClickHouse defines it: one granule.
func (t *sqlTokens) skippingIndex() (*chast.AddSkippingIndex, error) {
	t.next() // INDEX
	name, err := t.identifier()
	if err != nil {
		return nil, fmt.Errorf("expected an index name after ADD INDEX: %w", err)
	}
	expression, start := t.expression(func() bool { return t.keyword("TYPE") })
	if expression == "" {
		return nil, fmt.Errorf("expected an indexed expression after ADD INDEX %s at position %d", name, start)
	}
	t.skipWhitespace()
	if !t.keyword("TYPE") {
		token := t.current()
		return nil, fmt.Errorf("expected TYPE after the expression of index %s: %w", name,
			fmt.Errorf("expected %s, got %s at position %d", lexer.TokenIdentifier, token.Type, t.position(token)))
	}
	t.pos++
	indexType, typeStart := t.expression(func() bool { return t.keyword("GRANULARITY") })
	if indexType == "" {
		return nil, fmt.Errorf("expected an index type after TYPE at position %d", typeStart)
	}
	operation := &chast.AddSkippingIndex{Name: name, Expression: expression, IndexType: indexType}
	t.skipWhitespace()
	if !t.keyword("GRANULARITY") {
		return operation, nil
	}
	t.pos++
	granularity, err := t.identifier()
	if err != nil {
		return nil, fmt.Errorf("expected a value after GRANULARITY on index %s: %w", name, err)
	}
	parsed, err := strconv.ParseUint(granularity, 10, 64)
	if err != nil || parsed == 0 {
		return nil, fmt.Errorf("GRANULARITY on index %s must be a positive number, got %q", name, granularity)
	}
	operation.Granularity = parsed
	t.skipWhitespace()
	if token := t.current(); token.Type != lexer.TokenEOF {
		return nil, fmt.Errorf("unexpected %q after index %s at position %d", token.Value, name, t.position(token))
	}
	return operation, nil
}

// viewStorage is the storage clause the ClickHouse renderer writes for every
// materialized view, as the tokens a read of it yields. A view's storage is
// not modeled, so this is the only one a schema file may state: reading
// another as this one would create the view with a storage it did not ask
// for.
var viewStorage = []string{"ENGINE", "=", "MergeTree", "ORDER", "BY", "tuple", "(", ")"}

// viewStorage refuses a storage clause other than the one the renderer
// writes: keywords in either case, the engine and function names as
// ClickHouse spells them.
func (t *sqlTokens) viewStorage(view string) error {
	var storage []string
	for _, token := range t.tokens {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			storage = append(storage, token.Value)
		}
	}
	if !sameViewStorage(storage) {
		return fmt.Errorf(
			"%w: materialized view %s: storage %q is not modeled; a ClickHouse schema file may state only %q, the storage Ptah creates a materialized view with",
			ptaherr.ErrUnsupportedFeature, view, strings.Join(storage, " "), strings.Join(viewStorage, " "),
		)
	}
	return nil
}

func sameViewStorage(storage []string) bool {
	if len(storage) != len(viewStorage) {
		return false
	}
	for i, token := range storage {
		want := viewStorage[i]
		switch want {
		case "ENGINE", "ORDER", "BY":
			if !strings.EqualFold(token, want) {
				return false
			}
		default:
			if token != want {
				return false
			}
		}
	}
	return true
}
