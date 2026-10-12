package mysqlsource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// The names of the owner's SQL patterns, which each claim carries back.
const (
	sqlCharset      = "charset"
	sqlCharacterSet = "character-set"
	sqlOnUpdate     = "on-update"
)

// SQL is the owner's contribution to the SQL frontend: a column's character
// set, `CHARSET name` or `CHARACTER SET name`, and its `ON UPDATE
// <expression>`, read as the column's [mysqlschema.ColumnSettings]. The
// patterns are read in a MySQL or MariaDB document and in one that names no
// target, and a document of another target refuses them as column attributes
// it does not read.
//
// The options of one column share one facet, so the owner combines the claims
// of one column definition: a clause written twice keeps the last, as MySQL
// does, and the definition's last option carries the settings while the
// others add nothing. An option ends where the next column clause begins, so
// the patterns list no stop words of their own.
func SQL() sqlext.Extension {
	return sqlext.Extension{
		Owner:   mysqlschema.Owner,
		Targets: mysqlschema.Targets(),
		Facets:  []schemaext.Kind{mysqlschema.ColumnSettingsKind},
		Patterns: []sqlext.Pattern{
			{Name: sqlCharset, Site: sqlext.ColumnOption, Words: []string{"CHARSET"}},
			{Name: sqlCharacterSet, Site: sqlext.ColumnOption, Words: []string{"CHARACTER", "SET"}},
			{Name: sqlOnUpdate, Site: sqlext.ColumnOption, Words: []string{"ON", "UPDATE"}},
		},
		Parser: sqlParser{},
	}
}

// sqlParser reads the column options the owner's patterns claim.
type sqlParser struct{}

// ParseSQL reads every claim of one document. The claims of one column
// definition are combined, and the definition's last claim carries the
// settings they leave.
func (sqlParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, len(request.Claims))
	settings := make(map[int]mysqlschema.ColumnSettings)
	last := make(map[int]int)
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		combined, err := readColumnOption(request.Target, claim, settings[claim.Definition])
		if err != nil {
			return sqlext.Result{}, err
		}
		settings[claim.Definition] = combined
		last[claim.Definition] = i
	}
	for definition, i := range last {
		facets, err := mysqlschema.WithColumnSettings(schemaext.Facets{}, settings[definition])
		if err != nil {
			return sqlext.Result{}, err
		}
		values[i] = sqlext.Value{Facets: facets}
	}
	return sqlext.Result{Values: values}, nil
}

// readColumnOption reads one claim onto the settings its column carries so
// far.
func readColumnOption(target string, claim sqlext.Claim, settings mysqlschema.ColumnSettings) (mysqlschema.ColumnSettings, error) {
	tokens := newOptionTokens(target, claim)
	switch claim.Pattern {
	case sqlCharset:
		tokens.next() // CHARSET
		name, err := tokens.name()
		if err != nil {
			return mysqlschema.ColumnSettings{}, fmt.Errorf("expected charset name: %w", err)
		}
		settings.Charset = name
	case sqlCharacterSet:
		tokens.next() // CHARACTER
		tokens.next() // SET
		current := tokens.current()
		if current.Type != lexer.TokenIdentifier {
			return mysqlschema.ColumnSettings{}, fmt.Errorf("expected a character set name after CHARACTER SET at position %d", tokens.position())
		}
		settings.Charset = current.Value
		tokens.next()
	case sqlOnUpdate:
		tokens.next() // ON
		tokens.next() // UPDATE
		expression := strings.TrimSpace(tokens.rest())
		if expression == "" {
			return mysqlschema.ColumnSettings{}, fmt.Errorf("expected an expression after ON UPDATE at position %d", tokens.position())
		}
		settings.OnUpdate = expression
		return settings, nil
	default:
		return mysqlschema.ColumnSettings{}, fmt.Errorf("%w: MySQL SQL pattern %q", schemaext.ErrInvalidValue, claim.Pattern)
	}
	if rest := tokens.current(); rest.Type != lexer.TokenEOF {
		return mysqlschema.ColumnSettings{}, fmt.Errorf("unexpected %q after the character set at position %d", rest.Value, tokens.position())
	}
	return settings, nil
}

// optionTokens is a claimed option as the frontend's lexer reads it for the
// document's target, positioned against the document.
type optionTokens struct {
	text   string
	offset int
	tokens []lexer.Token
	pos    int
}

func newOptionTokens(target string, claim sqlext.Claim) *optionTokens {
	options := lexer.Options{}
	if target != "" {
		options = dialectlexer.Options(target)
	}
	scan := lexer.NewLexerWithOptions(claim.Text, options)
	tokens := &optionTokens{text: claim.Text, offset: claim.Offset}
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		tokens.tokens = append(tokens.tokens, token)
	}
	return tokens
}

// current is the token at the cursor, an EOF token at the end of the option.
func (t *optionTokens) current() lexer.Token {
	if t.pos >= len(t.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: len(t.text), End: len(t.text)}
	}
	return t.tokens[t.pos]
}

// next steps past the token at the cursor and the whitespace, comments and
// version-guarded spans after it, as the frontend steps over them.
func (t *optionTokens) next() {
	t.pos++
	t.skipSpace()
}

// skipSpace steps over whitespace, comments and version-guarded spans at the
// cursor.
func (t *optionTokens) skipSpace() {
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
func (t *optionTokens) position() int {
	return t.offset + t.current().Start
}

// rest is the option's text from the cursor to its end.
func (t *optionTokens) rest() string {
	return t.text[t.current().Start:]
}

// name reads a name as the frontend reads an identifier: a word, a
// double-quoted name, or a bracketed one, as written.
func (t *optionTokens) name() (string, error) {
	current := t.current()
	if current.MatchOperatorValue("[") {
		return t.bracketed()
	}
	if current.Type != lexer.TokenIdentifier && (current.Type != lexer.TokenString || !strings.HasPrefix(current.Value, `"`)) {
		return "", fmt.Errorf("expected identifier, got %s at position %d", current.Type, t.position())
	}
	t.next()
	return current.Value, nil
}

// bracketed reads a bracketed name, `]]` standing for a bracket inside it,
// every token between the brackets kept as written.
func (t *optionTokens) bracketed() (string, error) {
	start := t.position()
	var value strings.Builder
	value.WriteString(t.current().Value)
	t.pos++
	for {
		current := t.current()
		if current.Type == lexer.TokenEOF {
			return "", fmt.Errorf("unterminated bracketed identifier at position %d", start)
		}
		value.WriteString(current.Value)
		t.pos++
		if !current.MatchOperatorValue("]") {
			continue
		}
		if next := t.current(); !next.MatchOperatorValue("]") {
			t.skipSpace()
			return value.String(), nil
		}
		value.WriteString(t.current().Value)
		t.pos++
	}
}
