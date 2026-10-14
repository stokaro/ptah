package oratypesource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/feature/oratype"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// The SQL patterns, each consulted on an Oracle document alone.
const (
	createDomainPattern      = "create-domain"
	createObjectTypePattern  = "create-object-type"
	replaceObjectTypePattern = "replace-object-type"
)

// SQL reads CREATE DOMAIN [IF NOT EXISTS] and CREATE [OR REPLACE] TYPE ... AS
// [OBJECT] (...) in an Oracle document, so a schema the renderer writes reads
// back. The OBJECT keyword is optional on read, as the shared grammar reads
// the composite form PostgreSQL writes; the renderer writes it.
func SQL() sqlext.Extension {
	targets := []string{platform.Oracle}
	return sqlext.Extension{Owner: oratype.Owner, Targets: targets, Kinds: oratype.Kinds(),
		Patterns: []sqlext.Pattern{
			{Name: createDomainPattern, Site: sqlext.Statement, Words: []string{"CREATE", "DOMAIN"}, Targeted: true},
			{Name: createObjectTypePattern, Site: sqlext.Statement, Words: []string{"CREATE", "TYPE"}, AfterName: []string{"AS"},
				Next: []string{"(", "OBJECT"}, Targeted: true},
			{Name: replaceObjectTypePattern, Site: sqlext.Statement, Words: []string{"CREATE", "OR", "REPLACE", "TYPE"},
				AfterName: []string{"AS"}, Next: []string{"(", "OBJECT"}, Targeted: true},
		},
		Parser:   sqlParser{},
		Coverage: func(sqlext.CoverageRequest) (schemaext.Coverage, error) { return sourceCoverage(nil) },
	}
}

type sqlParser struct{}

// ParseSQL reads each claimed statement in source order.
func (sqlParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	if ctx == nil {
		return sqlext.Result{}, fmt.Errorf("%w: Oracle user type SQL requires a context", schemaext.ErrInvalidValue)
	}
	result := sqlext.Result{Values: make([]sqlext.Value, 0, len(request.Claims))}
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		tokens := newTokens(claim)
		var value sqlext.Value
		var err error
		switch claim.Pattern {
		case createDomainPattern:
			value, err = tokens.createDomain()
		case createObjectTypePattern, replaceObjectTypePattern:
			value, err = tokens.createObjectType()
		default:
			err = fmt.Errorf("%w: Oracle user type SQL pattern %q", schemaext.ErrInvalidValue, claim.Pattern)
		}
		if err != nil {
			return sqlext.Result{}, err
		}
		result.Values = append(result.Values, value)
	}
	return result, ctx.Err()
}

// tokens reads one claimed statement with Oracle's lexical rules. Types and
// expressions keep their source text.
type tokens struct {
	text   string
	offset int
	tokens []lexer.Token
	pos    int
}

func newTokens(claim sqlext.Claim) *tokens {
	t := &tokens{text: claim.Text, offset: claim.Offset}
	scan := lexer.NewLexerWithOptions(t.text, dialectlexer.Options(platform.Oracle))
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

func (t *tokens) current() lexer.Token {
	if t.pos >= len(t.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: len(t.text), End: len(t.text)}
	}
	return t.tokens[t.pos]
}

func (t *tokens) keyword(word string) bool {
	token := t.current()
	return token.MatchIdentifierValue(word)
}

func (t *tokens) operator(word string) bool {
	token := t.current()
	return token.MatchOperatorValue(word)
}

func (t *tokens) position() int { return t.offset + t.current().Start }

func (t *tokens) words(words ...string) error {
	for _, word := range words {
		if !t.keyword(word) {
			return fmt.Errorf("expected %s at position %d", word, t.position())
		}
		t.pos++
	}
	return nil
}

func (t *tokens) punctuation(text string) error {
	if !t.operator(text) {
		return fmt.Errorf("expected %q at position %d", text, t.position())
	}
	t.pos++
	return nil
}

// name reads one name part: an identifier as written, which Oracle folds
// when it resolves it, or a double-quoted one without its quotes.
func (t *tokens) name(label string) (string, error) {
	token := t.current()
	switch {
	case token.Type == lexer.TokenIdentifier:
		t.pos++
		return token.Value, nil
	case token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`):
		if len(token.Value) < 2 || !strings.HasSuffix(token.Value, `"`) {
			return "", fmt.Errorf("expected a closed quoted %s at position %d", label, t.offset+token.Start)
		}
		t.pos++
		return strings.ReplaceAll(token.Value[1:len(token.Value)-1], `""`, `"`), nil
	default:
		return "", fmt.Errorf("expected %s at position %d", label, t.offset+token.Start)
	}
}

// reference reads a name with an optional schema.
func (t *tokens) reference(kind schemaext.Kind) (objectidentity.ID, error) {
	first, err := t.name(oratype.Label(objectidentity.ID{Kind: objectidentity.Kind(kind)}) + " name")
	if err != nil {
		return objectidentity.ID{}, err
	}
	if !t.operator(".") {
		return oratype.Ref(kind, "", first), nil
	}
	t.pos++
	name, err := t.name("name after schema")
	if err != nil {
		return objectidentity.ID{}, err
	}
	return oratype.Ref(kind, first, name), nil
}

// span reads the source text up to a top-level token stop accepts, or the
// end. Parentheses, brackets and CASE ... END nest.
func (t *tokens) span(label string, stop func() bool) (string, error) {
	start, end, depth, cases := t.current().Start, t.current().Start, 0, 0
	for t.current().Type != lexer.TokenEOF {
		if depth == 0 && cases == 0 && stop() {
			break
		}
		switch {
		case t.operator("(") || t.operator("["):
			depth++
		case t.operator(")") || t.operator("]"):
			depth--
		case t.keyword("CASE"):
			cases++
		case t.keyword("END") && cases > 0:
			cases--
		}
		if depth < 0 {
			return "", fmt.Errorf("unexpected %s in %s at position %d", t.current().Value, label, t.position())
		}
		end = t.current().End
		t.pos++
	}
	if depth != 0 || cases != 0 || end == start {
		return "", fmt.Errorf("expected a complete %s at position %d", label, t.offset+start)
	}
	return strings.TrimSpace(t.text[start:end]), nil
}

// clauseStart reports a word that opens a domain clause.
func (t *tokens) clauseStart() bool {
	return t.keyword("NOT") || t.keyword("NULL") || t.keyword("DEFAULT") || t.keyword("CHECK") || t.keyword("CONSTRAINT")
}

// createDomain reads CREATE DOMAIN [IF NOT EXISTS] name AS type, then NOT
// NULL, NULL, DEFAULT and CHECK in any order, each once.
func (t *tokens) createDomain() (sqlext.Value, error) {
	if err := t.words("CREATE", "DOMAIN"); err != nil {
		return sqlext.Value{}, err
	}
	// The guard the renderer writes where the target has existence guards,
	// so a rendered schema reads back.
	if t.keyword("IF") {
		if err := t.words("IF", "NOT", "EXISTS"); err != nil {
			return sqlext.Value{}, err
		}
	}
	ref, err := t.reference(oratype.DomainKind)
	if err != nil {
		return sqlext.Value{}, err
	}
	if err := t.words("AS"); err != nil {
		return sqlext.Value{}, err
	}
	base, err := t.span("domain base type", t.clauseStart)
	if err != nil {
		return sqlext.Value{}, err
	}
	value := &oratype.DesiredDomain{BaseType: base}
	seen := make(map[string]bool)
	for t.current().Type != lexer.TokenEOF {
		clause := strings.ToUpper(t.current().Value)
		if clause == "NOT" {
			clause = "NULL"
		}
		if seen[clause] {
			return sqlext.Value{}, fmt.Errorf("domain %s declares %s more than once", oratype.QualifiedName(ref), clause)
		}
		seen[clause] = true
		if err := t.domainClause(value); err != nil {
			return sqlext.Value{}, err
		}
	}
	object, err := oratype.DesiredObject(ref, value)
	if err != nil {
		return sqlext.Value{}, err
	}
	return sqlext.Value{Statement: oratype.CreateDomainOf(ref, value), Objects: []schemaext.Object{object}}, nil
}

func (t *tokens) domainClause(value *oratype.DesiredDomain) error {
	switch {
	case t.keyword("NOT"):
		if err := t.words("NOT", "NULL"); err != nil {
			return err
		}
		value.NotNull = true
		return nil
	case t.keyword("NULL"):
		t.pos++
		return nil
	case t.keyword("DEFAULT"):
		t.pos++
		// A default is required after the keyword, so a NULL right after it
		// is the default, and DEFAULT NULL is no default at all.
		if t.keyword("NULL") {
			t.pos++
			return nil
		}
		text, err := t.span("domain default", t.clauseStart)
		if err != nil {
			return err
		}
		value.Default, value.DefaultExpr = domainDefault(text)
		return nil
	case t.keyword("CHECK"):
		t.pos++
		if err := t.punctuation("("); err != nil {
			return err
		}
		check, err := t.span("domain CHECK", func() bool { return t.operator(")") })
		if err != nil {
			return err
		}
		value.Check = check
		return t.punctuation(")")
	default:
		return fmt.Errorf("unsupported domain clause %q at position %d", t.current().Value, t.position())
	}
}

// domainDefault splits a DEFAULT the way the model keeps it: a string
// constant becomes a literal value, read as the server reads it, and anything
// else stays SQL. DEFAULT NULL is no default at all.
func domainDefault(text string) (literal, expression string) {
	if strings.EqualFold(text, "NULL") {
		return "", ""
	}
	if len(text) >= 2 && text[0] == '\'' && text[len(text)-1] == '\'' &&
		!strings.Contains(strings.ReplaceAll(text[1:len(text)-1], "''", ""), "'") {
		return strings.ReplaceAll(text[1:len(text)-1], "''", "'"), ""
	}
	return "", text
}

// createObjectType reads CREATE [OR REPLACE] TYPE name AS [OBJECT] (attribute
// type, ...).
func (t *tokens) createObjectType() (sqlext.Value, error) {
	if err := t.words("CREATE"); err != nil {
		return sqlext.Value{}, err
	}
	if t.keyword("OR") {
		if err := t.words("OR", "REPLACE"); err != nil {
			return sqlext.Value{}, err
		}
	}
	if err := t.words("TYPE"); err != nil {
		return sqlext.Value{}, err
	}
	ref, err := t.reference(oratype.ObjectTypeKind)
	if err != nil {
		return sqlext.Value{}, err
	}
	if err := t.words("AS"); err != nil {
		return sqlext.Value{}, err
	}
	if t.keyword("OBJECT") {
		t.pos++
	}
	if err := t.punctuation("("); err != nil {
		return sqlext.Value{}, err
	}
	value := &oratype.DesiredObjectType{}
	for {
		name, err := t.name("object type attribute")
		if err != nil {
			return sqlext.Value{}, err
		}
		typ, err := t.span("attribute type", func() bool { return t.operator(",") || t.operator(")") })
		if err != nil {
			return sqlext.Value{}, err
		}
		value.Attributes = append(value.Attributes, oratype.Attribute{Name: name, Type: typ})
		if !t.operator(",") {
			break
		}
		t.pos++
	}
	if err := t.punctuation(")"); err != nil {
		return sqlext.Value{}, err
	}
	if t.current().Type != lexer.TokenEOF {
		return sqlext.Value{}, fmt.Errorf("unexpected %s at position %d", t.current().Value, t.position())
	}
	object, err := oratype.DesiredObject(ref, value)
	if err != nil {
		return sqlext.Value{}, err
	}
	return sqlext.Value{Statement: oratype.ReplaceObjectTypeOf(ref, value, false), Objects: []schemaext.Object{object}}, nil
}
