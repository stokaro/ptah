package sqlitetable

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// sqlVirtualTable names the owner's SQL pattern, which each claim carries
// back.
const sqlVirtualTable = "virtual-table"

// VirtualDirective is how a SQLite document's header says it does not
// describe virtual tables: `-- ptah:not-described virtual_table`. The
// directive covers the whole kind and takes no name.
const VirtualDirective coverage.Kind = "virtual_table"

// SQL returns the SQLite owner's contributions to the SQL frontend.
//
// The first reads `CREATE VIRTUAL TABLE [IF NOT EXISTS] name USING
// module[(arguments)]` in a document of the sqlite target or of none, as a
// table the statement declares ([sqlext.TableDeclaration]) whose virtual
// declaration ([DesiredVirtual]) defines it. The arguments are kept verbatim,
// as the text between the module's outer parentheses: they are the module's,
// not a column list, and SQLite stores and compares them as written.
//
// The second is a SQLite document's claim about virtual tables and table
// options. SQLite SQL spells a virtual table as CREATE VIRTUAL TABLE, so a
// table declared with CREATE TABLE is an ordinary one, and a document that
// leaves a live virtual table out asks for it to go, unless its header
// declines virtual tables with [VirtualDirective]. Every CREATE TABLE can end
// in STRICT and WITHOUT ROWID, so a table declared without them has neither.
// A document that names no target reads the syntax but makes no claim, so the
// claim is an extension of its own, with no syntax.
func SQL() []sqlext.Extension {
	return []sqlext.Extension{
		{
			Owner:    Owner,
			Targets:  []string{platform.SQLite},
			Patterns: []sqlext.Pattern{{Name: sqlVirtualTable, Site: sqlext.Statement, Words: []string{"CREATE", "VIRTUAL", "TABLE"}}},
			Parser:   sqlParser{},
		},
		{
			Owner:    Owner,
			Targets:  []string{platform.SQLite},
			Header:   []coverage.Kind{VirtualDirective},
			Coverage: documentClaim,
		},
	}
}

// documentClaim is a SQLite document's claim about its tables: what
// [virtualClaim] says about virtual tables, and that a table's options are
// the ones its CREATE TABLE states.
func documentClaim(limits []coverage.Object) (schemaext.Coverage, error) {
	virtual, err := virtualClaim(limits)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	options, err := OptionsCoverage()
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return virtual.Combine(options)
}

// virtualClaim is a SQLite document's claim about virtual tables: complete,
// or none when its header declined them. The directive covers the whole
// kind: a named one is refused rather than widened to every virtual table or
// narrowed to a name the comparison may spell differently.
func virtualClaim(limits []coverage.Object) (schemaext.Coverage, error) {
	knowledge := schemaext.Knowledge{State: schemaext.Complete}
	for _, limit := range limits {
		if !limit.WholeKind() {
			return schemaext.Coverage{}, fmt.Errorf("%w: ptah:not-described %s names %q; the directive takes no name in a SQLite document",
				ptaherr.ErrUnsupportedFeature, VirtualDirective, limit.Name)
		}
		knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the document's header says it does not describe virtual tables"}
	}
	return VirtualCoverage(schemaext.Desired, knowledge, nil)
}

// sqlParser reads the CREATE VIRTUAL TABLE statements the owner's pattern
// claims.
type sqlParser struct{}

// ParseSQL reads every claim of one document, each to the table it declares.
// A malformed statement is refused with the position in the document.
func (sqlParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		if claim.Pattern != sqlVirtualTable {
			return sqlext.Result{}, fmt.Errorf("%w: SQLite SQL pattern %q", schemaext.ErrInvalidValue, claim.Pattern)
		}
		table, err := newVirtualTokens(request.Target, claim).declaration()
		if err != nil {
			return sqlext.Result{}, err
		}
		values = append(values, sqlext.Value{Table: table})
	}
	return sqlext.Result{Values: values}, nil
}

// virtualTokens is a claimed statement as the frontend's lexer reads it for
// the document's target, positioned against the document. It keeps the
// whitespace, because a qualified name ends at the first space.
type virtualTokens struct {
	text   string
	offset int
	tokens []lexer.Token
	pos    int
}

func newVirtualTokens(target string, claim sqlext.Claim) *virtualTokens {
	options := lexer.Options{}
	if target != "" {
		options = dialectlexer.Options(target)
	}
	scan := lexer.NewLexerWithOptions(claim.Text, options)
	tokens := &virtualTokens{text: claim.Text, offset: claim.Offset}
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		tokens.tokens = append(tokens.tokens, token)
	}
	return tokens
}

// current is the token at the cursor, an EOF token at the end of the
// statement.
func (t *virtualTokens) current() lexer.Token {
	if t.pos >= len(t.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: len(t.text), End: len(t.text)}
	}
	return t.tokens[t.pos]
}

func (t *virtualTokens) advance() {
	t.pos++
}

// skipSpace steps over whitespace, comments, and the version-guarded spans
// the frontend steps over too.
func (t *virtualTokens) skipSpace() {
	for {
		switch t.current().Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenUnknown:
			t.advance()
		default:
			return
		}
	}
}

// position is where token stands in the document.
func (t *virtualTokens) position(token lexer.Token) int {
	return t.offset + token.Start
}

// expect consumes the identifier word, whatever its letter case.
func (t *virtualTokens) expect(word string) error {
	t.skipSpace()
	current := t.current()
	if current.Type != lexer.TokenIdentifier {
		return fmt.Errorf("expected %s, got %s at position %d", lexer.TokenIdentifier, current.Type, t.position(current))
	}
	if !strings.EqualFold(current.Value, word) {
		return fmt.Errorf("expected '%s', got '%s' at position %d", word, current.Value, t.position(current))
	}
	t.advance()
	return nil
}

// declaration reads the statement: CREATE VIRTUAL TABLE, IF NOT EXISTS, the
// table's name, USING, the module and its arguments, and nothing after them.
func (t *virtualTokens) declaration() (*sqlext.TableDeclaration, error) {
	for _, word := range []string{"CREATE", "VIRTUAL", "TABLE"} {
		if err := t.expect(word); err != nil {
			return nil, err
		}
	}
	t.skipSpace()
	ifNotExists, err := t.ifNotExists()
	if err != nil {
		return nil, err
	}
	t.skipSpace()
	name, err := t.qualifiedIdentifier("virtual table name")
	if err != nil {
		return nil, err
	}
	if err := t.expect("USING"); err != nil {
		return nil, err
	}
	t.skipSpace()
	module, err := t.qualifiedIdentifier("virtual table module")
	if err != nil {
		return nil, err
	}
	arguments, err := t.arguments()
	if err != nil {
		return nil, err
	}
	t.skipSpace()
	if rest := t.current(); rest.Type != lexer.TokenEOF {
		return nil, fmt.Errorf("unexpected %q after the virtual table declaration at position %d", rest.Value, t.position(rest))
	}
	facets, err := schemaext.Facets{}.With(&DesiredVirtual{Virtual: Virtual{Module: module, Arguments: arguments}})
	if err != nil {
		return nil, err
	}
	return &sqlext.TableDeclaration{Name: name, IfNotExists: ifNotExists, Facets: facets}, nil
}

// ifNotExists consumes an IF NOT EXISTS at the cursor, and reports whether
// there was one.
func (t *virtualTokens) ifNotExists() (bool, error) {
	current := t.current()
	if !current.MatchIdentifierValue("IF") {
		return false, nil
	}
	t.advance()
	if err := t.expect("NOT"); err != nil {
		return false, fmt.Errorf("expected NOT after IF: %w", err)
	}
	if err := t.expect("EXISTS"); err != nil {
		return false, fmt.Errorf("expected EXISTS after IF NOT: %w", err)
	}
	return true, nil
}

// qualifiedIdentifier reads a name, its parts joined by dots that touch
// them, each part as written: quotes and brackets are kept.
func (t *virtualTokens) qualifiedIdentifier(label string) (string, error) {
	first, err := t.identifier()
	if err != nil {
		return "", fmt.Errorf("expected %s: %w", label, err)
	}
	var name strings.Builder
	name.WriteString(first)
	for current := t.current(); current.MatchOperatorValue("."); current = t.current() {
		name.WriteString(".")
		t.advance()
		t.skipSpace()
		part, err := t.identifier()
		if err != nil {
			return "", fmt.Errorf("expected identifier after '.' in %s: %w", label, err)
		}
		name.WriteString(part)
	}
	return name.String(), nil
}

// identifier reads one name part: a word, a double-quoted name, or a
// bracketed one.
func (t *virtualTokens) identifier() (string, error) {
	current := t.current()
	if current.MatchOperatorValue("[") {
		return t.bracketed()
	}
	if current.Type != lexer.TokenIdentifier && (current.Type != lexer.TokenString || !strings.HasPrefix(current.Value, `"`)) {
		return "", fmt.Errorf("expected identifier, got %s at position %d", current.Type, t.position(current))
	}
	t.advance()
	return current.Value, nil
}

// bracketed reads a bracketed name, `]]` standing for a bracket inside it.
func (t *virtualTokens) bracketed() (string, error) {
	start := t.position(t.current())
	var value strings.Builder
	value.WriteString(t.current().Value)
	t.advance()
	for {
		current := t.current()
		if current.Type == lexer.TokenEOF {
			return "", fmt.Errorf("unterminated bracketed identifier at position %d", start)
		}
		value.WriteString(current.Value)
		t.advance()
		if !current.MatchOperatorValue("]") {
			continue
		}
		if next := t.current(); !next.MatchOperatorValue("]") {
			return value.String(), nil
		}
		value.WriteString(t.current().Value)
		t.advance()
	}
}

// arguments returns the text between the module's outer parentheses,
// verbatim and trimmed, or "" when the module takes none. Nesting and quoting
// are tracked rather than parsed: an argument list may carry parentheses
// inside a quoted default and commas inside a tokenizer string, and both
// belong to the text SQLite stores.
func (t *virtualTokens) arguments() (string, error) {
	t.skipSpace()
	open := t.current()
	if !open.MatchOperatorValue("(") {
		return "", nil
	}
	depth := 0
	for current := t.current(); current.Type != lexer.TokenEOF; current = t.current() {
		t.advance()
		switch {
		case current.MatchOperatorValue("("):
			depth++
		case current.MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return strings.TrimSpace(t.text[open.End:current.Start]), nil
			}
		}
	}
	return "", fmt.Errorf("unterminated virtual table arguments at position %d", t.offset+open.End)
}
