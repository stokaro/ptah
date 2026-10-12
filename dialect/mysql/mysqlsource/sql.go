package mysqlsource

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/mysqlindex"
)

// The names of the owner's SQL patterns, which each claim carries back.
const (
	sqlCharset      = "charset"
	sqlCharacterSet = "character-set"
	sqlOnUpdate     = "on-update"
	sqlBlockSize    = "key-block-size"
	sqlWithParser   = "parser"
)

// SQL is the owner's contribution to the SQL frontend.
//
// A column's character set, `CHARSET name` or `CHARACTER SET name`, and its
// `ON UPDATE <expression>` are read as the column's
// [mysqlschema.ColumnSettings]. A key's `KEY_BLOCK_SIZE [=] n` is read as its
// [mysqlschema.IndexBlockSize] hint, on an index or a PRIMARY KEY, and a
// FULLTEXT index's `WITH PARSER name` as its [mysqlschema.DesiredIndex]
// parser, bare or inside the version guard MySQL writes it in. The patterns
// are read in a MySQL or MariaDB document and in one that names no target,
// and a document of another target refuses them as syntax it does not read.
//
// The options of one column or one key share a facet, so the owner combines
// the claims of one element: an option written twice keeps the last, as MySQL
// 8.4 and MariaDB 11.8 do, and the element's last claim carries the facets
// while the others add nothing. An option ends where the element's next
// option begins, so the patterns list no stop words of their own.
//
// Its claim is [BlockSizeCoverage]: a MySQL or MariaDB document states every
// index's block size, so an index it declares without one has none.
func SQL() sqlext.Extension {
	return sqlext.Extension{
		Owner:   mysqlschema.Owner,
		Targets: mysqlschema.Targets(),
		Facets:  []schemaext.Kind{mysqlschema.ColumnSettingsKind, mysqlschema.IndexBlockSizeKind, mysqlschema.IndexKind},
		Patterns: []sqlext.Pattern{
			{Name: sqlCharset, Site: sqlext.ColumnOption, Words: []string{"CHARSET"}},
			{Name: sqlCharacterSet, Site: sqlext.ColumnOption, Words: []string{"CHARACTER", "SET"}},
			{Name: sqlOnUpdate, Site: sqlext.ColumnOption, Words: []string{"ON", "UPDATE"}},
			{Name: sqlBlockSize, Site: sqlext.IndexOption, Words: []string{"KEY_BLOCK_SIZE"}},
			{Name: sqlWithParser, Site: sqlext.IndexOption, Words: []string{"WITH", "PARSER"}},
		},
		Parser:   sqlParser{},
		Coverage: func(sqlext.CoverageRequest) (schemaext.Coverage, error) { return BlockSizeCoverage() },
	}
}

// sqlParser reads the column and key options the owner's patterns claim.
type sqlParser struct{}

// ParseSQL reads every claim of one document. The claims of one element are
// combined, and the element's last claim carries the facets they leave.
func (sqlParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	elements := make(map[int]*elementOptions)
	var order []int
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		element := elements[claim.Definition]
		if element == nil {
			element = &elementOptions{}
			elements[claim.Definition] = element
			order = append(order, claim.Definition)
		}
		if err := element.read(request.Target, claim); err != nil {
			return sqlext.Result{}, err
		}
		element.last = i
	}
	values := make([]sqlext.Value, len(request.Claims))
	for _, definition := range order {
		element := elements[definition]
		facets, err := element.facets()
		if err != nil {
			return sqlext.Result{}, err
		}
		values[element.last] = sqlext.Value{Facets: facets}
	}
	return sqlext.Result{Values: values}, nil
}

// elementOptions are the options one column definition or one key carries so
// far.
type elementOptions struct {
	key       bool
	column    mysqlschema.ColumnSettings
	blockSize uint64
	parser    string
	// last is the element's last claim, which carries its facets.
	last int
}

// read reads one claim onto the options of its element.
func (e *elementOptions) read(target string, claim sqlext.Claim) error {
	switch claim.Pattern {
	case sqlCharset, sqlCharacterSet, sqlOnUpdate:
		settings, err := readColumnOption(target, claim, e.column)
		e.column = settings
		return err
	case sqlBlockSize:
		e.key = true
		size, err := readBlockSize(target, claim)
		e.blockSize = size
		return err
	case sqlWithParser:
		e.key = true
		name, err := readParser(target, claim)
		e.parser = name
		return err
	default:
		return fmt.Errorf("%w: MySQL SQL pattern %q", schemaext.ErrInvalidValue, claim.Pattern)
	}
}

// facets are the facets the element's options leave: a key's block size and
// parser, or a column's settings.
func (e *elementOptions) facets() (schemaext.Facets, error) {
	if !e.key {
		return mysqlschema.WithColumnSettings(schemaext.Facets{}, e.column)
	}
	facets, err := mysqlschema.WithIndexBlockSize(schemaext.Facets{}, e.blockSize)
	if err != nil || e.parser == "" {
		return facets, err
	}
	return mysqlschema.WithIndexOptions(facets, mysqlschema.DesiredIndex{Parser: e.parser})
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
	default: // sqlOnUpdate
		tokens.next() // ON
		tokens.next() // UPDATE
		expression := strings.TrimSpace(tokens.rest())
		if expression == "" {
			return mysqlschema.ColumnSettings{}, fmt.Errorf("expected an expression after ON UPDATE at position %d", tokens.position())
		}
		settings.OnUpdate = expression
		return settings, nil
	}
	if rest := tokens.current(); rest.Type != lexer.TokenEOF {
		return mysqlschema.ColumnSettings{}, fmt.Errorf("unexpected %q after the character set at position %d", rest.Value, tokens.position())
	}
	return settings, nil
}

// readBlockSize reads `KEY_BLOCK_SIZE [=] n`, a hint the target's server
// keeps as written.
func readBlockSize(target string, claim sqlext.Claim) (uint64, error) {
	tokens := newOptionTokens(target, claim)
	start := tokens.position()
	tokens.next() // KEY_BLOCK_SIZE
	if equals := tokens.current(); equals.MatchOperatorValue("=") {
		tokens.next()
	}
	size, err := strconv.ParseUint(tokens.current().Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("KEY_BLOCK_SIZE at position %d: expected a non-negative integer", start)
	}
	if err := mysqlindex.ValidateBlockSize(target, size); err != nil {
		return 0, err
	}
	tokens.next()
	if rest := tokens.current(); rest.Type != lexer.TokenEOF {
		return 0, fmt.Errorf("unexpected %q after the block size at position %d", rest.Value, tokens.position())
	}
	return size, nil
}

// readParser reads `WITH PARSER name`, the plugin a FULLTEXT index tokenizes
// its text with. The name is kept without the backticks a dump writes around
// it.
//
// The clause belongs to FULLTEXT alone, and is refused on any other key
// rather than ignored. MySQL 26.7 and MariaDB 12.3 both answer `ERROR 1064`
// to `KEY k (a) WITH PARSER ngram`, and the renderer writes the clause
// whatever the type, so reading it would write DDL neither engine takes.
func readParser(target string, claim sqlext.Claim) (string, error) {
	tokens := newOptionTokens(target, claim)
	tokens.next() // WITH
	tokens.next() // PARSER
	name, err := tokens.name()
	if err != nil {
		return "", fmt.Errorf("expected a parser name after WITH PARSER: %w", err)
	}
	if claim.IndexType != "FULLTEXT" {
		return "", fmt.Errorf("WITH PARSER belongs to a FULLTEXT index, and this one is %s", describeIndexType(claim.IndexType))
	}
	if rest := tokens.current(); rest.Type != lexer.TokenEOF {
		return "", fmt.Errorf("unexpected %q after the parser name at position %d", rest.Value, tokens.position())
	}
	return unquoteBackticks(name), nil
}

// describeIndexType names a key's type for a diagnostic, and says "an
// ordinary index" rather than printing an empty string for the plain one.
func describeIndexType(indexType string) string {
	if indexType == "" {
		return "an ordinary index"
	}
	return indexType
}

// unquoteBackticks is name without the backticks that quote it, a doubled
// backtick standing for one inside, and name itself when it is not quoted.
func unquoteBackticks(name string) string {
	if len(name) < 2 || name[0] != '`' || name[len(name)-1] != '`' {
		return name
	}
	return strings.ReplaceAll(name[1:len(name)-1], "``", "`")
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
