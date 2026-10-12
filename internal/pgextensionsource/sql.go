package pgextensionsource

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// The names of the owner's SQL patterns, which each claim carries back.
const (
	sqlCreate  = "create-extension"
	sqlComment = "comment-on-extension"
	sqlDrop    = "drop-extension"
	sqlAlter   = "alter-extension"
)

// SQL is the extension owner's contribution to the SQL frontend: the CREATE
// EXTENSION, COMMENT ON EXTENSION, DROP EXTENSION and ALTER EXTENSION
// statements of a document that names a PostgreSQL-family target, or names
// none. A document of another target reads them through the shared grammar,
// which refuses them.
//
// A CREATE declares an extension, with no target scope, as a declaration
// that names none in any other source. A COMMENT amends the extension a
// CREATE before it declared, in the same document or in an earlier one of
// the load. The reading is syntax only: a COMMENT on an extension nothing
// declared still reads, as the amendment it states, and the reader of the
// load refuses it. A statement read alone, as migration lint reads each one,
// is therefore never refused for what other statements declare.
//
// A DROP or an ALTER is a change a migration states: it reads as the
// operation it performs, which lint and safety classify, and declares and
// amends nothing, so a schema document's reader refuses it.
//
// Its claim is the one every source makes: a document describes every
// extension, and one it leaves out is absent.
func SQL() sqlext.Extension {
	return sqlext.Extension{
		Owner:   pgextension.Owner,
		Targets: Family(),
		Kinds:   []schemaext.Kind{pgextension.ExtensionKind},
		Patterns: []sqlext.Pattern{
			{Name: sqlCreate, Site: sqlext.Statement, Words: []string{"CREATE", "EXTENSION"}},
			{Name: sqlComment, Site: sqlext.Statement, Words: []string{"COMMENT", "ON", "EXTENSION"}},
			{Name: sqlDrop, Site: sqlext.Statement, Words: []string{"DROP", "EXTENSION"}},
			{Name: sqlAlter, Site: sqlext.Statement, Words: []string{"ALTER", "EXTENSION"}},
		},
		Parser: sqlParser{},
		Coverage: func(sqlext.CoverageRequest) (schemaext.Coverage, error) {
			return pgextension.CompleteCoverage(schemaext.Desired)
		},
	}
}

// sqlParser reads the statements the owner's patterns claim.
type sqlParser struct{}

// ParseSQL reads every claim of one document, in order, so a COMMENT finds
// the extension a CREATE before it declared.
func (sqlParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	known := make(map[string]schemaext.Object, len(request.Declared))
	for _, object := range request.Declared {
		known[object.Ref.Name.Normalized] = object
	}
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		tokens := newStatementTokens(request.Target, claim)
		var value sqlext.Value
		var err error
		switch claim.Pattern {
		case sqlCreate:
			value, err = tokens.create()
		case sqlComment:
			value, err = tokens.comment(known)
		case sqlDrop:
			value, err = tokens.drop()
		case sqlAlter:
			value, err = tokens.alter()
		default:
			err = fmt.Errorf("%w: PostgreSQL extension SQL pattern %q", schemaext.ErrInvalidValue, claim.Pattern)
		}
		if err != nil {
			return sqlext.Result{}, err
		}
		for _, object := range slices.Concat(value.Objects, value.Replaces) {
			known[object.Ref.Name.Normalized] = object
		}
		values = append(values, value)
	}
	return sqlext.Result{Values: values}, nil
}

// statementTokens is a claimed statement as the frontend's lexer reads it for
// the document's target, positioned against the document.
type statementTokens struct {
	target  string
	options lexer.Options
	offset  int
	end     int
	tokens  []lexer.Token
	pos     int
}

func newStatementTokens(target string, claim sqlext.Claim) *statementTokens {
	target = platform.NormalizeDialect(target)
	options := lexer.Options{}
	if target != "" {
		options = dialectlexer.Options(target)
	}
	scan := lexer.NewLexerWithOptions(claim.Text, options)
	tokens := &statementTokens{target: target, options: options, offset: claim.Offset, end: len(claim.Text)}
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment:
		default:
			tokens.tokens = append(tokens.tokens, token)
		}
	}
	if target == "" {
		// A document that names no target reads a string as the shared
		// grammar does there.
		tokens.options.BackslashEscapes = true
	}
	return tokens
}

// current is the token at the cursor, an EOF token at the end of the
// statement.
func (t *statementTokens) current() lexer.Token {
	if t.pos >= len(t.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: t.end, End: t.end}
	}
	return t.tokens[t.pos]
}

// position is where token stands in the document.
func (t *statementTokens) position(token lexer.Token) int {
	return t.offset + token.Start
}

// words consumes the pattern's opening words, which the claim begins with.
func (t *statementTokens) words(count int) {
	t.pos = min(t.pos+count, len(t.tokens))
}

func (t *statementTokens) keyword(word string) bool {
	token := t.current()
	return token.MatchIdentifierValue(word)
}

// name reads an extension or a schema name: a plain identifier, folded as the
// target folds one, or a double-quoted one, taken as written.
func (t *statementTokens) name(what string) (string, error) {
	token := t.current()
	switch {
	case token.Type == lexer.TokenIdentifier:
		t.pos++
		return t.fold(token.Value), nil
	case token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`):
		t.pos++
		return strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(token.Value, `"`), `"`), `""`, `"`), nil
	default:
		return "", fmt.Errorf("expected %s: expected identifier, got %s at position %d", what, token.Type, t.position(token))
	}
}

// fold is a plain identifier as the target's server reads it.
func (t *statementTokens) fold(value string) string {
	switch t.target {
	case platform.Postgres, platform.YugabyteDB:
		return identifier.ComparisonASCIIInsensitive.IdentityKey(value)
	case platform.CockroachDB:
		return identifier.ComparisonUnicodeInsensitive.IdentityKey(value)
	default:
		return value
	}
}

// stringConstant reads a single-quoted string constant, in any of the forms
// the target's server reads one.
func (t *statementTokens) stringConstant(what string) (string, error) {
	token := t.current()
	if token.Type != lexer.TokenString || strings.HasPrefix(token.Value, `"`) {
		return "", fmt.Errorf("expected a string constant for %s at position %d", what, t.position(token))
	}
	text, ok := lexer.StringValue(token.Value, t.options)
	if !ok {
		return "", fmt.Errorf("the string constant for %s at position %d is not one the server reads", what, t.position(token))
	}
	t.pos++
	return text, nil
}

// create reads `CREATE EXTENSION [IF NOT EXISTS] name [WITH] [SCHEMA schema]
// [VERSION version]`, the options in either order.
func (t *statementTokens) create() (sqlext.Value, error) {
	t.words(2)
	var declared pgextension.DesiredExtension
	if t.keyword("IF") {
		t.pos++
		if !t.keyword("NOT") {
			return sqlext.Value{}, fmt.Errorf("expected NOT EXISTS after IF at position %d", t.position(t.current()))
		}
		t.pos++
		if !t.keyword("EXISTS") {
			return sqlext.Value{}, fmt.Errorf("expected EXISTS after IF NOT at position %d", t.position(t.current()))
		}
		t.pos++
		declared.IfNotExists = true
	}
	name, err := t.name("extension name")
	if err != nil {
		return sqlext.Value{}, err
	}
	if t.keyword("WITH") {
		t.pos++
		if !t.keyword("SCHEMA") && !t.keyword("VERSION") {
			return sqlext.Value{}, fmt.Errorf("expected SCHEMA or VERSION after WITH in CREATE EXTENSION")
		}
	}
	if err := t.createOptions(&declared); err != nil {
		return sqlext.Value{}, err
	}
	object, err := Declare(name, declared)
	if err != nil {
		return sqlext.Value{}, err
	}
	operation := &pgextension.Operation{Name: name, Action: pgextension.Create,
		Schema: declared.Schema, Version: declared.Version, IfNotExists: declared.IfNotExists}
	return sqlext.Value{Statement: operation, Objects: []schemaext.Object{object}}, nil
}

// createOptions reads SCHEMA and VERSION, each at most once.
func (t *statementTokens) createOptions(declared *pgextension.DesiredExtension) error {
	schema, version := false, false
	for {
		token := t.current()
		switch {
		case token.Type == lexer.TokenEOF:
			return nil
		case t.keyword("SCHEMA"):
			if schema {
				return fmt.Errorf("duplicate SCHEMA option in CREATE EXTENSION")
			}
			schema = true
			t.pos++
			value, err := t.name("extension schema")
			if err != nil {
				return err
			}
			declared.Schema = value
		case t.keyword("VERSION"):
			if version {
				return fmt.Errorf("duplicate VERSION option in CREATE EXTENSION")
			}
			version = true
			t.pos++
			value, err := t.version()
			if err != nil {
				return err
			}
			declared.Version = value
		default:
			return fmt.Errorf("unsupported CREATE EXTENSION option %q at position %d", token.Value, t.position(token))
		}
	}
}

// version reads a version: a string constant, or a name written bare or
// double-quoted, taken as written.
func (t *statementTokens) version() (string, error) {
	token := t.current()
	switch {
	case token.Type == lexer.TokenString && !strings.HasPrefix(token.Value, `"`):
		return t.stringConstant("the extension version")
	case token.Type == lexer.TokenString, token.Type == lexer.TokenIdentifier:
		t.pos++
		return strings.Trim(token.Value, `"`), nil
	default:
		return "", fmt.Errorf("expected extension version, got %s at position %d", token.Type, t.position(token))
	}
}

// comment reads `COMMENT ON EXTENSION name IS 'text'`. It amends the
// declaration known holds for the extension; without one it states the
// amendment alone, which the reader of the load refuses.
func (t *statementTokens) comment(known map[string]schemaext.Object) (sqlext.Value, error) {
	t.words(3)
	name, err := t.name("extension name")
	if err != nil {
		return sqlext.Value{}, err
	}
	if !t.keyword("IS") {
		return sqlext.Value{}, fmt.Errorf("expected IS after COMMENT ON EXTENSION %s at position %d", name, t.position(t.current()))
	}
	t.pos++
	text, err := t.stringConstant("the extension comment")
	if err != nil {
		return sqlext.Value{}, err
	}
	if token := t.current(); token.Type != lexer.TokenEOF {
		return sqlext.Value{}, fmt.Errorf("unexpected %q after COMMENT ON EXTENSION %s at position %d", token.Value, name, t.position(token))
	}
	amended := pgextension.DesiredExtension{Comment: text}
	var targets []string
	if held, found := known[pgextension.Ref(name).Name.Normalized]; found {
		if declared, ok := held.Value.(*pgextension.DesiredExtension); ok {
			amended = *declared.Copy()
			amended.Comment = text
		}
		targets = held.Targets
	}
	object, err := Declare(name, amended)
	if err != nil {
		return sqlext.Value{}, err
	}
	object.Targets = targets
	operation := &pgextension.CommentOperation{Name: name, Comment: text}
	return sqlext.Value{Statement: operation, Replaces: []schemaext.Object{object}}, nil
}

// drop reads `DROP EXTENSION [IF EXISTS] name [CASCADE | RESTRICT]`.
// PostgreSQL also takes several names in one statement; the operation drops
// one, so that form is refused rather than read as part of itself.
func (t *statementTokens) drop() (sqlext.Value, error) {
	t.words(2)
	operation := &pgextension.Operation{Action: pgextension.Drop}
	if t.keyword("IF") {
		t.pos++
		if !t.keyword("EXISTS") {
			return sqlext.Value{}, fmt.Errorf("expected EXISTS after IF at position %d", t.position(t.current()))
		}
		t.pos++
		operation.IfExists = true
	}
	name, err := t.name("extension name")
	if err != nil {
		return sqlext.Value{}, err
	}
	operation.Name = name
	if token := t.current(); token.Type == lexer.TokenOperator && token.Value == "," {
		return sqlext.Value{}, fmt.Errorf("DROP EXTENSION %s, ... at position %d: the extension owner reads one extension per DROP EXTENSION; "+
			"write a statement for each", name, t.position(token))
	}
	switch {
	case t.keyword("CASCADE"):
		t.pos++
		operation.Cascade = true
	case t.keyword("RESTRICT"):
		t.pos++
	}
	if err := t.finished("DROP EXTENSION " + name); err != nil {
		return sqlext.Value{}, err
	}
	return t.operation(operation)
}

// alter reads `ALTER EXTENSION name UPDATE TO version` and `ALTER EXTENSION
// name SET SCHEMA schema`. An UPDATE without TO goes to the control file's
// default version, which the operation cannot name, and the forms that add
// or drop a member are not ones the owner models, so each is refused.
func (t *statementTokens) alter() (sqlext.Value, error) {
	t.words(2)
	name, err := t.name("extension name")
	if err != nil {
		return sqlext.Value{}, err
	}
	operation := &pgextension.Operation{Name: name}
	switch token := t.current(); {
	case t.keyword("UPDATE"):
		t.pos++
		if !t.keyword("TO") {
			return sqlext.Value{}, fmt.Errorf("ALTER EXTENSION %s UPDATE at position %d: the extension owner reads an update to a named version; "+
				"write UPDATE TO the version", name, t.position(token))
		}
		t.pos++
		operation.Action = pgextension.UpdateVersion
		if operation.Version, err = t.version(); err != nil {
			return sqlext.Value{}, err
		}
	case t.keyword("SET"):
		t.pos++
		if !t.keyword("SCHEMA") {
			return sqlext.Value{}, fmt.Errorf("expected SCHEMA after SET in ALTER EXTENSION %s at position %d", name, t.position(t.current()))
		}
		t.pos++
		operation.Action = pgextension.SetSchema
		if operation.Schema, err = t.name("extension schema"); err != nil {
			return sqlext.Value{}, err
		}
	default:
		return sqlext.Value{}, fmt.Errorf("unsupported ALTER EXTENSION %s action %q at position %d: the extension owner reads UPDATE TO and SET SCHEMA",
			name, token.Value, t.position(token))
	}
	if err := t.finished("ALTER EXTENSION " + name); err != nil {
		return sqlext.Value{}, err
	}
	return t.operation(operation)
}

// finished refuses anything after a statement's last clause.
func (t *statementTokens) finished(statement string) error {
	if token := t.current(); token.Type != lexer.TokenEOF {
		return fmt.Errorf("unexpected %q after %s at position %d", token.Value, statement, t.position(token))
	}
	return nil
}

// operation is the value of a statement that changes an extension and
// declares nothing: its operation, valid as the planner's would be.
func (t *statementTokens) operation(operation *pgextension.Operation) (sqlext.Value, error) {
	if err := operation.Validate(); err != nil {
		return sqlext.Value{}, err
	}
	return sqlext.Value{Statement: operation}, nil
}
