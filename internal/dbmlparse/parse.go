package dbmlparse

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
)

// Options carries what the caller knows that the document does not.
type Options struct {
	// File is the name diagnostics quote. Empty leaves them line and column
	// only, which is what an in-memory document has.
	File string
}

// Parse reads a DBML document into Ptah's schema model.
//
// The whole document is read before anything is returned, so a file with two
// mistakes reports the first rather than a cascade -- and the position it
// reports is the token's own, not the parser's recovery point.
func Parse(source string, opts Options) (*goschema.Database, error) {
	p := &parser{lex: newLexer(source), file: opts.File, db: &goschema.Database{}}
	if err := p.prime(); err != nil {
		return nil, err
	}
	for p.tok.kind != tokenEOF {
		if err := p.declaration(); err != nil {
			return nil, err
		}
	}
	return p.db, nil
}

type parser struct {
	lex  *lexer
	file string
	tok  token
	db   *goschema.Database
	// tables indexes the struct name a table was recorded under, so a column
	// can find the table it belongs to without a second pass.
	tableStruct map[string]string
}

// prime reads the first token.
func (p *parser) prime() error {
	p.tableStruct = make(map[string]string)
	return p.advance()
}

func (p *parser) advance() error {
	next, err := p.lex.next()
	if err != nil {
		return p.wrap(err)
	}
	p.tok = next
	return nil
}

// wrap prefixes a lexer error with the file, since the lexer knows only the
// position.
func (p *parser) wrap(err error) error {
	if p.file == "" {
		return err
	}
	return fmt.Errorf("%s:%w", p.file, err)
}

// errorf reports a diagnostic at the current token.
func (p *parser) errorf(format string, args ...any) error {
	return fmt.Errorf("%s: %s", p.tok.position(p.file), fmt.Sprintf(format, args...))
}

// declaration reads one top-level construct.
func (p *parser) declaration() error {
	if p.tok.kind != tokenWord {
		return p.errorf("expected a declaration, found %s", p.tok.describe())
	}
	switch strings.ToLower(p.tok.text) {
	case "table":
		return p.table()
	case "enum":
		return p.enum()
	case "ref":
		return p.ref()
	case "project", "tablegroup", "note":
		return p.skipPresentation()
	default:
		return p.errorf("unknown declaration %q", p.tok.text)
	}
}

// skipPresentation consumes a construct that describes the diagram rather than
// the database.
//
// Skipped rather than refused: a Project block or a TableGroup is legitimate
// DBML that says nothing about schema state, and failing on one would make Ptah
// reject documents dbdiagram itself writes. What is lost is reported by the
// caller's loss policy rather than here.
func (p *parser) skipPresentation() error {
	for p.tok.kind != tokenEOF && !p.isPunct("{") {
		if err := p.advance(); err != nil {
			return err
		}
	}
	if p.tok.kind == tokenEOF {
		return nil
	}
	return p.skipBlock()
}

// skipBlock consumes a balanced { ... } run.
func (p *parser) skipBlock() error {
	depth := 0
	for p.tok.kind != tokenEOF {
		switch {
		case p.isPunct("{"):
			depth++
		case p.isPunct("}"):
			depth--
		}
		if err := p.advance(); err != nil {
			return err
		}
		if depth == 0 {
			return nil
		}
	}
	return p.errorf("unterminated block")
}

// isPunct reports whether the current token is the given punctuation.
func (p *parser) isPunct(text string) bool {
	return p.tok.kind == tokenPunct && p.tok.text == text
}

// isWord reports whether the current token is the given keyword, case-insensitively.
func (p *parser) isWord(text string) bool {
	return p.tok.kind == tokenWord && strings.EqualFold(p.tok.text, text)
}

// expectPunct consumes one required punctuation token.
func (p *parser) expectPunct(text string) error {
	if !p.isPunct(text) {
		return p.errorf("expected %q, found %s", text, p.tok.describe())
	}
	return p.advance()
}

// name reads an identifier, bare or quoted.
func (p *parser) name() (string, error) {
	if p.tok.kind != tokenWord && p.tok.kind != tokenQuoted {
		return "", p.errorf("expected a name, found %s", p.tok.describe())
	}
	value := p.tok.text
	return value, p.advance()
}

// qualifiedName reads `name` or `schema.name`.
func (p *parser) qualifiedName() (schema, name string, err error) {
	first, err := p.name()
	if err != nil {
		return "", "", err
	}
	if !p.isPunct(".") {
		return "", first, nil
	}
	if err := p.advance(); err != nil {
		return "", "", err
	}
	second, err := p.name()
	if err != nil {
		return "", "", err
	}
	return first, second, nil
}
