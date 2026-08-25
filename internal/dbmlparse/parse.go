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
	// OnDiagnostic receives one line per construct the document declares and
	// the schema model cannot carry.
	//
	// A channel rather than a return value, because these are not failures:
	// the document parses, the schema is usable, and something in it described
	// the diagram or held seed rows rather than schema state. A caller that
	// wants them prints them; a caller that does not passes nil and the parse
	// is unchanged. What must NOT happen is the third option -- losing them
	// with nowhere to say so (stokaro/ptah#2065).
	OnDiagnostic func(string)
}

// diagnose reports a loss, if the caller asked to hear about them.
func (o Options) diagnosef(format string, args ...any) {
	if o.OnDiagnostic == nil {
		return
	}
	o.OnDiagnostic(fmt.Sprintf(format, args...))
}

// Parse reads a DBML document into Ptah's schema model.
//
// The whole document is read before anything is returned, so a file with two
// mistakes reports the first rather than a cascade -- and the position it
// reports is the token's own, not the parser's recovery point.
func Parse(source string, opts Options) (*goschema.Database, error) {
	p := &parser{lex: newLexer(source), opts: opts, file: opts.File, db: &goschema.Database{}}
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
	opts Options
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
	case "project", "tablegroup", "note", "sticky note":
		return p.presentation()
	case "tablepartial":
		return p.presentation()
	case "records", "tablerecords":
		return p.records()
	case "use", "reuse", "include", "import":
		// Multi-file composition is refused rather than skipped: a document
		// that pulls in another one describes a schema this parser has not
		// read, and carrying on would hand back a model missing whatever the
		// other file declared -- silently, and looking complete.
		return p.errorf(
			"%s is not supported: this reads one document, so compose the schema in a single file",
			strings.ToLower(p.tok.text))
	default:
		return p.errorf("unknown declaration %q", p.tok.text)
	}
}

// presentation consumes a construct that describes the diagram rather than the
// database, and says so.
//
// Skipped rather than refused: a Project block or a TableGroup is legitimate
// DBML that says nothing about schema state, and failing on one would make Ptah
// reject documents dbdiagram itself writes. Diagnosed rather than dropped: a
// reader who wrote one and finds it absent from the applied schema deserves to
// have been told, and a loss with nowhere to say so is how a document and a
// database quietly stop agreeing (stokaro/ptah#2065).
func (p *parser) presentation() error {
	kind := strings.ToLower(p.tok.text)
	position := p.tok.position(p.file)
	p.opts.diagnosef("%s: %s describes the diagram rather than the database, and is not applied", position, kind)
	return p.skipPresentation()
}

// records consumes a seed-row block and says it was not applied.
//
// One diagnostic per block rather than one per row: a Records block holding a
// thousand rows would otherwise bury everything else the parse reported, and
// the fact a reader needs is that the block exists and did nothing, not how
// large it was.
//
// Refused as data rather than converted: Ptah has managed data with its own
// declaration, keys and safety gates, and quietly turning diagram seed rows
// into rows a migration writes would apply data nobody asked to have applied.
func (p *parser) records() error {
	position := p.tok.position(p.file)
	if err := p.advance(); err != nil {
		return err
	}
	target := ""
	if p.tok.kind == tokenWord || p.tok.kind == tokenQuoted {
		_, name, err := p.qualifiedName()
		if err != nil {
			return err
		}
		target = name
	}
	p.opts.diagnosef(
		"%s: records for %q are seed data, not schema, and are not applied; declare reference data if you want Ptah to manage rows",
		position, target)
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

// skipPresentation consumes a balanced construct without interpreting it.
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
