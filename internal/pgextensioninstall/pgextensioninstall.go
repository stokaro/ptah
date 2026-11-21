// Package pgextensioninstall reads the PostgreSQL statements that change which
// extensions a database holds: CREATE EXTENSION, ALTER EXTENSION ... UPDATE and
// DROP EXTENSION. Each name and version is decoded with the PostgreSQL lexer in
// ptah.run/sqlsyntax/pgsyntax, as the server decodes it: a bare word folded to
// lower case, a quoted or U&"..." identifier, and a string in any of its forms,
// E'...', U&'...' and dollar quotes included.
//
// The dev-database replay guard and migration lint both read these statements
// through [Read], and the two must agree: the guard runs an extension update
// only where lint can say what the update installs, and lint reports a
// statement it cannot read rather than pass over it. Two readers with two
// grammars drift apart, and a statement one accepts and the other skips is an
// install nobody assessed.
package pgextensioninstall

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/sqlsyntax/pgsyntax"
)

// Kind is which statement a [Statement] is.
type Kind int

// The statements [Read] reads.
const (
	// Create is CREATE EXTENSION.
	Create Kind = iota + 1
	// Update is ALTER EXTENSION name UPDATE.
	Update
	// Drop is DROP EXTENSION.
	Drop
)

// Statement is what one extension statement names.
type Statement struct {
	// Kind is which statement it is.
	Kind Kind
	// Name is the extension a Create or an Update names, as the server stores
	// the name.
	Name string
	// Dropped are the extensions a Drop names, in the order written.
	Dropped []string
	// Version is the version a Create or an Update names, and empty when it
	// names none, so the server takes the extension's default version.
	Version string
	// IfNotExists reports CREATE EXTENSION IF NOT EXISTS, which installs
	// nothing where the database holds the extension already.
	IfNotExists bool
	// Cascade reports CREATE EXTENSION ... CASCADE, which also installs each
	// extension the installed version requires and the database lacks, and
	// DROP EXTENSION ... CASCADE, which also drops what depends on the
	// extensions it names.
	Cascade bool
}

// nameLength is the longest name the server stores, in bytes: NAMEDATALEN
// less its terminating byte. A longer name is cut to it.
const nameLength = 63

// Read reads text as one statement, as the server reads it.
//
// ok is false, and err nil, for a statement that is none of CREATE EXTENSION,
// ALTER EXTENSION name UPDATE and DROP EXTENSION, among them every other ALTER
// EXTENSION form. For one of them ok is true, and err says what of it the
// reader could not read: a name or a version it cannot decode, a version
// written as a number, an option the statement does not take, text that
// continues after the statement, a quote that never closes, or a zero byte.
// The statement then carries its Kind alone, so a caller can report it as an
// extension statement that went unread rather than skip it.
//
// One terminating semicolon is accepted, and comments and white space are
// ignored where the server ignores them.
func Read(text string) (statement Statement, ok bool, err error) {
	tokens, scanErr := scan(text)
	r := reader{text: text, tokens: tokens}
	kind := r.kind()
	if kind == 0 {
		return Statement{}, false, nil
	}
	switch {
	case scanErr != nil:
		return Statement{Kind: kind}, true, fmt.Errorf("its text does not close where the server reads it (%w)", scanErr)
	case strings.IndexByte(text, 0) >= 0:
		return Statement{Kind: kind}, true, errors.New("it holds a zero byte")
	}
	r.at = 0
	switch kind {
	case Create:
		statement, err = r.create()
	case Update:
		statement, err = r.update()
	default:
		statement, err = r.drop()
	}
	if err != nil {
		return Statement{Kind: kind}, true, err
	}
	return statement, true, nil
}

// scan returns the tokens of text, and when a construct does not close, the
// tokens before it with the error, so the statement is still recognized.
func scan(text string) ([]pgsyntax.Token, error) {
	tokens, err := pgsyntax.Scan(text, pgsyntax.Options{})
	if err == nil {
		return tokens, nil
	}
	var prefix []pgsyntax.Token
	for offset := 0; offset < len(text); {
		token, nextErr := pgsyntax.Next(text[offset:], pgsyntax.Options{})
		if nextErr != nil || token.End == 0 {
			break
		}
		prefix = append(prefix, pgsyntax.Token{Kind: token.Kind, Start: offset + token.Start, End: offset + token.End})
		offset += token.End
	}
	return prefix, err
}

// reader walks the tokens of one statement. at is the index of the next token
// to read, white space and comments included.
type reader struct {
	text   string
	tokens []pgsyntax.Token
	at     int
}

// kind recognizes the statement from its leading tokens.
func (r *reader) kind() Kind {
	switch {
	case r.words("CREATE", "EXTENSION"):
		return Create
	case r.words("DROP", "EXTENSION"):
		return Drop
	case r.words("ALTER", "EXTENSION"):
		r.skipName()
		if r.words("UPDATE") {
			return Update
		}
	}
	return 0
}

// create reads CREATE EXTENSION [IF NOT EXISTS] name [WITH] [SCHEMA schema |
// VERSION version | CASCADE]..., the options in any order, as the server reads
// them.
func (r *reader) create() (Statement, error) {
	r.words("CREATE", "EXTENSION")
	statement := Statement{Kind: Create, IfNotExists: r.words("IF", "NOT", "EXISTS")}
	name, err := r.name("extension name")
	if err != nil {
		return Statement{}, err
	}
	statement.Name = name
	r.words("WITH")
	versioned := false
	for !r.done() {
		switch {
		case r.words("SCHEMA"):
			if _, err := r.name("schema"); err != nil {
				return Statement{}, err
			}
		case r.words("VERSION"):
			if versioned {
				return Statement{}, errors.New("it names VERSION twice")
			}
			versioned = true
			if statement.Version, err = r.constant(); err != nil {
				return Statement{}, err
			}
		case r.words("CASCADE"):
			if statement.Cascade {
				return Statement{}, errors.New("it names CASCADE twice")
			}
			statement.Cascade = true
		default:
			return Statement{}, r.unexpected("%s is not an option CREATE EXTENSION takes")
		}
	}
	return statement, nil
}

// update reads ALTER EXTENSION name UPDATE [TO version].
func (r *reader) update() (Statement, error) {
	r.words("ALTER", "EXTENSION")
	name, err := r.name("extension name")
	if err != nil {
		return Statement{}, err
	}
	r.words("UPDATE")
	statement := Statement{Kind: Update, Name: name}
	if r.words("TO") {
		if statement.Version, err = r.constant(); err != nil {
			return Statement{}, err
		}
	}
	if !r.done() {
		return Statement{}, r.unexpected("%s follows the version, and ALTER EXTENSION ... UPDATE takes nothing there")
	}
	return statement, nil
}

// drop reads DROP EXTENSION [IF EXISTS] name [, ...] [CASCADE | RESTRICT].
func (r *reader) drop() (Statement, error) {
	r.words("DROP", "EXTENSION")
	r.words("IF", "EXISTS")
	statement := Statement{Kind: Drop}
	for {
		name, err := r.name("extension name")
		if err != nil {
			return Statement{}, err
		}
		statement.Dropped = append(statement.Dropped, name)
		if !r.punctuation(",") {
			break
		}
	}
	statement.Cascade = r.words("CASCADE")
	if !statement.Cascade {
		r.words("RESTRICT")
	}
	if !r.done() {
		return Statement{}, r.unexpected("%s follows the names, and DROP EXTENSION takes only CASCADE or RESTRICT there")
	}
	return statement, nil
}

// skip moves past white space and comments.
func (r *reader) skip() {
	for r.at < len(r.tokens) {
		switch r.tokens[r.at].Kind {
		case pgsyntax.Whitespace, pgsyntax.LineComment, pgsyntax.BlockComment:
			r.at++
		default:
			return
		}
	}
}

// words reads the bare words values, compared without case, and reports
// whether they were next. It reads nothing when they were not.
func (r *reader) words(values ...string) bool {
	start := r.at
	for _, value := range values {
		r.skip()
		if r.at >= len(r.tokens) || r.tokens[r.at].Kind != pgsyntax.Word || !strings.EqualFold(r.tokens[r.at].Text(r.text), value) {
			r.at = start
			return false
		}
		r.at++
	}
	return true
}

// punctuation reads the punctuation value and reports whether it was next.
func (r *reader) punctuation(value string) bool {
	r.skip()
	if r.at < len(r.tokens) && r.tokens[r.at].Kind == pgsyntax.Punctuation && r.tokens[r.at].Text(r.text) == value {
		r.at++
		return true
	}
	return false
}

// done reports whether nothing but a terminating semicolon is left.
func (r *reader) done() bool {
	start := r.at
	r.punctuation(";")
	r.skip()
	if r.at >= len(r.tokens) {
		return true
	}
	r.at = start
	r.skip()
	return false
}

// unexpected is the error for the token that stands where the statement takes
// nothing: format, which names it with %s, or text after a semicolon, which is
// a second statement the server runs.
func (r *reader) unexpected(format string) error {
	if r.punctuation(";") {
		return errors.New("text follows its semicolon")
	}
	return fmt.Errorf(format, r.next())
}

// next names the next token for a message.
func (r *reader) next() string {
	r.skip()
	if r.at >= len(r.tokens) {
		return "the end of the statement"
	}
	return fmt.Sprintf("%q", r.tokens[r.at].Text(r.text))
}

// name reads a name: a bare word folded to lower case, or a quoted or U&"..."
// identifier as written, each cut to the length the server stores. what names
// the name in the error.
func (r *reader) name(what string) (string, error) {
	r.skip()
	if r.at >= len(r.tokens) {
		return "", fmt.Errorf("it names no %s", what)
	}
	token := r.tokens[r.at]
	switch token.Kind {
	case pgsyntax.Word:
		r.at++
		return truncated(folded(token.Text(r.text))), nil
	case pgsyntax.QuotedIdentifier:
		value, next, ok := pgsyntax.Value(r.text, r.tokens, r.at, pgsyntax.Options{})
		if !ok {
			return "", fmt.Errorf("its %s %s is not an identifier the server reads", what, token.Text(r.text))
		}
		r.at = next
		return truncated(value), nil
	}
	return "", fmt.Errorf("its %s %s is not a name", what, token.Text(r.text))
}

// constant reads a version: a name, or a string in any form the server takes
// as a string constant.
func (r *reader) constant() (string, error) {
	r.skip()
	if r.at >= len(r.tokens) {
		return "", errors.New("it names no version")
	}
	token := r.tokens[r.at]
	text := token.Text(r.text)
	switch token.Kind {
	case pgsyntax.Word, pgsyntax.QuotedIdentifier:
		return r.name("version")
	case pgsyntax.String, pgsyntax.DollarString:
		// N'...' is a typed literal and B'...' and X'...' bit strings: none
		// is the string constant a version takes.
		if token.Kind == pgsyntax.String && strings.IndexByte("NnBbXx", text[0]) >= 0 {
			return "", fmt.Errorf("its version %s is not a string constant", text)
		}
		value, next, ok := pgsyntax.Value(r.text, r.tokens, r.at, pgsyntax.Options{})
		if !ok {
			return "", fmt.Errorf("its version %s is not a string the server reads", text)
		}
		r.at = next
		return value, nil
	}
	return "", fmt.Errorf("its version %s is neither a name nor a string", text)
}

// skipName moves past the name ALTER EXTENSION names, decoded or not, so the
// statement is recognized whatever its name is written as.
func (r *reader) skipName() {
	r.skip()
	if r.at >= len(r.tokens) {
		return
	}
	if r.tokens[r.at].Kind == pgsyntax.QuotedIdentifier {
		if _, next, ok := pgsyntax.Value(r.text, r.tokens, r.at, pgsyntax.Options{}); ok {
			r.at = next
			return
		}
		r.at++
		if r.words("UESCAPE") {
			r.skip()
			r.at++
		}
		return
	}
	r.at++
}

// folded is a bare word as the server folds it: ASCII letters to lower case,
// and every other byte as written.
func folded(word string) string {
	return strings.Map(func(c rune) rune {
		if 'A' <= c && c <= 'Z' {
			return c + ('a' - 'A')
		}
		return c
	}, word)
}

// truncated cuts a name to the bytes the server stores, at a character
// boundary.
func truncated(name string) string {
	if len(name) <= nameLength {
		return name
	}
	cut := nameLength
	for cut > 0 && !utf8.RuneStart(name[cut]) {
		cut--
	}
	return name[:cut]
}
