package parser

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// A CONSTRAINT keyword written without a symbol.
//
// The MySQL family makes the symbol optional: its grammar is
// `[CONSTRAINT [symbol]] FOREIGN KEY ...`, and the same for PRIMARY KEY,
// UNIQUE and CHECK. PostgreSQL requires the name, as the SQL standard does.
// Measured on MySQL 8.4.11, MariaDB 11.8.9 and PostgreSQL 18.6, each clause
// written after a bare CONSTRAINT and compared with the same clause written
// without the keyword:
//
//	clause                       MySQL 8.4.11         MariaDB 11.8.9       PostgreSQL 18.6
//	CONSTRAINT FOREIGN KEY       same as FOREIGN KEY  same as FOREIGN KEY  syntax error
//	CONSTRAINT PRIMARY KEY       same as PRIMARY KEY  same as PRIMARY KEY  syntax error
//	CONSTRAINT UNIQUE            same as UNIQUE       same as UNIQUE       syntax error
//	CONSTRAINT CHECK             same as CHECK        same as CHECK        syntax error
//	CONSTRAINT KEY|INDEX|...     ERROR 1064           ERROR 1064           syntax error
//	column CONSTRAINT CHECK      same as CHECK        ERROR 1064           syntax error
//	column CONSTRAINT REFERENCES ERROR 1064           same as REFERENCES   syntax error
//	column CONSTRAINT UNIQUE,    ERROR 1064           ERROR 1064           syntax error
//	  PRIMARY KEY, NOT NULL,
//	  DEFAULT
//
// "Same as" is the catalog, names included: a foreign key takes the next
// `<table>_ibfk_<n>` in the one sequence the table's unnamed keys share, a
// CHECK the next `<table>_chk_<n>` on MySQL and `CONSTRAINT_<n>` on MariaDB,
// and a UNIQUE the name of its first column or of the index it names. So the
// reader drops the keyword and reads the clause as the one written without it,
// which gives it the name that clause gets and keeps it in that numbering; a
// second path for the spelling would be a second answer to the same question.
//
// Without this reading, `CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id)` takes
// FOREIGN for the symbol and KEY for an index, and the refusal that follows
// blames a misspelled keyword the statement does not have (stokaro/ptah#3730).
// `CONSTRAINT UNIQUE KEY uq (a)` parses without an error, as a plain index
// where the author wrote a unique one.

// constraintKindWords open the constraint that follows a CONSTRAINT symbol, at
// table level or on a column.
//
// They are what tells a symbol from a kind written in its place. A symbol is
// followed by one of them; a kind written without a symbol is followed by its
// own tail -- KEY, a parenthesis, a table name -- in every spelling any of the
// three engines accepts. So a word followed by one of these is the symbol, and
// `CONSTRAINT primary PRIMARY KEY` names its key `primary`, as an engine that
// does not reserve the word reads it.
var constraintKindWords = []string{"PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "EXCLUDE", "REFERENCES", "NOT", "DEFAULT"}

// indexKindWords open an index, which takes no CONSTRAINT keyword on any engine
// measured: MySQL and MariaDB answer ERROR 1064, with a symbol or without one,
// and PostgreSQL a syntax error.
var indexKindWords = []string{"KEY", "INDEX", "FULLTEXT", "SPATIAL"}

// tableElementKindWords are the words a table-level element may open with after
// CONSTRAINT: the constraint kinds, and the index keywords the MySQL family
// accepts in the same position without the CONSTRAINT keyword.
var tableElementKindWords = slices.Concat([]string{"PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "EXCLUDE"}, indexKindWords)

// mysqlSymbolLessTableKinds are the table-level kinds the MySQL family accepts
// after a CONSTRAINT without a symbol.
var mysqlSymbolLessTableKinds = []string{"PRIMARY", "UNIQUE", "FOREIGN", "CHECK"}

// columnConstraintKindWords are the words a column constraint may open with
// after CONSTRAINT.
var columnConstraintKindWords = []string{"CHECK", "REFERENCES", "UNIQUE", "PRIMARY", "NOT", "NULL", "DEFAULT"}

// mysqlSymbolLessColumnKinds is the one column constraint each engine of the
// MySQL family accepts after a CONSTRAINT without a symbol. The two engines
// disagree, and each refuses the other's.
var mysqlSymbolLessColumnKinds = map[string]string{
	platform.MySQL:   "CHECK",
	platform.MariaDB: "REFERENCES",
}

// constraintSymbolOmitted reports whether the cursor, just past CONSTRAINT, sits
// on a kind from kinds rather than on a symbol.
//
// A quoted name is always a symbol: CONSTRAINT `FOREIGN` FOREIGN KEY names the
// key FOREIGN on both MySQL-family engines. An unquoted word is the kind unless
// another kind follows it; see constraintKindWords.
func (p *Parser) constraintSymbolOmitted(kinds []string) bool {
	if p.current.Type != lexer.TokenIdentifier || !containsFold(kinds, p.current.Value) {
		return false
	}
	next := p.peekSignificantToken()
	return next.Type != lexer.TokenIdentifier || !containsFold(constraintKindWords, next.Value)
}

// peekSignificantToken answers the token after the current one, stepping over
// whitespace and comments, and leaves the parser where it is.
func (p *Parser) peekSignificantToken() lexer.Token {
	scan := *p.lexer
	for {
		token := scan.NextToken()
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenUnknown:
		default:
			return token
		}
	}
}

// containsFold reports whether words holds word, compared without case.
func containsFold(words []string, word string) bool {
	return slices.ContainsFunc(words, func(candidate string) bool {
		return strings.EqualFold(candidate, word)
	})
}

// admitSymbolLessTableConstraint accepts a table-level CONSTRAINT written
// without a symbol where the dialect's grammar does, and refuses it by name
// where it does not. The cursor stays on the kind, so the element is then read
// exactly as it would be without the keyword.
func (p *Parser) admitSymbolLessTableConstraint(start int) error {
	if err := p.refuseConstraintBeforeIndex(start); err != nil {
		return err
	}
	kind := strings.ToUpper(p.current.Value)
	if !p.isMySQLFamilyDialect() {
		return p.constraintSymbolRequired(start, kind)
	}
	if !slices.Contains(mysqlSymbolLessTableKinds, kind) {
		return fmt.Errorf(
			"CONSTRAINT at position %d is followed by %s, not by a name: %s accepts CONSTRAINT "+
				"without a name only before PRIMARY KEY, UNIQUE, FOREIGN KEY and CHECK",
			start, kind, p.dialect)
	}
	return nil
}

// refuseConstraintBeforeIndex refuses a CONSTRAINT, with a symbol or without
// one, in front of an index keyword. The cursor sits on the word after the
// symbol, or after CONSTRAINT when there is none.
//
// Without the refusal, `CONSTRAINT k KEY (a)` reads as an index `k`, which no
// engine measured creates.
func (p *Parser) refuseConstraintBeforeIndex(start int) error {
	if p.current.Type != lexer.TokenIdentifier || !containsFold(indexKindWords, p.current.Value) {
		return nil
	}
	return fmt.Errorf(
		"CONSTRAINT at position %d stands before %s, which declares an index: an index takes "+
			"no CONSTRAINT keyword, and MySQL and MariaDB answer ERROR 1064 (42000) to one; "+
			"drop the CONSTRAINT keyword",
		start, strings.ToUpper(p.current.Value))
}

// handleSymbolLessColumnConstraint reads a column constraint written after a
// CONSTRAINT without a symbol, as the same constraint written without the
// keyword, on the engine that accepts it, and refuses it by name everywhere
// else.
func (p *Parser) handleSymbolLessColumnConstraint(column *ast.ColumnNode, start int) error {
	kind := strings.ToUpper(p.current.Value)
	accepted, family := mysqlSymbolLessColumnKinds[p.dialect]
	if !family {
		return p.constraintSymbolRequired(start, kind)
	}
	if kind != accepted {
		return fmt.Errorf(
			"CONSTRAINT at position %d is followed by %s, not by a name: on a column, %s accepts "+
				"CONSTRAINT without a name only before %s, and answers ERROR 1064 (42000) to this; "+
				"drop the CONSTRAINT keyword",
			start, kind, p.dialect, accepted)
	}
	if kind == "CHECK" {
		return p.handleCheck(column)
	}
	return p.handleReferences(column)
}

// constraintSymbolRequired refuses a CONSTRAINT without a symbol on a dialect
// whose grammar requires one. A document with no dialect is refused too: it is
// read to render anywhere, and the spelling renders on the MySQL family alone.
func (p *Parser) constraintSymbolRequired(start int, kind string) error {
	return fmt.Errorf(
		"CONSTRAINT at position %d is followed by %s, not by a name: %s requires a name after "+
			"CONSTRAINT, and only MySQL and MariaDB accept the keyword without one; name the "+
			"constraint, or drop the CONSTRAINT keyword",
		start, kind, describeFunctionalKeyDialect(p.dialect))
}
