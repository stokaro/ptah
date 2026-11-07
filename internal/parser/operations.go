package parser

import (
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/lexer"
	"ptah.run/internal/statementform"
)

// unreadStatements are the statements every engine's documents may carry that
// change no schema object, and that this parser steps over to their semicolon. Being
// stepped over says nothing about what a statement holds.
var unreadStatements = []string{
	"ANALYZE", "BEGIN", "CALL", "COMMIT", "DELETE", "INSERT", "MERGE", "PRAGMA", "REINDEX", "ROLLBACK", "SELECT",
	"SET", "SHOW", "UPDATE", "VACUUM", "WITH",
}

// statementOpenings are the first words of what any engine reads as an operation or
// steps over. A document of no dialect steps over each, since nothing says which
// form it is written in.
var statementOpenings = descriptortable.NewIndex(func(dialects []descriptor.Dialect) []string {
	var openings []string
	for _, described := range dialects {
		statements := described.Grammar.Forms
		openings = append(openings, statements.UnreadStatements...)
		for _, form := range statements.Operations {
			openings = append(openings, statementform.Opening(form))
		}
	}
	slices.Sort(openings)
	return slices.Compact(openings)
})

// SkipsUnread reports whether a read in dialect steps over a statement opening with
// keyword without reading it, so that the statement parsing says nothing about its
// text. Keyword is in upper case. A caller that reports on the text asks this, rather
// than list the statements itself, so the two cannot disagree.
func SkipsUnread(keyword, dialect string) bool {
	if slices.Contains(unreadStatements, keyword) {
		return true
	}
	target := grammarOf(dialect)
	if !target.known {
		return slices.Contains(statementOpenings.Get(), keyword)
	}
	return slices.Contains(target.grammar.Forms.UnreadStatements, keyword)
}

// operationForm is the form of the document's dialect that opens with keyword, with
// the terms after the keyword.
func (p *Parser) operationForm(keyword string) ([]statementform.Term, bool) {
	for _, form := range p.target.grammar.Forms.Operations {
		terms, err := statementform.Parse(form)
		if err == nil && terms[0].Word == keyword {
			return terms[1:], true
		}
	}
	return nil, false
}

// parseOperation reads a statement that changes no schema object, to its end, and
// contributes no node. It is read rather than stepped over, so text the form does not
// hold is refused.
func (p *Parser) parseOperation(keyword string, terms []statementform.Term) error {
	p.advance()
	p.skipWhitespace()
	if err := p.readTerms(keyword, terms); err != nil {
		return err
	}
	return p.endStatement(keyword)
}

func (p *Parser) readTerms(statement string, terms []statementform.Term) error {
	for _, term := range terms {
		if err := p.readTerm(statement, term); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) readTerm(statement string, term statementform.Term) error {
	switch term.Kind {
	case statementform.Word:
		if err := p.expect(lexer.TokenIdentifier, term.Word); err != nil {
			return fmt.Errorf("unsupported %s clause: %w", statement, err)
		}
		p.skipWhitespace()
	case statementform.Name:
		if _, err := p.parseTableName("name"); err != nil {
			return err
		}
		p.skipWhitespace()
	case statementform.List:
		for {
			if err := p.readTerms(statement, term.Choices[0]); err != nil {
				return err
			}
			if !p.current.MatchOperatorValue(",") {
				return nil
			}
			p.advance()
			p.skipWhitespace()
		}
	case statementform.Optional, statementform.Choice:
		for _, choice := range term.Choices {
			if p.current.MatchIdentifierValue(choice[0].Word) {
				return p.readTerms(statement, choice)
			}
		}
		if term.Kind == statementform.Choice {
			return ptaherr.At(p.current.Start, fmt.Errorf("unsupported %s clause: %s", statement, p.current.Value))
		}
	}
	return nil
}

// parseRenameTable reads RENAME TABLE a TO b [, c TO d]: a rename as ALTER TABLE
// writes it, one per pair, in the order the server applies them.
func (p *Parser) parseRenameTable() (ast.Node, error) {
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "TABLE"); err != nil {
		return nil, fmt.Errorf("unsupported RENAME clause: %w", err)
	}
	p.skipWhitespace()
	list := &ast.StatementList{}
	for {
		from, err := p.parseTableName("table name")
		if err != nil {
			return nil, err
		}
		p.skipWhitespace()
		if err := p.expect(lexer.TokenIdentifier, "TO"); err != nil {
			return nil, fmt.Errorf("expected TO after RENAME TABLE %s: %w", from, err)
		}
		p.skipWhitespace()
		to, err := p.parseTableName("table name")
		if err != nil {
			return nil, err
		}
		p.skipWhitespace()
		list.Statements = append(list.Statements, &ast.AlterTableNode{
			Name:       from,
			Operations: []ast.AlterOperation{&ast.RenameTableOperation{NewName: to}},
		})
		if !p.current.MatchOperatorValue(",") {
			break
		}
		p.advance()
		p.skipWhitespace()
	}
	return list, p.endStatement("RENAME TABLE")
}
