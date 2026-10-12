package parser

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/internal/lexer"
)

// WithSQL selects the owners whose syntax the parse reads beside the shared
// grammar; see [ptah.run/core/sqlext]. Without it the parse reads the shared
// grammar alone.
func WithSQL(owners sqlext.Set) Option {
	return func(p *Parser) {
		p.owners = owners
	}
}

// WithDeclared hands the owners the objects the documents read before this one
// declared, such as the earlier files of a schema directory, which an owner's
// statement may amend. Without it a document is read alone.
func WithDeclared(objects schemaext.Objects) Option {
	return func(p *Parser) {
		p.declared = objects
	}
}

// ownerClaim is a span an owner's pattern matched, waiting for the owner's
// value, and where the value goes.
type ownerClaim struct {
	match sqlext.Match
	apply func(sqlext.Value) error
}

// ownerCandidate returns the first pattern the document's owners register at
// site whose words open the syntax at the cursor.
func (p *Parser) ownerCandidate(site sqlext.Site) (sqlext.Candidate, bool) {
	candidates := p.owners.Candidates(p.dialect, site)
	if len(candidates) == 0 {
		return sqlext.Candidate{}, false
	}
	longest := 0
	for _, candidate := range candidates {
		longest = max(longest, len(candidate.Pattern.Words)+1)
	}
	words := p.upcomingWords(longest)
	for _, candidate := range candidates {
		if candidate.Matches(words) {
			return candidate, true
		}
	}
	return sqlext.Candidate{}, false
}

// upcomingWords answers the values of the next count significant tokens from
// the cursor, the current one included, and leaves the parser where it is.
func (p *Parser) upcomingWords(count int) []string {
	words := make([]string, 0, count)
	token := p.current
	scan := *p.lexer
	for len(words) < count && token.Type != lexer.TokenEOF {
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenUnknown:
		default:
			words = append(words, token.Value)
		}
		token = scan.NextToken()
	}
	return words
}

// claimOwnerSyntax reads the span of an owner's syntax at the cursor, when an
// owner's pattern at site matches, and records it for the owner to read once
// the document is parsed; apply receives the owner's value then. It reports
// whether it claimed a span.
func (p *Parser) claimOwnerSyntax(site sqlext.Site, subject string, apply func(sqlext.Value) error) bool {
	candidate, found := p.ownerCandidate(site)
	if !found {
		return false
	}
	text, start := p.ownerSpan(site, candidate.Pattern)
	p.ownerClaims = append(p.ownerClaims, ownerClaim{
		match: sqlext.Match{Candidate: candidate, Claim: sqlext.Claim{
			Pattern: candidate.Pattern.Name, Site: site, Subject: subject, Text: text, Offset: start,
		}},
		apply: apply,
	})
	return true
}

// ownerSpan consumes the span a pattern claimed and returns its source text
// and offset. A span ends at a top-level stop of the pattern, at the end the
// site gives every span, or at the end of the document. The pattern's own
// opening word is never a stop.
func (p *Parser) ownerSpan(site sqlext.Site, pattern sqlext.Pattern) (string, int) {
	p.skipWhitespace()
	start := p.current.Start
	end := start
	depth := 0
	first := true
	for p.current.Type != lexer.TokenEOF {
		if depth == 0 && !first && (p.endsOwnerSpan(site) || p.isOwnerStop(pattern.Stops) || p.opensColumnClause(site)) {
			break
		}
		if p.current.MatchOperatorValue("(") {
			depth++
		}
		if p.current.MatchOperatorValue(")") {
			depth--
			// A closing parenthesis below the span's level belongs to what
			// contains the span, such as the column list.
			if depth < 0 {
				break
			}
		}
		first = false
		end = p.current.End
		p.advance()
	}
	return p.input[start:end], start
}

// endsOwnerSpan reports whether the current top-level token ends every span
// that opens at site.
func (p *Parser) endsOwnerSpan(site sqlext.Site) bool {
	if p.current.Type == lexer.TokenSemicolon {
		return true
	}
	switch site {
	case sqlext.TableElement, sqlext.AlterOperation, sqlext.TableOption, sqlext.ColumnOption:
		return p.current.MatchOperatorValue(",")
	case sqlext.ViewClause:
		return p.current.MatchIdentifierValue("AS")
	default:
		return false
	}
}

// opensColumnClause reports whether, at a column option, the current token
// opens the next clause of the column: a constraint or attribute the shared
// grammar reads, or the opening word of any owner's column option the
// document consults. Column clauses follow one another without a comma, so
// these are where an owner's option ends, and no owner repeats the
// grammar's words or another owner's.
func (p *Parser) opensColumnClause(site sqlext.Site) bool {
	if site != sqlext.ColumnOption || p.current.Type != lexer.TokenIdentifier {
		return false
	}
	if columnClauseStart(p.current.Value) {
		return true
	}
	for _, candidate := range p.owners.Candidates(p.dialect, sqlext.ColumnOption) {
		if strings.EqualFold(candidate.Pattern.Words[0], p.current.Value) {
			return true
		}
	}
	return false
}

// isOwnerStop reports whether the current token is one of a pattern's stops.
func (p *Parser) isOwnerStop(stops []string) bool {
	switch p.current.Type {
	case lexer.TokenIdentifier, lexer.TokenOperator:
		return containsFold(stops, p.current.Value)
	default:
		return false
	}
}

// readOwnerClaims hands every claim of the document to its owner, one call
// per owner, and applies the values.
func (p *Parser) readOwnerClaims(ctx context.Context) error {
	if len(p.ownerClaims) == 0 {
		return nil
	}
	matches := make([]sqlext.Match, 0, len(p.ownerClaims))
	for _, claim := range p.ownerClaims {
		matches = append(matches, claim.match)
	}
	values, err := p.owners.Parse(ctx, p.dialect, matches, p.declared)
	if err != nil {
		return err
	}
	for i, claim := range p.ownerClaims {
		if err := claim.apply(values[i]); err != nil {
			return err
		}
	}
	return nil
}

// claimOwnerTableElement claims an owner's element of a CREATE TABLE column
// list. It stands where the element stood, as the index it declares.
func (p *Parser) claimOwnerTableElement(table *ast.CreateTableNode) bool {
	index := &ast.IndexNode{Table: table.Name}
	claimed := p.claimOwnerSyntax(sqlext.TableElement, table.Name, func(value sqlext.Value) error {
		if value.Index == nil {
			return fmt.Errorf("%w: an owner's table element of %s declares no index", schemaext.ErrInvalidValue, table.Name)
		}
		index.Name = value.Index.Name
		index.Columns = []string{value.Index.Expression}
		index.Facets = value.Index.Facets
		return nil
	})
	if claimed {
		table.AddIndex(index)
	}
	return claimed
}

// claimOwnerAlterOperation claims an owner's ALTER TABLE operation. It stands
// where the operation stood, as the owner's operation, and the index it
// declares is a note of the parse.
func (p *Parser) claimOwnerAlterOperation(table string) (ast.AlterOperation, bool) {
	operation := &ast.ExtensionAlterOperation{}
	claimed := p.claimOwnerSyntax(sqlext.AlterOperation, table, func(value sqlext.Value) error {
		if value.Operation == nil {
			return fmt.Errorf("%w: an owner's ALTER TABLE %s operation carries no payload", schemaext.ErrInvalidValue, table)
		}
		operation.Payload = value.Operation
		if value.Index != nil {
			if p.declaredIndexes == nil {
				p.declaredIndexes = make(map[*ast.ExtensionAlterOperation]sqlext.Index)
			}
			p.declaredIndexes[operation] = *value.Index
		}
		return nil
	})
	return operation, claimed
}

// claimOwnerStatement claims a whole statement an owner's pattern opens. It
// stands where the statement stood, as the owner's statement, and what the
// statement declares and amends is a note of the parse. A statement that
// declares a table stands there as the CREATE TABLE instead, once the owner
// has read it; see [Parser.placeOwnerTables].
func (p *Parser) claimOwnerStatement() (ast.Node, bool) {
	statement := &ast.ExtensionStatement{}
	note := &OwnerStatement{}
	claimed := p.claimOwnerSyntax(sqlext.Statement, "", func(value sqlext.Value) error {
		if value.Table != nil {
			table := ast.NewCreateTable(value.Table.Name)
			if value.Table.IfNotExists {
				table.SetIfNotExists()
			}
			table.Facets = value.Table.Facets
			if p.ownerTables == nil {
				p.ownerTables = make(map[*ast.ExtensionStatement]*ast.CreateTableNode)
			}
			p.ownerTables[statement] = table
			return nil
		}
		statement.Payload = value.Statement
		note.Objects, note.Replaces = value.Objects, value.Replaces
		return nil
	})
	if !claimed {
		return statement, false
	}
	note.Text = p.ownerClaims[len(p.ownerClaims)-1].match.Claim.Text
	if p.ownerStatements == nil {
		p.ownerStatements = make(map[*ast.ExtensionStatement]*OwnerStatement)
	}
	p.ownerStatements[statement] = note
	return statement, true
}

// claimOwnerTableOption claims an owner's option of a CREATE TABLE after its
// column list. The facets it reads are the table's.
func (p *Parser) claimOwnerTableOption(table *ast.CreateTableNode) bool {
	return p.claimOwnerSyntax(sqlext.TableOption, table.Name, func(value sqlext.Value) error {
		merged, err := table.Facets.Merge(value.Facets)
		if err != nil {
			return fmt.Errorf("table %s: %w", table.Name, err)
		}
		table.Facets = merged
		return nil
	})
}

// claimOwnerColumnOption claims an owner's option of the column definition
// being read. The facets it reads are the column's; the claim carries the
// definition's number, so an owner can combine the options of one column.
func (p *Parser) claimOwnerColumnOption(column *ast.ColumnNode) bool {
	claimed := p.claimOwnerSyntax(sqlext.ColumnOption, column.Name, func(value sqlext.Value) error {
		merged, err := column.Facets.Merge(value.Facets)
		if err != nil {
			return fmt.Errorf("column %s: %w", column.Name, err)
		}
		column.Facets = merged
		return nil
	})
	if claimed {
		p.ownerClaims[len(p.ownerClaims)-1].match.Claim.Definition = p.columnDefinitions
	}
	return claimed
}

// claimOwnerViewClauses claims every owner's clause between a materialized
// view's name and AS. The facets they read are the view's.
func (p *Parser) claimOwnerViewClauses(view *ast.CreateMaterializedViewNode) {
	for {
		p.skipWhitespace()
		claimed := p.claimOwnerSyntax(sqlext.ViewClause, view.Name, func(value sqlext.Value) error {
			merged, err := view.Facets.Merge(value.Facets)
			if err != nil {
				return fmt.Errorf("materialized view %s: %w", view.Name, err)
			}
			view.Facets = merged
			return nil
		})
		if !claimed {
			return
		}
	}
}

// DeclaredIndex returns the index an owner's ALTER TABLE operation of the
// parsed document declares, beside the owner's payload the operation
// carries.
func (p *Parser) DeclaredIndex(operation *ast.ExtensionAlterOperation) (sqlext.Index, bool) {
	index, found := p.declaredIndexes[operation]
	return index, found
}

// OwnerStatement is what an owner's statement of a parsed document declares
// and amends, and the statement as the document writes it.
type OwnerStatement struct {
	// Objects are the objects the statement declares; see
	// [sqlext.Value.Objects].
	Objects []schemaext.Object
	// Replaces are the objects the statement amends; see
	// [sqlext.Value.Replaces].
	Replaces []schemaext.Object
	// Text is the statement as the document writes it, without the
	// semicolon that ends it, so a refusal can name it.
	Text string
}

// OwnerStatement returns what an owner's statement of the parsed document
// declares and amends.
func (p *Parser) OwnerStatement(statement *ast.ExtensionStatement) (OwnerStatement, bool) {
	note, found := p.ownerStatements[statement]
	if !found {
		return OwnerStatement{}, false
	}
	return *note, true
}

// placeOwnerTables puts each table an owner's statement declares where the
// statement stood. An owner's statement is always one of the document's
// top-level statements, so the list holds every one.
func (p *Parser) placeOwnerTables(statements *ast.StatementList) {
	if len(p.ownerTables) == 0 {
		return
	}
	for i, node := range statements.Statements {
		statement, ok := node.(*ast.ExtensionStatement)
		if !ok {
			continue
		}
		if table, declared := p.ownerTables[statement]; declared {
			statements.Statements[i] = table
		}
	}
}

// ParseContext is [Parser.Parse] with the context the owners' parsers run
// under.
func (p *Parser) ParseContext(ctx context.Context) (*ast.StatementList, error) {
	statements, err := p.parseStatements()
	if err != nil {
		return nil, err
	}
	if err := p.readOwnerClaims(ctx); err != nil {
		return nil, err
	}
	p.placeOwnerTables(statements)
	return statements, nil
}
