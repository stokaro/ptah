package parser

import (
	"context"
	"fmt"

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
		if depth == 0 && !first && (p.endsOwnerSpan(site) || p.isOwnerStop(pattern.Stops)) {
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
	case sqlext.TableElement, sqlext.AlterOperation:
		return p.current.MatchOperatorValue(",")
	case sqlext.ViewClause:
		return p.current.MatchIdentifierValue("AS")
	default:
		return false
	}
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
	values, err := p.owners.Parse(ctx, p.dialect, matches)
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
	return statements, nil
}
