package parser

import (
	"context"
	"fmt"
	"slices"
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
		if len(candidate.Pattern.AfterName) > 0 {
			before, after, named := p.namedWords(len(candidate.Pattern.Words), len(candidate.Pattern.AfterName)+1)
			if named && candidate.MatchesNamed(before, after) {
				return candidate, true
			}
			continue
		}
		if candidate.Matches(words) {
			return candidate, true
		}
	}
	return sqlext.Candidate{}, false
}

// tokenScan reads the significant tokens ahead of the cursor without moving
// the parser.
type tokenScan struct {
	token lexer.Token
	lexer lexer.Lexer
}

func (p *Parser) scanAhead() *tokenScan {
	return &tokenScan{token: p.current, lexer: *p.lexer}
}

// next returns the next significant token, or the end of the document.
func (s *tokenScan) next() lexer.Token {
	for {
		token := s.token
		if token.Type == lexer.TokenEOF {
			return token
		}
		s.token = s.lexer.NextToken()
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenUnknown:
		default:
			return token
		}
	}
}

// identifier reports whether first opens an identifier as
// [Parser.expectIdentifier] reads one, a word, a double-quoted name or a
// bracketed one, and steps over the rest of a bracketed one.
func (s *tokenScan) identifier(first lexer.Token) bool {
	switch {
	case first.Type == lexer.TokenIdentifier || isDoubleQuotedIdentifierToken(first):
		return true
	case first.MatchOperatorValue("["):
		for {
			token := s.next()
			switch {
			case token.Type == lexer.TokenEOF:
				return false
			case !token.MatchOperatorValue("]"):
				continue
			}
			probe := *s
			if closing := probe.next(); !closing.MatchOperatorValue("]") {
				return true
			}
			*s = probe
		}
	default:
		return false
	}
}

// upcomingWords answers the values of the next count significant tokens from
// the cursor, the current one included, and leaves the parser where it is.
func (p *Parser) upcomingWords(count int) []string {
	words := make([]string, 0, count)
	scan := p.scanAhead()
	for len(words) < count {
		token := scan.next()
		if token.Type == lexer.TokenEOF {
			break
		}
		words = append(words, token.Value)
	}
	return words
}

// namedWords answers the opening significant tokens from the cursor, as many
// as opening, and the following ones after the qualified name that stands
// next, as many as following, and leaves the parser where it is. The name is
// read as [Parser.parseQualifiedIdentifier] reads one: an identifier, then a
// dot and an identifier as often as the document writes them. It reports
// false where the opening tokens are not there or no identifier follows
// them.
func (p *Parser) namedWords(opening, following int) (before, after []string, named bool) {
	scan := p.scanAhead()
	before = make([]string, 0, opening)
	for range opening {
		token := scan.next()
		if token.Type == lexer.TokenEOF {
			return nil, nil, false
		}
		before = append(before, token.Value)
	}
	if !scan.identifier(scan.next()) {
		return nil, nil, false
	}
	for {
		probe := *scan
		if dot := probe.next(); dot.Type != lexer.TokenOperator || dot.Value != "." {
			break
		}
		if !probe.identifier(probe.next()) {
			break
		}
		*scan = probe
	}
	after = make([]string, 0, following)
	for len(after) < following {
		token := scan.next()
		if token.Type == lexer.TokenEOF {
			break
		}
		after = append(after, token.Value)
	}
	return before, after, true
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
	opening := len(pattern.Words)
	for p.current.Type != lexer.TokenEOF {
		if depth == 0 && opening == 0 && (p.endsOwnerSpan(site) || p.isOwnerStop(pattern.Stops) || p.opensNextOption(site)) {
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
		if p.current.Type != lexer.TokenWhitespace && p.current.Type != lexer.TokenComment && opening > 0 {
			opening--
		}
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
	case sqlext.TableElement, sqlext.AlterOperation, sqlext.TableOption, sqlext.ColumnOption, sqlext.IndexOption:
		return p.current.MatchOperatorValue(",")
	case sqlext.TablePrefix:
		return p.current.MatchIdentifierValue("TABLE")
	case sqlext.ViewClause:
		return p.current.MatchIdentifierValue("AS")
	default:
		return false
	}
}

// opensNextOption reports whether, at a column or an index option, the
// current token opens the next one: a column constraint or attribute, or a
// key option, the shared grammar reads, or the opening word of any owner's
// pattern at the site the document consults. Such options follow one another
// without a comma, so these are where an owner's option ends, and no owner
// repeats the grammar's words or another owner's. Among a key's options a
// version guard opens the next options too, since the reader reads the
// guard's body as options; see [Parser.readGuardedKeyOptions].
func (p *Parser) opensNextOption(site sqlext.Site) bool {
	if site == sqlext.TablePrefix {
		_, found := p.ownerCandidate(site)
		return found
	}
	if site == sqlext.IndexOption && p.current.Type == lexer.TokenUnknown {
		return true
	}
	if p.current.Type != lexer.TokenIdentifier {
		return false
	}
	switch site {
	case sqlext.ColumnOption:
		if columnClauseStart(p.current.Value) {
			return true
		}
	case sqlext.RoutineOption:
		if isFunctionClauseKeyword(p.current.Value) {
			return true
		}
	case sqlext.IndexOption:
		if _, known := p.keyOptionReader(); known {
			return true
		}
	default:
		return false
	}
	for _, candidate := range p.owners.Candidates(p.dialect, site) {
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
	values, err := p.owners.Parse(ctx, p.dialect, matches, sqlext.Document{Declared: p.declared, DroppedTables: p.droppedTables})
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
// list. It retains the element's order and its index or child declarations.
func (p *Parser) claimOwnerTableElement(table *ast.CreateTableNode) bool {
	index := &ast.IndexNode{Table: table.Name}
	position := len(table.Elements)
	claimed := p.claimOwnerSyntax(sqlext.TableElement, table.Name, func(value sqlext.Value) error {
		if value.Index == nil {
			objects, err := schemaext.NewObjects(value.Objects...)
			if err != nil {
				return err
			}
			combined, err := table.OwnedObjects.Merge(objects)
			if err != nil {
				return err
			}
			table.OwnedObjects = combined
			table.Indexes = slices.DeleteFunc(table.Indexes, func(candidate *ast.IndexNode) bool { return candidate == index })
			table.Elements[position] = ast.TableElement{Objects: objects.Refs()}
			return nil
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
// where the operation stood, as the owner's operation. Its index or child
// declarations are captured as notes of the parse.
func (p *Parser) claimOwnerAlterOperation(table string) (ast.AlterOperation, bool) {
	operation := &ast.ExtensionAlterOperation{}
	claimed := p.claimOwnerSyntax(sqlext.AlterOperation, table, func(value sqlext.Value) error {
		if value.Operation == nil {
			return fmt.Errorf("%w: an owner's ALTER TABLE %s operation carries no payload", schemaext.ErrInvalidValue, table)
		}
		operation.Payload = value.Operation
		if len(value.Objects) > 0 {
			objects, err := schemaext.NewObjects(value.Objects...)
			if err != nil {
				return err
			}
			if p.declaredObjects == nil {
				p.declaredObjects = make(map[*ast.ExtensionAlterOperation]schemaext.Objects)
			}
			p.declaredObjects[operation] = objects
		}
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
		if value.Declined {
			if p.declinedStatements == nil {
				p.declinedStatements = make(map[*ast.ExtensionStatement]OwnerStatement)
			}
			p.declinedStatements[statement] = *note
			delete(p.ownerStatements, statement)
			return nil
		}
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
		note.Objects, note.Replaces, note.TableFacets = value.Objects, value.Replaces, value.TableFacets
		note.TableReferences, note.UnmodeledReason = value.TableReferences, value.UnmodeledReason
		note.DescribedBy = value.DescribedBy
		return nil
	})
	if !claimed {
		return statement, false
	}
	note.Text = p.ownerClaims[len(p.ownerClaims)-1].match.Claim.Text
	note.offset = p.ownerClaims[len(p.ownerClaims)-1].match.Claim.Offset
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
		p.ownerClaims[len(p.ownerClaims)-1].match.Claim.Definition = p.definitions
	}
	return claimed
}

// claimOwnerIndexOption claims an owner's option of the key being read, whose
// name and FULLTEXT or SPATIAL prefix the claim carries, with the key's
// number. The facets it reads are the key's: they reach the node the element
// builds once it is built; see [keyFacets].
func (p *Parser) claimOwnerIndexOption(name, prefix string) bool {
	slot := p.keyOptions.facets
	if slot == nil {
		slot = &keyFacets{}
	}
	if !p.claimOwnerSyntax(sqlext.IndexOption, name, slot.merge) {
		return false
	}
	p.keyOptions.facets = slot
	claim := &p.ownerClaims[len(p.ownerClaims)-1].match.Claim
	claim.Definition, claim.IndexType = p.definitions, prefix
	return true
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
	// DescribedBy identifies the later claim carrying this statement's model.
	DescribedBy int
	// TableReferences must resolve when the schema applies this statement.
	TableReferences []string
	// UnmodeledReason is the owner's refusal to describe this statement.
	UnmodeledReason string
	// Objects are the objects the statement declares; see
	// [sqlext.Value.Objects].
	Objects []schemaext.Object
	// Replaces are the objects the statement amends; see
	// [sqlext.Value.Replaces].
	Replaces []schemaext.Object
	// TableFacets are the owner's facets of a declared table the statement
	// sets; see [sqlext.Value.TableFacets].
	TableFacets *sqlext.TableFacets
	// Text is the statement as the document writes it, without the
	// semicolon that ends it, so a refusal can name it.
	Text string
	// offset is where the statement opens in the document.
	offset int
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
	if err := p.placeDeclinedStatements(statements); err != nil {
		return nil, err
	}
	return statements, nil
}

// placeDeclinedStatements reads each statement an owner declined by itself
// with the shared grammar, as if no pattern had matched it, and puts what it
// reads where the statement stood. The shared grammar reads a statement
// without the state the statements before it left, so reading one alone reads
// it as the whole document would have.
func (p *Parser) placeDeclinedStatements(statements *ast.StatementList) error {
	if len(p.declinedStatements) == 0 {
		return nil
	}
	placed := make([]ast.Node, 0, len(statements.Statements))
	for _, node := range statements.Statements {
		statement, ok := node.(*ast.ExtensionStatement)
		claim, declined := p.declinedStatements[statement]
		if !ok || !declined {
			placed = append(placed, node)
			continue
		}
		// The span ends at the last token before the semicolon, so its text
		// is one statement without one.
		read, err := NewParser(claim.Text, WithDialect(p.dialect), WithCapabilities(p.capabilities), WithTimeout(p.timeout),
			WithSQL(sqlext.None())).parseStatements()
		if err != nil {
			return fmt.Errorf("statement at position %d: %w", claim.offset, err)
		}
		placed = append(placed, read.Statements...)
	}
	statements.Statements = placed
	return nil
}

// parseOwnedTablePrefix claims modifiers before the common table grammar. The
// owner receives the complete table identity after the frontend reads its name.
func (p *Parser) parseOwnedTablePrefix() (*ast.CreateTableNode, bool, error) {
	var table *ast.CreateTableNode
	start := len(p.ownerClaims)
	for p.claimOwnerSyntax(sqlext.TablePrefix, "", func(value sqlext.Value) error {
		merged, err := table.Facets.Merge(value.Facets)
		if err != nil {
			return fmt.Errorf("table %s: %w", table.Name, err)
		}
		table.Facets = merged
		return nil
	}) {
		p.skipWhitespace()
	}
	end := len(p.ownerClaims)
	if start == end {
		return nil, false, nil
	}
	if !p.current.MatchIdentifierValue("TABLE") {
		return nil, true, fmt.Errorf("expected TABLE after a table prefix, found %s at position %d", p.current.Value, p.current.Start)
	}
	var err error
	table, err = p.parseCreateTable()
	if err != nil {
		return nil, true, err
	}
	for i := start; i < end; i++ {
		p.ownerClaims[i].match.Claim.Subject = table.Name
	}
	return table, true, nil
}

// DeclaredObjects returns the immutable child declarations captured for an
// owned ALTER TABLE operation. An unrecorded operation returns an empty set.
func (p *Parser) DeclaredObjects(operation *ast.ExtensionAlterOperation) schemaext.Objects {
	return p.declaredObjects[operation]
}
