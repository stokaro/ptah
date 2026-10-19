package parser

import (
	"ptah.run/core/ast"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/routineext"
	routinebuiltin "ptah.run/internal/routineext/builtin"
)

func (p *Parser) routineServices() routineext.Services {
	return routinebuiltin.Lookup(p.target.grammar.Routines.Body)
}

// routineCursor lends a grammar owner the shared token stream. Claims remain
// with the document so all owners run through the same validation and facets.
type routineCursor struct{ parser *Parser }

func (c routineCursor) Input() string   { return c.parser.input }
func (c routineCursor) Dialect() string { return c.parser.dialect }
func (c routineCursor) FamilyOptions() lexer.Options {
	return dialectlexer.Options(c.parser.target.family)
}
func (c routineCursor) Current() *lexer.Token      { return &c.parser.current }
func (c routineCursor) Previous() *lexer.Token     { return &c.parser.previous }
func (c routineCursor) AtEnd() bool                { return c.parser.isAtEnd() }
func (c routineCursor) Advance()                   { c.parser.advance() }
func (c routineCursor) CheckTimeout() error        { return c.parser.checkTimeout() }
func (c routineCursor) Statement(start int) string { return c.parser.rawStatement(start) }
func (c routineCursor) Fragment(start, end int) string {
	return c.parser.rawStatementFragment(start, end)
}
func (c routineCursor) Function(start int) (ast.Node, error) {
	return c.parser.parseCreateFunction(start)
}
func (c routineCursor) Fallback(target string, start int) (ast.Node, error) {
	return c.parser.parseRoutineBestEffort(target, start)
}
func (c routineCursor) DefinerFallback(start int) (ast.Node, error) {
	return c.parser.parseCreateDefinerRoutineBestEffort(start)
}
func (c routineCursor) BatchSeparator() bool { return c.parser.isCurrentSQLServerGoBatchSeparator() }
func (c routineCursor) ClaimOptions(node *ast.RoutineNode, spans []routineext.OptionSpan, offset int) {
	c.parser.claimOwnerRoutineOptions(node, spans, offset)
}

func (p *Parser) parseRoutineBestEffort(target string, start int) (ast.Node, error) {
	switch target {
	case "PROC", "PROCEDURE":
		return routinebuiltin.Raw(routineCursor{p}, start, target)
	default:
		return p.parseCreateFunction(start)
	}
}
