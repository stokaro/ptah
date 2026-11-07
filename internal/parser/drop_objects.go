package parser

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/lexer"
)

// dropForm is how the document's dialect reads DROP of kind, and whether it reads
// one. A document of no dialect takes any engine's spelling.
func (p *Parser) dropForm(kind string) (descriptor.Drop, bool) {
	if !p.target.known {
		return descriptor.Drop{
			Kind: kind, IfExists: true, List: kind != descriptor.DropKindTrigger, Behavior: true,
			OnTable: kind == descriptor.DropKindTrigger, Arguments: true, Sync: true,
		}, true
	}
	index := slices.IndexFunc(p.target.grammar.Forms.Drops, func(drop descriptor.Drop) bool { return drop.Kind == kind })
	if index < 0 {
		return descriptor.Drop{}, false
	}
	return p.target.grammar.Forms.Drops[index], true
}

// parseDropObject reads a DROP of a kind other than a table or an index, with the
// cursor on the word that names the kind. It returns one node per object, as the
// nodes model one object each, in a list when the statement names several.
func (p *Parser) parseDropObject(target string) (ast.Node, error) {
	start := p.current.Start
	kind := target
	if target == "MATERIALIZED" {
		if !p.nextIsWord("VIEW") {
			return nil, ptaherr.At(start, fmt.Errorf("expected VIEW after DROP MATERIALIZED"))
		}
		p.advance()
		p.skipWhitespace()
		kind = descriptor.DropKindMaterializedView
	}
	form, reads := p.dropForm(kind)
	if !reads || !slices.Contains(descriptor.DropKinds(), kind) {
		return nil, p.refuseHead(start, "DROP", target, "unsupported DROP target: %s")
	}
	p.advance()
	p.skipWhitespace()

	ifExists, err := p.parseDropIfExists(form)
	if err != nil {
		return nil, err
	}
	targets, err := p.parseDropTargets(form)
	if err != nil {
		return nil, err
	}
	cascade, err := p.parseDropBehavior(form)
	if err != nil {
		return nil, err
	}
	p.parseDropSync(form)
	if err := p.endStatement("DROP " + kind); err != nil {
		return nil, err
	}
	return dropNodes(kind, targets, ifExists, cascade), nil
}

// parseDropIfExists reads IF EXISTS where the form takes it. Where it does not, the
// words are read as the clause the engine has no reading of.
func (p *Parser) parseDropIfExists(form descriptor.Drop) (bool, error) {
	if !p.current.MatchIdentifierValue("IF") {
		return false, nil
	}
	if !form.IfExists {
		return false, ptaherr.At(p.current.Start, fmt.Errorf("unsupported DROP %s clause: IF EXISTS", form.Kind))
	}
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "EXISTS"); err != nil {
		return false, fmt.Errorf("expected EXISTS after DROP %s IF: %w", form.Kind, err)
	}
	p.skipWhitespace()
	return true, nil
}

// dropTarget is one object a DROP names.
type dropTarget struct {
	name       string
	table      string
	parameters *string
}

// parseDropTargets reads the names of a DROP, and what the form takes after each.
func (p *Parser) parseDropTargets(form descriptor.Drop) ([]dropTarget, error) {
	label := strings.ToLower(form.Kind) + " name"
	var targets []dropTarget
	for {
		name, err := p.parseTableName(label)
		if err != nil {
			return nil, err
		}
		target := dropTarget{name: name}
		p.skipWhitespace()
		if form.Arguments && p.current.MatchOperatorValue("(") {
			parameters, err := p.collectParenthesizedBody("routine arguments")
			if err != nil {
				return nil, err
			}
			target.parameters = &parameters
			p.skipWhitespace()
		}
		if form.OnTable {
			if err := p.expect(lexer.TokenIdentifier, "ON"); err != nil {
				return nil, fmt.Errorf("expected ON after DROP TRIGGER %s: %w", name, err)
			}
			p.skipWhitespace()
			if target.table, err = p.parseTableName("table name"); err != nil {
				return nil, err
			}
			p.skipWhitespace()
		}
		targets = append(targets, target)
		if !form.List || !p.current.MatchOperatorValue(",") {
			return targets, nil
		}
		p.advance()
		p.skipWhitespace()
	}
}

// parseDropBehavior reads CASCADE or RESTRICT where the form takes them, and
// reports CASCADE.
func (p *Parser) parseDropBehavior(form descriptor.Drop) (bool, error) {
	if !form.Behavior {
		return false, nil
	}
	cascade := p.current.MatchIdentifierValue("CASCADE")
	if cascade || p.current.MatchIdentifierValue("RESTRICT") {
		p.advance()
		p.skipWhitespace()
	}
	return cascade, nil
}

// parseDropSync reads SYNC where the form takes it. It waits for the drop to finish
// and changes nothing about the schema it leaves.
func (p *Parser) parseDropSync(form descriptor.Drop) {
	if form.Sync && p.current.MatchIdentifierValue("SYNC") {
		p.advance()
		p.skipWhitespace()
	}
}

// dropNodes builds the nodes of a DROP: one node, or a list of one node per object.
func dropNodes(kind string, targets []dropTarget, ifExists, cascade bool) ast.Node {
	nodes := make([]ast.Node, 0, len(targets))
	for _, target := range targets {
		nodes = append(nodes, dropNode(kind, target, ifExists, cascade))
	}
	if len(nodes) == 1 {
		return nodes[0]
	}
	return &ast.StatementList{Statements: nodes}
}

func dropNode(kind string, target dropTarget, ifExists, cascade bool) ast.Node {
	switch kind {
	case descriptor.DropKindView:
		return &ast.DropViewNode{Name: target.name, IfExists: ifExists, Cascade: cascade}
	case descriptor.DropKindMaterializedView:
		return &ast.DropMaterializedViewNode{Name: target.name, IfExists: ifExists, Cascade: cascade}
	case descriptor.DropKindTrigger:
		return &ast.DropTriggerNode{Name: target.name, Table: target.table, IfExists: ifExists, Cascade: cascade}
	case descriptor.DropKindSequence:
		schema, name := splitLastPart(target.name)
		return &ast.DropSequenceNode{Schema: schema, Name: name, IfExists: ifExists, Cascade: cascade}
	case descriptor.DropKindType:
		return &ast.DropTypeNode{Name: target.name, IfExists: ifExists, Cascade: cascade}
	default:
		routine := string(ast.RoutineKindFunction)
		if kind != descriptor.DropKindFunction {
			routine = string(ast.RoutineKindProcedure)
		}
		return &ast.DropFunctionNode{Name: target.name, Kind: routine, Parameters: target.parameters, IfExists: ifExists, Cascade: cascade}
	}
}

// splitLastPart splits a qualified name into what qualifies it and its last part.
func splitLastPart(qualified string) (schema, name string) {
	parts := splitQualifiedIdentifier(qualified)
	if len(parts) == 1 {
		return "", parts[0]
	}
	return strings.Join(parts[:len(parts)-1], "."), parts[len(parts)-1]
}

// endStatement refuses what follows a statement this parser read to its end. A
// word there is a clause this reading does not hold, which an engine may have. A
// batch reads a next statement with no semicolon in between, where the engine's
// clients have a batch separator.
func (p *Parser) endStatement(statement string) error {
	p.skipWhitespace()
	if p.isAtEnd() || p.current.Type == lexer.TokenSemicolon || p.target.batches {
		return nil
	}
	if p.current.Type == lexer.TokenIdentifier {
		return ptaherr.At(p.current.Start, fmt.Errorf("unsupported %s clause: %s", statement, p.current.Value))
	}
	return ptaherr.At(p.current.Start, fmt.Errorf("expected the end of %s, got %s", statement, p.current.Type))
}
