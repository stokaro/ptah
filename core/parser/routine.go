package parser

import (
	"fmt"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/lexer"
	"github.com/stokaro/ptah/core/platform"
)

type routineParser interface {
	parseCreateRoutine(p *Parser, target string, statementStart int) (ast.Node, error)
	parseCreateDefinerRoutine(p *Parser, statementStart int) (ast.Node, error)
}

func (p *Parser) routineParser() routineParser {
	switch p.dialect {
	case platform.MySQL, platform.MariaDB:
		return mysqlFamilyRoutineParser{}
	case platform.SQLServer:
		return sqlServerRoutineParser{}
	default:
		if platform.IsPostgresFamily(p.dialect) {
			return postgresRoutineParser{}
		}
		return compatibilityRoutineParser{}
	}
}

func (p *Parser) isSQLServerRoutineDialect() bool {
	return p.dialect == platform.SQLServer
}

type compatibilityRoutineParser struct{}

func (compatibilityRoutineParser) parseCreateRoutine(p *Parser, target string, statementStart int) (ast.Node, error) {
	switch target {
	case "PROCEDURE":
		return p.parseCreateRawRoutineStatement(statementStart)
	case "PROC":
		return p.parseCreateRawSQLServerRoutine(statementStart)
	}
	return p.parseCreateFunction(statementStart)
}

func (compatibilityRoutineParser) parseCreateDefinerRoutine(p *Parser, statementStart int) (ast.Node, error) {
	return p.parseCreateDefinerRoutineBestEffort(statementStart)
}

type mysqlFamilyRoutineParser struct{}

func (mysqlFamilyRoutineParser) parseCreateRoutine(p *Parser, target string, statementStart int) (ast.Node, error) {
	if target == "PROCEDURE" {
		return p.parseCreateMySQLRoutineStatement(statementStart, ast.RoutineKindProcedure)
	}
	if target == "PROC" {
		return compatibilityRoutineParser{}.parseCreateRoutine(p, target, statementStart)
	}
	return p.parseCreateMySQLRoutineStatement(statementStart, ast.RoutineKindFunction)
}

func (mysqlFamilyRoutineParser) parseCreateDefinerRoutine(p *Parser, statementStart int) (ast.Node, error) {
	for !p.isAtEnd() {
		if err := p.checkTimeout(); err != nil {
			return nil, err
		}
		switch {
		case p.current.MatchIdentifierValue("FUNCTION"):
			return p.parseCreateMySQLRoutineStatement(statementStart, ast.RoutineKindFunction)
		case p.current.MatchIdentifierValue("PROCEDURE"):
			return p.parseCreateMySQLRoutineStatement(statementStart, ast.RoutineKindProcedure)
		case p.current.Type == lexer.TokenSemicolon:
			return nil, fmt.Errorf("unsupported CREATE DEFINER target at position %d", p.current.Start)
		default:
			p.advance()
		}
	}
	return nil, fmt.Errorf("unsupported CREATE DEFINER target at position %d", p.current.Start)
}

type sqlServerRoutineParser struct{}

func (sqlServerRoutineParser) parseCreateRoutine(p *Parser, target string, statementStart int) (ast.Node, error) {
	switch target {
	case "FUNCTION":
		return p.parseCreateSQLServerRoutineStatement(statementStart, ast.RoutineKindFunction)
	case "PROC", "PROCEDURE":
		return p.parseCreateSQLServerRoutineStatement(statementStart, ast.RoutineKindProcedure)
	}
	return compatibilityRoutineParser{}.parseCreateRoutine(p, target, statementStart)
}

func (sqlServerRoutineParser) parseCreateDefinerRoutine(p *Parser, statementStart int) (ast.Node, error) {
	return compatibilityRoutineParser{}.parseCreateDefinerRoutine(p, statementStart)
}
