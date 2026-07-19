package parser

import (
	"github.com/stokaro/ptah/core/ast"
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
		return compatibilityRoutineParser{}
	}
}

type compatibilityRoutineParser struct{}

func (compatibilityRoutineParser) parseCreateRoutine(p *Parser, target string, statementStart int) (ast.Node, error) {
	if target == "PROCEDURE" {
		return p.parseCreateRawRoutineStatement(statementStart)
	}
	return p.parseCreateFunction(statementStart)
}

func (compatibilityRoutineParser) parseCreateDefinerRoutine(p *Parser, statementStart int) (ast.Node, error) {
	return p.parseCreateDefinerRoutineBestEffort(statementStart)
}

type mysqlFamilyRoutineParser struct{}

func (mysqlFamilyRoutineParser) parseCreateRoutine(p *Parser, target string, statementStart int) (ast.Node, error) {
	if target == "PROCEDURE" {
		return p.parseCreateRawRoutineStatement(statementStart)
	}
	return p.parseCreateFunction(statementStart)
}

func (mysqlFamilyRoutineParser) parseCreateDefinerRoutine(p *Parser, statementStart int) (ast.Node, error) {
	return p.parseCreateDefinerRoutineBestEffort(statementStart)
}

type sqlServerRoutineParser struct{}

func (sqlServerRoutineParser) parseCreateRoutine(p *Parser, target string, statementStart int) (ast.Node, error) {
	if target == "FUNCTION" || target == "PROCEDURE" {
		return p.parseCreateRawSQLServerRoutine(statementStart)
	}
	return compatibilityRoutineParser{}.parseCreateRoutine(p, target, statementStart)
}

func (sqlServerRoutineParser) parseCreateDefinerRoutine(p *Parser, statementStart int) (ast.Node, error) {
	return compatibilityRoutineParser{}.parseCreateDefinerRoutine(p, statementStart)
}
