package parser

import (
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/lexer"
)

func (p mysqlRoutineBodyParser) parseDeclareStatement(startIdx, endIdx int) *ast.MySQLRoutineDeclaration {
	nameIdx := p.nextSignificant(startIdx + 1)
	if nameIdx == -1 || nameIdx >= endIdx {
		return nil
	}

	if conditionIdx := p.nextSignificant(nameIdx + 1); conditionIdx != -1 &&
		conditionIdx < endIdx &&
		p.tokens[conditionIdx].MatchIdentifierValue("CONDITION") {
		return p.parseConditionDeclaration(nameIdx, conditionIdx, endIdx)
	}

	return p.parseVariableDeclaration(nameIdx, endIdx)
}

func (p mysqlRoutineBodyParser) parseConditionDeclaration(nameIdx, conditionIdx, endIdx int) *ast.MySQLRoutineDeclaration {
	forIdx := p.findSignificantKeyword(conditionIdx+1, endIdx, "FOR")
	if forIdx == -1 {
		return nil
	}
	valueStartIdx := p.nextSignificant(forIdx + 1)
	if valueStartIdx == -1 || valueStartIdx >= endIdx {
		return nil
	}

	return &ast.MySQLRoutineDeclaration{
		Kind:         ast.MySQLRoutineDeclarationCondition,
		Names:        []string{p.tokens[nameIdx].Value},
		ConditionSQL: p.rawTokenRange(valueStartIdx, p.statementContentEnd(endIdx)),
	}
}

func (p mysqlRoutineBodyParser) parseVariableDeclaration(nameIdx, endIdx int) *ast.MySQLRoutineDeclaration {
	names, typeStartIdx := p.parseDeclareNames(nameIdx, endIdx)
	if len(names) == 0 || typeStartIdx == -1 || typeStartIdx >= endIdx {
		return nil
	}

	defaultIdx := p.findSignificantKeyword(typeStartIdx, endIdx, "DEFAULT")
	typeEnd := p.statementContentEnd(endIdx)
	defaultSQL := ""
	if defaultIdx != -1 {
		typeEnd = p.tokens[defaultIdx].Start
		defaultStartIdx := p.nextSignificant(defaultIdx + 1)
		if defaultStartIdx == -1 || defaultStartIdx >= endIdx {
			return nil
		}
		defaultSQL = p.rawTokenRange(defaultStartIdx, p.statementContentEnd(endIdx))
	}

	return &ast.MySQLRoutineDeclaration{
		Kind:       ast.MySQLRoutineDeclarationVariable,
		Names:      names,
		TypeSQL:    strings.TrimSpace(p.rawFragment(p.tokens[typeStartIdx].Start, typeEnd)),
		DefaultSQL: defaultSQL,
	}
}

func (p mysqlRoutineBodyParser) parseDeclareNames(nameIdx, endIdx int) ([]string, int) {
	names := []string{p.tokens[nameIdx].Value}
	nextIdx := p.nextSignificant(nameIdx + 1)
	if nextIdx == -1 || nextIdx >= endIdx || !p.tokens[nextIdx].MatchOperatorValue(",") {
		return names, nextIdx
	}

	for nextIdx != -1 && nextIdx < endIdx && p.tokens[nextIdx].MatchOperatorValue(",") {
		nameIdx = p.nextSignificant(nextIdx + 1)
		if nameIdx == -1 || nameIdx >= endIdx || p.tokens[nameIdx].Type != lexer.TokenIdentifier {
			return names, -1
		}
		names = append(names, p.tokens[nameIdx].Value)
		nextIdx = p.nextSignificant(nameIdx + 1)
	}
	return names, nextIdx
}

func (p mysqlRoutineBodyParser) parseCursorStatement(startIdx, endIdx int) *ast.MySQLRoutineCursor {
	nameIdx := p.nextSignificant(startIdx + 1)
	if nameIdx == -1 || nameIdx >= endIdx {
		return nil
	}

	forIdx := p.findSignificantKeyword(nameIdx+1, endIdx, "FOR")
	if forIdx == -1 {
		return nil
	}
	selectIdx := p.nextSignificant(forIdx + 1)
	if selectIdx == -1 || selectIdx >= endIdx {
		return nil
	}

	return &ast.MySQLRoutineCursor{
		Name:      p.tokens[nameIdx].Value,
		SelectSQL: p.rawTokenRange(selectIdx, p.statementContentEnd(endIdx)),
	}
}

func (p mysqlRoutineBodyParser) parseHandlerStatement(startIdx, endIdx int) *ast.MySQLRoutineHandler {
	actionIdx := p.nextSignificant(startIdx + 1)
	if actionIdx == -1 || actionIdx >= endIdx {
		return nil
	}
	handlerIdx := p.findSignificantKeyword(actionIdx+1, endIdx, "HANDLER")
	if handlerIdx == -1 {
		return nil
	}
	forIdx := p.findSignificantKeyword(handlerIdx+1, endIdx, "FOR")
	if forIdx == -1 {
		return nil
	}

	conditionStartIdx := p.nextSignificant(forIdx + 1)
	if conditionStartIdx == -1 || conditionStartIdx >= endIdx {
		return nil
	}
	conditions, statementStartIdx := p.parseHandlerConditions(conditionStartIdx, endIdx)
	if statementStartIdx == -1 {
		return nil
	}

	return &ast.MySQLRoutineHandler{
		Action:       strings.ToUpper(p.tokens[actionIdx].Value),
		Conditions:   conditions,
		StatementSQL: p.rawTokenRange(statementStartIdx, p.statementContentEnd(endIdx)),
	}
}

func (p mysqlRoutineBodyParser) parseHandlerConditions(startIdx, endIdx int) ([]string, int) {
	conditions := make([]string, 0, 1)
	currentIdx := startIdx
	for currentIdx != -1 && currentIdx < endIdx {
		conditionEndIdx := p.handlerConditionEnd(currentIdx, endIdx)
		if conditionEndIdx == -1 {
			return nil, -1
		}
		conditions = append(conditions, p.rawTokenRange(currentIdx, p.tokens[conditionEndIdx].Start))

		nextIdx := p.nextSignificant(conditionEndIdx)
		if nextIdx == -1 || nextIdx >= endIdx {
			return nil, -1
		}
		if !p.tokens[nextIdx].MatchOperatorValue(",") {
			return conditions, nextIdx
		}
		currentIdx = p.nextSignificant(nextIdx + 1)
	}
	return nil, -1
}

func (p mysqlRoutineBodyParser) handlerConditionEnd(startIdx, endIdx int) int {
	if startIdx == -1 || startIdx >= endIdx {
		return -1
	}

	if p.tokens[startIdx].MatchIdentifierValue("SQLSTATE") {
		valueIdx := p.nextSignificant(startIdx + 1)
		if valueIdx != -1 && valueIdx < endIdx && p.tokens[valueIdx].MatchIdentifierValue("VALUE") {
			valueIdx = p.nextSignificant(valueIdx + 1)
		}
		if valueIdx == -1 || valueIdx >= endIdx {
			return -1
		}
		return p.nextSignificant(valueIdx + 1)
	}

	if p.tokens[startIdx].MatchIdentifierValue("NOT") {
		foundIdx := p.nextSignificant(startIdx + 1)
		if foundIdx != -1 && foundIdx < endIdx && p.tokens[foundIdx].MatchIdentifierValue("FOUND") {
			return p.nextSignificant(foundIdx + 1)
		}
	}

	return p.nextSignificant(startIdx + 1)
}
