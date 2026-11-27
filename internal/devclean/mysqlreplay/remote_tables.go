package mysqlreplay

import (
	"strings"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
	"ptah.run/internal/mysqlstored"
)

// remoteTableOperation names a CREATE or ALTER TABLE option that can make
// writes reach account tables through a table in the dev database. A server
// name or connection string does not prove where the rows live, so every
// realm refuses the option, including a run that trusts its SQL.
func remoteTableOperation(tokens []lexer.Token, reading mysqlstored.Server) string {
	if !isKeyword(replay.TokenAt(tokens, 0), "CREATE", "ALTER") {
		return ""
	}
	kind := replay.StatementObjectKindIndex(tokens)
	if !isKeyword(replay.TokenAt(tokens, kind), "TABLE") {
		return ""
	}
	_, start := replay.QualifiedIdentifierAt(tokens, replay.ObjectNameIndex(tokens, kind))
	options := tokens[start:]
	alter := isKeyword(tokens[0], "ALTER")
	if !alter && isOperatorAt(options, 0, "(") {
		// The initial list defines columns and constraints, not table options.
		options = options[tableDefinitionEnd(options):]
	}
	return remoteTableOptions(options, reading, alter)
}

// tableDefinitionEnd returns the index after the closing parenthesis of the
// first group, or the end of tokens when the group is incomplete.
func tableDefinitionEnd(tokens []lexer.Token) int {
	depth := 0
	for index, token := range tokens {
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return index + 1
			}
		}
	}
	return len(tokens)
}

// remoteTableOptions reads every table option, including repeated options
// and options of named partitions. A column change's names and a query's
// expressions are not options. A quoted engine value follows the session's
// string rules, just as the statement's tokenization does.
func remoteTableOptions(tokens []lexer.Token, reading mysqlstored.Server, alter bool) string {
	optionLevels := []bool{true}
	skipClause := false
	for index := 0; index < len(tokens); index++ {
		token := tokens[index]
		depth := len(optionLevels) - 1
		switch {
		case token.MatchOperatorValue("("):
			optionLevels = append(optionLevels, false)
			continue
		case token.MatchOperatorValue(")"):
			optionLevels = optionLevels[:max(depth, 1)]
			continue
		case token.MatchOperatorValue(","):
			optionLevels[depth] = depth == 0
			if depth == 0 {
				skipClause = false
			}
			continue
		case depth == 0 && isKeyword(token, "SELECT", "WITH", "AS"):
			return ""
		case depth == 0 && alter && isKeyword(token, "ADD", "CHANGE", "MODIFY", "DROP", "RENAME", "ALTER", "CONVERT", "ORDER"):
			skipClause = !isKeyword(replay.TokenAt(tokens, index+1), "PARTITION")
		}
		if skipClause {
			continue
		}
		if depth > 0 && isKeyword(token, "PARTITION", "SUBPARTITION") &&
			(isOperatorAt(tokens, index-1, "(") || isOperatorAt(tokens, index-1, ",")) {
			optionLevels[depth] = true
			index++ // The partition's name is not an option.
			continue
		}
		if !optionLevels[depth] {
			continue
		}
		if isKeyword(token, "CONNECTION") {
			return "CONNECTION table option that can forward writes to account tables"
		}
		if !isKeyword(token, "ENGINE") {
			continue
		}
		engine, next := remoteEngineValue(tokens, index+1, reading)
		index = next
		if engine != "" {
			return engine + " storage engine that can forward writes to account tables"
		}
	}
	return ""
}

// remoteEngineValue reads the value after ENGINE, with an optional equals
// sign, and returns its name only for a forwarding engine. The index it
// returns is the value's, so a caller does not read it as another option.
func remoteEngineValue(tokens []lexer.Token, at int, reading mysqlstored.Server) (string, int) {
	if isOperatorAt(tokens, at, "=") {
		at++
	}
	value := replay.TokenAt(tokens, at)
	engine := replay.NormalizedIdentifier(value)
	if value.Type == lexer.TokenString {
		decoded, _ := lexer.StringValue(value.Value, mysqlstored.LexerOptions(reading))
		engine = strings.ToUpper(decoded)
	}
	switch engine {
	case "FEDERATED", "SPIDER", "CONNECT":
		return engine, at
	default:
		return "", at
	}
}
