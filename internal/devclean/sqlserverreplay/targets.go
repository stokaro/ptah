package sqlserverreplay

import (
	"strings"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// sqlServerWriteTargets returns the names an INSERT, UPDATE, DELETE or MERGE
// writes to, each as its parts. tokens is the whole statement, with the common
// table expressions in front of it, and executable is the statement they serve
// (see [sqlServerExecutableTokens]).
//
// The target stands after TOP and after INTO or FROM, which T-SQL lets the
// statement leave out: INSERT other.dbo.t VALUES (1) and DELETE other.dbo.t
// write as INSERT INTO and DELETE FROM do. A one-part target of an UPDATE or a
// DELETE is the table its FROM clause exposes under that name (see
// [sqlServerFromTargets]). OUTPUT ... INTO writes too. A target a common table
// expression stands for writes the tables it reads, so every name the
// definitions hold is a target then (measured on SQL Server 2025: WITH c AS
// (SELECT id FROM other.dbo.kept) UPDATE c SET id = 9 writes into other, through
// a second common table expression as well).
func sqlServerWriteTargets(tokens, executable []lexer.Token) [][]string {
	index := sqlServerMutationDestinationIndex(executable)
	if index == replay.MutationTargetNotFound {
		return nil
	}
	var targets [][]string
	target, next := replay.QualifiedIdentifierAt(executable, index)
	if len(target) > 0 {
		targets = append(targets, target)
	}
	switch replay.NormalizedIdentifier(executable[0]) {
	case "UPDATE", "DELETE":
		if len(target) == 1 {
			targets = append(targets, sqlServerFromTargets(executable, next, target[0])...)
		}
	}
	targets = append(targets, sqlServerOutputIntoTargets(executable)...)
	return append(targets, sqlServerCTETargets(tokens[:len(tokens)-len(executable)], targets)...)
}

// sqlServerFromTargets returns the tables the FROM clause after start exposes
// as name, which an UPDATE or a DELETE whose target is name writes to: a table
// or a derived table whose alias is name, and a table whose own name ends with
// name. The server binds UPDATE kept SET id = 9 FROM other.dbo.kept to the
// table in other, and UPDATE x ... FROM other.dbo.kept AS x as well (measured
// on SQL Server 2025). A derived table writes the table it reads, so every name
// it holds is a target. The match is wider than the server's, which binds a
// table that has an alias only by the alias: a wider match can only refuse
// more.
func sqlServerFromTargets(tokens []lexer.Token, start int, name string) [][]string {
	from := sqlServerTopLevelKeyword(tokens, "FROM", start)
	if from == replay.MutationTargetNotFound {
		return nil
	}
	var targets [][]string
	for index := from + 1; index < len(tokens); index++ {
		if !sqlServerSourceStart(tokens, index, from) {
			continue
		}
		targets = append(targets, sqlServerSourceTargets(tokens, index, name)...)
	}
	return targets
}

// sqlServerSourceStart reports whether a table source can start at index of
// a FROM clause whose FROM is at from: right after FROM, a comma, JOIN or APPLY,
// or an opening parenthesis. A comma inside a function's arguments is read as
// one too, which can only make a name a candidate that is none.
func sqlServerSourceStart(tokens []lexer.Token, index, from int) bool {
	if index == from+1 {
		return true
	}
	previous := tokens[index-1]
	return previous.MatchOperatorValue(",") || previous.MatchOperatorValue("(") ||
		previous.MatchIdentifierValue("JOIN") || previous.MatchIdentifierValue("APPLY")
}

// sqlServerSourceTargets returns what the table source at index writes to
// when an UPDATE or a DELETE names it as name: the table, or every name a
// derived table holds, or nothing when the source does not expose name. A
// function's arguments are skipped, so its alias is read: an inline function
// is updatable as a view is. Anything in parentheses with name for its alias is
// taken for a derived table, as a VALUES list is; a parenthesized join takes no
// alias.
func sqlServerSourceTargets(tokens []lexer.Token, index int, name string) [][]string {
	if tokens[index].MatchOperatorValue("(") {
		end := skipSQLServerParenthesized(tokens, index)
		if !strings.EqualFold(sqlServerAliasAt(tokens, end), name) {
			return nil
		}
		return sqlServerNamesIn(tokens[index:end])
	}
	parts, next := replay.QualifiedIdentifierAt(tokens, index)
	if len(parts) == 0 {
		return nil
	}
	if next < len(tokens) && tokens[next].MatchOperatorValue("(") {
		next = skipSQLServerParenthesized(tokens, next)
	}
	if strings.EqualFold(parts[len(parts)-1], name) || strings.EqualFold(sqlServerAliasAt(tokens, next), name) {
		return [][]string{parts}
	}
	return nil
}

// sqlServerAliasAt returns the alias a table source gives itself at index,
// after an optional AS, or "". Any name there is taken for an alias, a keyword
// too: no table is named after one, so a keyword taken for an alias matches no
// target.
func sqlServerAliasAt(tokens []lexer.Token, index int) string {
	if sqlServerKeywordSequenceAt(tokens, index, "AS") {
		index++
	}
	if index >= len(tokens) {
		return ""
	}
	return replay.IdentifierValue(tokens[index])
}

// sqlServerOutputIntoTargets returns the name after every INTO that follows
// an OUTPUT at the statement's top level, which is the table OUTPUT ... INTO
// writes. INTO stands nowhere else there once the target is read.
func sqlServerOutputIntoTargets(tokens []lexer.Token) [][]string {
	output := sqlServerTopLevelKeyword(tokens, "OUTPUT", 0)
	if output == replay.MutationTargetNotFound {
		return nil
	}
	var targets [][]string
	for into := sqlServerTopLevelKeyword(tokens, "INTO", output+1); into != replay.MutationTargetNotFound; into = sqlServerTopLevelKeyword(tokens, "INTO", into+1) {
		if target, _ := replay.QualifiedIdentifierAt(tokens, into+1); len(target) > 0 {
			targets = append(targets, target)
		}
	}
	return targets
}

// sqlServerCTETargets returns every name the common table expression
// definitions hold when one of targets is the name of one of them, and
// nothing otherwise. definitions is the statement's WITH clause, or empty.
func sqlServerCTETargets(definitions []lexer.Token, targets [][]string) [][]string {
	if len(definitions) == 0 {
		return nil
	}
	names := sqlServerCTENames(definitions)
	for _, target := range targets {
		if len(target) != 1 {
			continue
		}
		for _, name := range names {
			if strings.EqualFold(target[0], name) {
				return sqlServerNamesIn(definitions)
			}
		}
	}
	return nil
}

// sqlServerCTENames returns the names a WITH clause defines: WITH a AS (...),
// b (x) AS (...).
func sqlServerCTENames(definitions []lexer.Token) []string {
	var names []string
	for index := 1; index < len(definitions); {
		names = append(names, replay.IdentifierValue(definitions[index]))
		index++
		if index < len(definitions) && definitions[index].MatchOperatorValue("(") {
			index = skipSQLServerParenthesized(definitions, index)
		}
		if !sqlServerKeywordSequenceAt(definitions, index, "AS") {
			return names
		}
		index = skipSQLServerParenthesized(definitions, index+1)
		if index >= len(definitions) || !definitions[index].MatchOperatorValue(",") {
			return names
		}
		index++
	}
	return names
}

// sqlServerNamesIn returns every name tokens hold, each read whole from its
// first part, columns and aliases included.
func sqlServerNamesIn(tokens []lexer.Token) [][]string {
	var names [][]string
	for index := 0; index < len(tokens); index++ {
		name, next := replay.QualifiedIdentifierAt(tokens, index)
		if len(name) == 0 {
			continue
		}
		names = append(names, name)
		index = next - 1
	}
	return names
}

// sqlServerTopLevelKeyword returns the index of the first value outside
// parentheses from start on, or [replay.MutationTargetNotFound]. The FROM of
// a subquery in the SET clause is not the FROM an UPDATE binds its target in.
func sqlServerTopLevelKeyword(tokens []lexer.Token, value string, start int) int {
	depth := 0
	for index := max(start, 0); index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth--
		case depth == 0 && tokens[index].MatchIdentifierValue(value):
			return index
		}
	}
	return replay.MutationTargetNotFound
}
