package sqlreach

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// YQLReads is what a YQL query reads from the database it is sent to, as far
// as its text says: the names of the objects at its source positions, and the
// directory a TablePathPrefix pragma in front of it resolves them against.
type YQLReads struct {
	// Prefix is the directory a `PRAGMA TablePathPrefix` names, or empty.
	Prefix string
	// Sources are the names at the query's source positions, in the order
	// they are first written, without backticks.
	Sources []string
	// Selects are the text's SELECT statements, without the pragmas in front
	// of them.
	Selects []string
}

// errYQLSource starts every refusal of [ReadYQLSources].
var errYQLSource = errors.New("the objects a YDB query reads cannot be named from its text")

// ReadYQLSources names what text, one SELECT or a SELECT after
// TablePathPrefix pragmas, reads: every name written after FROM or JOIN, after
// ANY there, and after a comma in a list of sources. That is the half of a
// read-only proof the text can give. Whether a name is a table this database
// stores, or an external table whose rows come from another server, is the
// catalog's to say: measured on 25.1.4.7 and 26.2.1.14, a SELECT over an
// external table runs in a read-only transaction and fetches the table's
// files from the object storage it names, and so does a SELECT over a view
// that reads one.
//
// It is default-deny. A source it cannot name is refused rather than left
// out: a named expression, a call such as a table function, a parenthesized
// source that is not a subquery, a dotted or cluster name, a name holding a
// backtick or a backslash, a statement other than SELECT and the pragma, and
// any pragma but TablePathPrefix. Every SELECT is held to [YQLReadOnly] first,
// so a statement that writes, or that a number written against a word would
// hide a source in, never reaches the walk.
func ReadYQLSources(text string) (YQLReads, error) {
	var reads YQLReads
	for _, statement := range yqlStatements(text) {
		tokens := SignificantTokens(statement, true, platform.YDB)
		if len(tokens) == 0 {
			continue
		}
		if IsKeyword(tokens[0], "PRAGMA") {
			prefix, err := yqlTablePathPrefix(tokens)
			if err != nil {
				return YQLReads{}, err
			}
			reads.Prefix = prefix
			continue
		}
		if err := YQLReadOnly(statement); err != nil {
			return YQLReads{}, fmt.Errorf("%w: %w", errYQLSource, err)
		}
		reads.Selects = append(reads.Selects, strings.TrimSpace(statement))
		sources, err := yqlSourceNames(tokens)
		if err != nil {
			return YQLReads{}, err
		}
		for _, source := range sources {
			if !slices.Contains(reads.Sources, source) {
				reads.Sources = append(reads.Sources, source)
			}
		}
	}
	return reads, nil
}

// yqlStatements splits text at the semicolons that end its statements.
func yqlStatements(text string) []string {
	var statements []string
	start := 0
	for _, token := range SignificantTokens(text, true, platform.YDB) {
		if token.Type != lexer.TokenSemicolon {
			continue
		}
		statements = append(statements, text[start:token.Start])
		start = token.End
	}
	return append(statements, text[start:])
}

// yqlTablePathPrefix reads `PRAGMA TablePathPrefix = '<dir>'` and
// `PRAGMA TablePathPrefix('<dir>')`, and refuses every other pragma: one that
// changes how the query is read is a question the walk does not answer.
func yqlTablePathPrefix(tokens []lexer.Token) (string, error) {
	shapes := [][]string{{"PRAGMA", "TablePathPrefix", "=", "'"}, {"PRAGMA", "TablePathPrefix", "(", "'", ")"}}
	for _, shape := range shapes {
		if len(tokens) != len(shape) {
			continue
		}
		matched := true
		value := ""
		for i, want := range shape {
			token := tokens[i]
			switch want {
			case "'":
				text, ok := yqlPlainString(token)
				matched = matched && ok
				value = text
			case "=", "(", ")":
				matched = matched && token.MatchOperatorValue(want)
			default:
				matched = matched && IsKeyword(token, want)
			}
		}
		if matched {
			return value, nil
		}
	}
	return "", fmt.Errorf("%w: it carries a pragma other than TablePathPrefix", errYQLSource)
}

// yqlPlainString is the text of a single- or double-quoted string literal
// with no escape and no suffix in it.
func yqlPlainString(token lexer.Token) (string, bool) {
	value := token.Value
	if token.Type != lexer.TokenString || len(value) < 2 || strings.Contains(value, `\`) {
		return "", false
	}
	quote := value[0]
	if quote != '\'' && quote != '"' || value[len(value)-1] != quote {
		return "", false
	}
	inner := value[1 : len(value)-1]
	if strings.IndexByte(inner, quote) >= 0 {
		return "", false
	}
	return inner, true
}

// yqlSourceNames returns the name at every source position of one SELECT's
// tokens, or refuses the first source it cannot name.
func yqlSourceNames(tokens []lexer.Token) ([]string, error) {
	var names []string
	depth := 0
	lists := make(map[int]bool)
	for i, token := range tokens {
		switch {
		case token.MatchOperatorValue("("):
			depth++
			continue
		case token.MatchOperatorValue(")"):
			delete(lists, depth)
			depth--
			continue
		case token.Type == lexer.TokenIdentifier && slices.Contains(yqlSourceListEnds, strings.ToUpper(token.Value)):
			delete(lists, depth)
		}
		anchor := IsKeyword(token, "FROM") || IsKeyword(token, "JOIN")
		if anchor {
			lists[depth] = true
		}
		if !anchor && !(token.MatchOperatorValue(",") && lists[depth]) {
			continue
		}
		name, err := yqlSourceName(tokens, i+1)
		if err != nil {
			return nil, err
		}
		if name != "" {
			names = append(names, name)
		}
	}
	return names, nil
}

// yqlSourceName names the source starting at i: empty for a subquery, whose
// own sources the walk meets in turn, or a refusal for anything it cannot
// name.
func yqlSourceName(tokens []lexer.Token, i int) (string, error) {
	if i < len(tokens) && IsKeyword(tokens[i], "ANY") {
		i++
	}
	if i >= len(tokens) {
		return "", fmt.Errorf("%w: a source position holds nothing", errYQLSource)
	}
	token := tokens[i]
	next := lexer.Token{}
	if i+1 < len(tokens) {
		next = tokens[i+1]
	}
	switch {
	case token.MatchOperatorValue("("):
		if IsKeyword(next, "SELECT") || next.MatchOperatorValue("(") {
			return "", nil
		}
		return "", fmt.Errorf("%w: a parenthesized source is not a subquery", errYQLSource)
	case token.Type != lexer.TokenIdentifier || strings.HasPrefix(token.Value, "$"):
		return "", fmt.Errorf("%w: %s at a source position is not the name of an object", errYQLSource, token.Value)
	case next.MatchOperatorValue("("):
		return "", fmt.Errorf("%w: %s is called at a source position", errYQLSource, token.Value)
	case next.MatchOperatorValue(".") || next.MatchOperatorValue(":"):
		return "", fmt.Errorf("%w: %s names a cluster or an external source", errYQLSource, token.Value)
	}
	name := token.Value
	if strings.HasPrefix(name, "`") {
		inner := strings.TrimSuffix(strings.TrimPrefix(name, "`"), "`")
		if len(name) < 2 || !strings.HasSuffix(name, "`") || strings.ContainsAny(inner, "`\\") {
			return "", fmt.Errorf("%w: %s is quoted in a way the proof does not read", errYQLSource, name)
		}
		name = inner
	}
	if name == "" {
		return "", fmt.Errorf("%w: a source has an empty name", errYQLSource)
	}
	return name, nil
}
