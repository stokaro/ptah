package ydbcoordination

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbtype"
)

// Verb is what a statement does to a node.
type Verb int

// The three statements Ptah writes for a coordination node.
const (
	// Create creates a node: `CREATE COORDINATION NODE path [WITH (...)]`.
	Create Verb = iota + 1
	// Alter changes the settings it names: `ALTER COORDINATION NODE path SET
	// (...)`.
	Alter
	// Drop drops a node: `DROP COORDINATION NODE path`.
	Drop
)

// keyword is the verb's first word.
func (v Verb) keyword() string {
	switch v {
	case Create:
		return "CREATE"
	case Alter:
		return "ALTER"
	case Drop:
		return "DROP"
	default:
		return ""
	}
}

// Statement is one of Ptah's coordination node statements.
type Statement struct {
	Verb Verb
	// Path is the node's path as the statement names it: relative to the
	// TablePathPrefix in effect, or to the database root where none is, or
	// absolute where it starts with a slash.
	Path string
	// Spec is the configuration a CREATE gives the node, or the settings an
	// ALTER changes. A DROP carries none.
	Spec Spec
}

// ErrStatement is returned for text that is one of Ptah's coordination node
// statements and cannot be run as written.
var ErrStatement = errors.New("invalid coordination node statement")

// Text writes the statement without a terminator. The settings are written in
// the order of [Settings], and only the ones the spec sets.
func (s Statement) Text() (string, error) {
	keyword := s.Verb.keyword()
	if keyword == "" {
		return "", fmt.Errorf("%w: unknown verb %d", ErrStatement, s.Verb)
	}
	text := keyword + " COORDINATION NODE " + sqlident.Quote(platform.YDB, s.Path)
	options := optionList(s.Spec)
	switch {
	case s.Verb == Create && options != "":
		text += " WITH (" + options + ")"
	case s.Verb == Alter && options == "":
		return "", fmt.Errorf("%w: ALTER COORDINATION NODE %s names no setting to change", ErrStatement, s.Path)
	case s.Verb == Alter:
		text += " SET (" + options + ")"
	case s.Verb == Drop && options != "":
		return "", fmt.Errorf("%w: DROP COORDINATION NODE %s takes no setting", ErrStatement, s.Path)
	}
	return text, nil
}

// optionList writes the settings spec sets as `name = value` pairs.
func optionList(spec Spec) string {
	attributes := Attributes(spec)
	options := make([]string, 0, len(attributes))
	for _, attribute := range attributes {
		value := ydbtype.StringLiteral(attribute[1])
		if attribute[0] == SettingSelfCheckPeriod || attribute[0] == SettingSessionGracePeriod {
			value = "Interval(" + value + ")"
		}
		options = append(options, attribute[0]+" = "+value)
	}
	return strings.Join(options, ", ")
}

// Query is a query that runs one of Ptah's coordination node statements.
type Query struct {
	Statement
	// PathPrefix is the path the query's `PRAGMA TablePathPrefix` sets, or
	// empty where it sets none.
	PathPrefix string
}

// Absolute is the absolute path of the node the query names, on the
// database whose absolute path is database.
//
// A relative TablePathPrefix is refused, as YDB refuses one: measured on
// 25.1.4.7 and 26.2.1.14, `PRAGMA TablePathPrefix("relp"); CREATE TABLE t
// (...)` answers `Table path not in database, path: relp/t, database:
// /local`.
func (q Query) Absolute(database string) (string, error) {
	if strings.HasPrefix(q.Path, "/") {
		return path.Clean(q.Path), nil
	}
	switch {
	case q.PathPrefix == "":
		return path.Join("/"+strings.Trim(database, "/"), q.Path), nil
	case strings.HasPrefix(q.PathPrefix, "/"):
		return path.Join(q.PathPrefix, q.Path), nil
	default:
		return "", fmt.Errorf("%w: path %s is not in database %s: the TablePathPrefix %q is relative, and YDB "+
			"reads a prefix only as an absolute path", ErrStatement, path.Join(q.PathPrefix, q.Path),
			"/"+strings.Trim(database, "/"), q.PathPrefix)
	}
}

// Recognize reports whether text is a query running one of Ptah's
// coordination node statements, and reads it.
//
// Such a query is the statement, preceded by nothing but definitions: the
// translation setting at its head, PRAGMA, DECLARE, DEFINE and IMPORT
// statements and named expressions, which a migration carries into every
// query that follows them. Of those, only `PRAGMA TablePathPrefix` changes
// what the statement names. A statement mixed with any other, or not last in
// its query, is refused with [ErrStatement], since the coordination service
// runs the one statement and nothing else; so is one Ptah cannot read. Text
// that holds none of the three statements, a string or a comment that merely
// mentions one included, is not recognized and reports no error.
func Recognize(text string) (Query, bool, error) {
	if !containsFold(text, "coordination") {
		return Query{}, false, nil
	}
	statements := sqlutil.SplitSourceStatements(text, platform.YDB)
	tokens := make([][]lexer.Token, len(statements))
	found := -1
	for i, statement := range statements {
		tokens[i] = significantTokens(statement.Text)
		if !isCoordinationStatement(tokens[i]) {
			continue
		}
		if found >= 0 {
			return Query{}, true, fmt.Errorf("%w: a query runs one coordination node statement, and this one "+
				"holds two; put each in a query of its own", ErrStatement)
		}
		found = i
	}
	if found < 0 {
		return Query{}, false, nil
	}
	if found != len(statements)-1 {
		return Query{}, true, fmt.Errorf("%w: %s is followed by another statement in its query, and Ptah runs "+
			"it through YDB's coordination service, which takes it alone", ErrStatement, firstWords(tokens[found]))
	}
	var query Query
	for _, definition := range tokens[:found] {
		prefix, isPrefix, err := readDefinition(definition, firstWords(tokens[found]))
		if err != nil {
			return Query{}, true, err
		}
		if isPrefix {
			query.PathPrefix = prefix
		}
	}
	statement, err := parseStatement(tokens[found])
	if err != nil {
		return Query{}, true, err
	}
	query.Statement = statement
	return query, true, nil
}

// containsFold reports whether text contains word, ignoring ASCII case,
// without copying text: every query a YDB connection runs is asked.
func containsFold(text, word string) bool {
	for i := 0; i+len(word) <= len(text); i++ {
		if strings.EqualFold(text[i:i+len(word)], word) {
			return true
		}
	}
	return false
}

// significantTokens are the tokens of a statement that carry meaning: not
// whitespace, not comments, and not the translation setting at the head of a
// query.
func significantTokens(statement string) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.YDB))
	var tokens []lexer.Token
	for {
		token := lexr.NextToken()
		switch {
		case token.Type == lexer.TokenEOF:
			return tokens
		case token.Type == lexer.TokenWhitespace, token.Type == lexer.TokenComment,
			token.Type == lexer.TokenSemicolon:
			continue
		case token.Type == lexer.TokenUnknown && strings.HasPrefix(token.Value, "--!"):
			continue
		default:
			tokens = append(tokens, token)
		}
	}
}

// isCoordinationStatement reports whether tokens open one of the three
// statements.
func isCoordinationStatement(tokens []lexer.Token) bool {
	if len(tokens) < 3 || !tokens[1].MatchIdentifierValue("COORDINATION") || !tokens[2].MatchIdentifierValue("NODE") {
		return false
	}
	return tokens[0].MatchIdentifierValue("CREATE") || tokens[0].MatchIdentifierValue("ALTER") ||
		tokens[0].MatchIdentifierValue("DROP")
}

// firstWords names a statement by its opening words, for a message.
func firstWords(tokens []lexer.Token) string {
	words := make([]string, 0, 3)
	for _, token := range tokens[:min(3, len(tokens))] {
		words = append(words, strings.ToUpper(token.Value))
	}
	return strings.Join(words, " ")
}

// definitionVerbs open the statements that define a name or a setting for
// the rest of their query.
var definitionVerbs = []string{"PRAGMA", "DECLARE", "DEFINE", "IMPORT"}

// readDefinition checks that tokens are a definition, and reads the path a
// TablePathPrefix pragma sets.
func readDefinition(tokens []lexer.Token, statement string) (prefix string, isPrefix bool, err error) {
	if len(tokens) == 0 {
		return "", false, nil
	}
	first := tokens[0]
	isNamed := first.Type == lexer.TokenIdentifier && strings.HasPrefix(first.Value, "$")
	if !isNamed && !slices.ContainsFunc(definitionVerbs, first.MatchIdentifierValue) {
		return "", false, fmt.Errorf("%w: %s shares its query with %s, and Ptah runs it through YDB's "+
			"coordination service, which takes it alone", ErrStatement, statement, firstWords(tokens))
	}
	if !first.MatchIdentifierValue("PRAGMA") || len(tokens) < 2 || !tokens[1].MatchIdentifierValue("TablePathPrefix") {
		return "", false, nil
	}
	// PRAGMA TablePathPrefix("path") or PRAGMA TablePathPrefix = "path".
	rest := tokens[2:]
	var value lexer.Token
	switch {
	case len(rest) == 3 && rest[0].MatchOperatorValue("(") && rest[2].MatchOperatorValue(")"):
		value = rest[1]
	case len(rest) == 2 && rest[0].MatchOperatorValue("="):
		value = rest[1]
	default:
		return "", false, fmt.Errorf("%w: %s follows a TablePathPrefix pragma Ptah cannot read; "+
			"write it as PRAGMA TablePathPrefix(\"/database/path\")", ErrStatement, statement)
	}
	text, ok := stringValue(value)
	if !ok {
		return "", false, fmt.Errorf("%w: %s follows a TablePathPrefix pragma whose path is not a plain "+
			"string", ErrStatement, statement)
	}
	return text, true, nil
}

// stringValue reads a YQL string token that holds no escape, with or without
// the Utf8 suffix. A coordination node statement holds nothing but paths,
// durations and mode names, none of which needs one.
func stringValue(token lexer.Token) (string, bool) {
	if token.Type != lexer.TokenString {
		return "", false
	}
	text := strings.TrimSuffix(token.Value, "u")
	if len(text) < 2 || (text[0] != '\'' && text[0] != '"') || text[len(text)-1] != text[0] {
		return "", false
	}
	inner := text[1 : len(text)-1]
	if strings.ContainsAny(inner, `\'"`) {
		return "", false
	}
	return inner, true
}

// parseStatement reads one of the three statements.
func parseStatement(tokens []lexer.Token) (Statement, error) {
	statement := Statement{Verb: Drop}
	switch {
	case tokens[0].MatchIdentifierValue("CREATE"):
		statement.Verb = Create
	case tokens[0].MatchIdentifierValue("ALTER"):
		statement.Verb = Alter
	}
	opening := firstWords(tokens)
	if len(tokens) < 4 {
		return Statement{}, fmt.Errorf("%w: %s names no node", ErrStatement, opening)
	}
	name := tokens[3]
	nodePath, ok := lexer.YQLIdentifierValue(name.Value)
	if name.Type != lexer.TokenIdentifier || !ok || nodePath == "" || strings.HasPrefix(nodePath, "$") {
		return Statement{}, fmt.Errorf("%w: %s %s does not name a node; write its path in backticks", ErrStatement,
			opening, name.Value)
	}
	statement.Path = nodePath
	rest := tokens[4:]
	clause := map[Verb]string{Create: "WITH", Alter: "SET"}[statement.Verb]
	switch {
	case len(rest) == 0 && statement.Verb == Alter:
		return Statement{}, fmt.Errorf("%w: ALTER COORDINATION NODE %s names no setting; write SET (...)",
			ErrStatement, nodePath)
	case len(rest) == 0:
		return statement, nil
	case clause == "" || !rest[0].MatchIdentifierValue(clause):
		return Statement{}, fmt.Errorf("%w: %s %s is followed by %s, which Ptah does not read", ErrStatement,
			opening, nodePath, rest[0].Value)
	}
	spec, err := parseOptions(rest[1:], opening+" "+nodePath)
	if err != nil {
		return Statement{}, err
	}
	statement.Spec = spec
	if statement.Verb == Create {
		if err := Validate(spec); err != nil {
			return Statement{}, fmt.Errorf("%w: %s %s: %w", ErrStatement, opening, nodePath, err)
		}
	}
	return statement, nil
}

// parseOptions reads `(name = value, ...)` and nothing after it.
func parseOptions(tokens []lexer.Token, subject string) (Spec, error) {
	var spec Spec
	refuse := func(format string, args ...any) (Spec, error) {
		return Spec{}, fmt.Errorf("%w: %s: %s", ErrStatement, subject, fmt.Sprintf(format, args...))
	}
	if len(tokens) == 0 || !tokens[0].MatchOperatorValue("(") {
		return refuse("the settings are written in parentheses")
	}
	seen := make(map[string]bool)
	i := 1
	for {
		if i+2 >= len(tokens) {
			return refuse("the settings end early; write name = value, separated by commas, in parentheses")
		}
		setting := strings.ToLower(tokens[i].Value)
		if tokens[i].Type != lexer.TokenIdentifier || !slices.Contains(Settings(), setting) {
			return refuse("%s is not a setting; the settings are %s", tokens[i].Value, strings.Join(Settings(), ", "))
		}
		if seen[setting] {
			return refuse("%s is set twice", setting)
		}
		seen[setting] = true
		if !tokens[i+1].MatchOperatorValue("=") {
			return refuse("%s is followed by %s, not =", setting, tokens[i+1].Value)
		}
		value, next, err := optionValue(tokens, i+2, setting)
		if err != nil {
			return refuse("%v", err)
		}
		if err := set(&spec, setting, value); err != nil {
			return refuse("%v", err)
		}
		if err := validateSetting(spec, setting); err != nil {
			return refuse("%v", err)
		}
		switch {
		case next < len(tokens) && tokens[next].MatchOperatorValue(","):
			i = next + 1
		case next == len(tokens)-1 && tokens[next].MatchOperatorValue(")"):
			return spec, nil
		default:
			return refuse("the settings end with ) and nothing follows it")
		}
	}
}

// optionValue reads the value of setting starting at tokens[at], and returns
// the index after it. A period is `Interval('...')`, a mode a string.
func optionValue(tokens []lexer.Token, at int, setting string) (string, int, error) {
	if setting != SettingSelfCheckPeriod && setting != SettingSessionGracePeriod {
		value, ok := stringValue(tokens[at])
		if !ok {
			return "", 0, fmt.Errorf("%s takes a string such as 'strict', not %s", setting, tokens[at].Value)
		}
		return value, at + 1, nil
	}
	if at+3 >= len(tokens) || !tokens[at].MatchIdentifierValue("Interval") || !tokens[at+1].MatchOperatorValue("(") ||
		!tokens[at+3].MatchOperatorValue(")") {
		return "", 0, fmt.Errorf("%s takes an interval such as Interval('PT1S')", setting)
	}
	value, ok := stringValue(tokens[at+2])
	if !ok {
		return "", 0, fmt.Errorf("%s takes an interval such as Interval('PT1S'), not %s", setting, tokens[at+2].Value)
	}
	return value, at + 4, nil
}

// validateSetting checks the one setting just read on its own: a mode's
// value, and a period's bounds that do not depend on another setting. The
// combination is checked where the node's other settings are known.
func validateSetting(spec Spec, setting string) error {
	switch setting {
	case SettingSelfCheckPeriod:
		return Validate(Spec{SelfCheckPeriodMillis: spec.SelfCheckPeriodMillis,
			SessionGracePeriodMillis: MaxSessionGracePeriodMillis})
	case SettingSessionGracePeriod:
		if spec.SessionGracePeriodMillis > MaxSessionGracePeriodMillis {
			return Validate(Spec{SessionGracePeriodMillis: spec.SessionGracePeriodMillis})
		}
		return nil
	default:
		return Validate(Spec{
			ReadConsistencyMode:     spec.ReadConsistencyMode,
			AttachConsistencyMode:   spec.AttachConsistencyMode,
			RateLimiterCountersMode: spec.RateLimiterCountersMode,
		})
	}
}
