package replay

import (
	"slices"
	"strings"

	"ptah.run/internal/lexer"
)

// ContainsIdentifier reports whether a token of tokens reads as value, an
// upper-case word compared as [NormalizedIdentifier] spells it.
func ContainsIdentifier(tokens []lexer.Token, value string) bool {
	for _, token := range tokens {
		if NormalizedIdentifier(token) == value {
			return true
		}
	}
	return false
}

// NormalizedIdentifier returns an identifier or string token without its
// quotes, with doubled quote characters undone, in upper case. Any other token
// reads as "".
func NormalizedIdentifier(token lexer.Token) string {
	value := token.Value
	switch token.Type {
	case lexer.TokenIdentifier:
	case lexer.TokenString:
	default:
		return ""
	}
	value = strings.TrimPrefix(value, "`")
	value = strings.TrimSuffix(value, "`")
	value = strings.TrimPrefix(value, `"`)
	value = strings.TrimSuffix(value, `"`)
	value = strings.TrimPrefix(value, "'")
	value = strings.TrimSuffix(value, "'")
	value = strings.TrimPrefix(value, "[")
	value = strings.TrimSuffix(value, "]")
	value = strings.ReplaceAll(value, "``", "`")
	value = strings.ReplaceAll(value, `""`, `"`)
	value = strings.ReplaceAll(value, "''", "'")
	value = strings.ReplaceAll(value, "]]", "]")
	return strings.ToUpper(value)
}

// MutationTargetNotFound is the index the token searches return when what
// they look for is absent.
const MutationTargetNotFound = -1

// IsDDLAction reports whether a statement begins with CREATE, ALTER, DROP or
// RENAME.
func IsDDLAction(tokens []lexer.Token) bool {
	if len(tokens) == 0 {
		return false
	}
	switch NormalizedIdentifier(tokens[0]) {
	case "CREATE", "ALTER", "DROP", "RENAME":
		return true
	default:
		return false
	}
}

// UsesTemporaryObject reports whether a DDL statement names a TEMP or
// TEMPORARY object.
func UsesTemporaryObject(tokens []lexer.Token) bool {
	if !IsDDLAction(tokens) {
		return false
	}
	kindIndex := StatementObjectKindIndex(tokens)
	if kindIndex == MutationTargetNotFound {
		return false
	}
	return ContainsIdentifier(tokens[1:kindIndex], "TEMP") ||
		ContainsIdentifier(tokens[1:kindIndex], "TEMPORARY")
}

// TopLevelValueAfterKeyword returns the word after keyword at the statement's
// top level, reading from start, and whether an = stood between them. It
// stops at a top-level SELECT, so a query's clauses are not read as the
// statement's own.
func TopLevelValueAfterKeyword(tokens []lexer.Token, keyword string, start int) (string, bool) {
	depth := 0
	for index := max(start, 0); index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && NormalizedIdentifier(tokens[index]) == "SELECT":
			return "", false
		case depth == 0 && NormalizedIdentifier(tokens[index]) == keyword:
			index++
			assigned := false
			if index < len(tokens) && tokens[index].MatchOperatorValue("=") {
				index++
				assigned = true
			}
			if index < len(tokens) {
				return NormalizedIdentifier(tokens[index]), assigned
			}
			return "", assigned
		}
	}
	return "", false
}

// CommaSeparatedQualifiedTargets reads the comma-separated list of qualified
// names that begins at start, up to stopKeyword at the top level, or to the
// end when stopKeyword is empty.
func CommaSeparatedQualifiedTargets(
	tokens []lexer.Token,
	start int,
	stopKeyword string,
) [][]string {
	var targets [][]string
	expectTarget := true
	depth := 0
	for index := start; index < len(tokens); index++ {
		if depth == 0 && stopKeyword != "" &&
			tokens[index].MatchIdentifierValue(stopKeyword) {
			return targets
		}
		if expectTarget {
			target, next := QualifiedIdentifierAt(tokens, index)
			if len(target) == 0 {
				continue
			}
			targets = append(targets, target)
			expectTarget = false
			index = next - 1
			continue
		}
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && tokens[index].MatchOperatorValue(","):
			expectTarget = true
		}
	}
	return targets
}

// LeadingCTEExecutableTokens returns the statement a leading WITH clause
// introduces, from its SELECT, INSERT, UPDATE, DELETE or MERGE on, or tokens
// as they are when there is none.
func LeadingCTEExecutableTokens(tokens []lexer.Token) []lexer.Token {
	if len(tokens) == 0 || !tokens[0].MatchIdentifierValue("WITH") {
		return tokens
	}
	depth := 0
	closedDefinition := false
	for index := 1; index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
			closedDefinition = depth == 0
		case depth == 0 && closedDefinition && isDMLStatementToken(tokens[index]):
			return tokens[index:]
		}
	}
	return tokens
}

func isDMLStatementToken(token lexer.Token) bool {
	switch {
	case token.MatchIdentifierValue("SELECT"),
		token.MatchIdentifierValue("INSERT"),
		token.MatchIdentifierValue("UPDATE"),
		token.MatchIdentifierValue("DELETE"),
		token.MatchIdentifierValue("MERGE"):
		return true
	default:
		return false
	}
}

// MutationTargets returns the qualified names a statement writes to, each as
// its parts: the target of an INSERT, UPDATE, DELETE or TRUNCATE, the object a
// DDL statement creates, alters or drops, and the table it is on.
func MutationTargets(tokens []lexer.Token) [][]string {
	if len(tokens) == 0 {
		return nil
	}
	first := NormalizedIdentifier(tokens[0])
	switch first {
	case "INSERT", "REPLACE", "MERGE":
		return targetAfterKeywordWithModifiers(tokens, "INTO", 1, "TABLE")
	case "UPDATE":
		return targetAfterModifiers(tokens, 1, "LOW_PRIORITY", "IGNORE", "ONLY")
	case "DELETE":
		return targetAfterKeyword(tokens, "FROM", 1)
	case "TRUNCATE":
		return targetAfterModifiers(tokens, 1, "TABLE", "ONLY")
	case "OPTIMIZE", "ANALYZE", "REPAIR":
		return targetAfterKeyword(tokens, "TABLE", 1)
	case "CREATE", "ALTER", "DROP":
		return ddlMutationTargets(tokens)
	case "RENAME":
		return targetAfterKeyword(tokens, "TABLE", 1)
	case "COMMENT":
		return targetAfterKeyword(tokens, "ON", 1)
	default:
		return nil
	}
}

func targetAfterKeywordWithModifiers(
	tokens []lexer.Token,
	keyword string,
	start int,
	modifiers ...string,
) [][]string {
	index := FindIdentifier(tokens, keyword, start)
	if index == MutationTargetNotFound {
		return nil
	}
	return targetAfterModifiers(tokens, SkipNameModifiers(tokens, index+1), modifiers...)
}

func ddlMutationTargets(tokens []lexer.Token) [][]string {
	kindIndex := StatementObjectKindIndex(tokens)
	if kindIndex == MutationTargetNotFound {
		return nil
	}
	kind := NormalizedIdentifier(tokens[kindIndex])
	if kind == "INDEX" || TokenSequenceAt(tokens, kindIndex, "UNIQUE", "INDEX") ||
		TokenSequenceAt(tokens, kindIndex, "FULLTEXT", "INDEX") ||
		TokenSequenceAt(tokens, kindIndex, "SPATIAL", "INDEX") ||
		TokenSequenceAt(tokens, kindIndex, "CLUSTERED", "INDEX") ||
		TokenSequenceAt(tokens, kindIndex, "NONCLUSTERED", "INDEX") {
		return targetAfterKeyword(tokens, "ON", kindIndex+1)
	}

	nameIndex := ObjectNameIndex(tokens, kindIndex)
	targets := qualifiedTargetAt(tokens, nameIndex)
	if kind == "TRIGGER" {
		targets = append(targets, targetAfterKeyword(tokens, "ON", nameIndex)...)
	}
	return targets
}

// StatementObjectKindIndex returns the index of the word naming the kind of
// object a statement acts on, such as TABLE or VIEW, among its first 16
// tokens, or [MutationTargetNotFound].
func StatementObjectKindIndex(tokens []lexer.Token) int {
	limit := min(len(tokens), 16)
	for index := 1; index < limit; index++ {
		if isStatementObjectKind(NormalizedIdentifier(tokens[index])) {
			return index
		}
	}
	return MutationTargetNotFound
}

func isStatementObjectKind(value string) bool {
	switch value {
	case "ACCESS", "AVAILABILITY", "CAST", "CREDENTIAL", "DATABASE",
		"DICTIONARY", "DOMAIN", "ENDPOINT", "EVENT", "EXTERNAL", "EXTENSION",
		"FOREIGN", "FUNCTION", "FULLTEXT", "INDEX", "LANGUAGE", "LIVE",
		"LOGIN", "LOGFILE", "MATERIALIZED", "NAMED", "NONCLUSTERED",
		"PROCEDURE", "PUBLICATION", "QUOTA", "RESOURCE", "ROLE", "ROW",
		"SCHEMA", "SEQUENCE", "SERVER", "SETTINGS", "SPATIAL", "SUBSCRIPTION",
		"SYNONYM", "TABLE", "TABLESPACE", "TEXT", "TRANSFORM", "TRIGGER", "TYPE",
		"UNDO", "UNIQUE", "USER", "VIEW", "WINDOW", "WORKLOAD":
		return true
	default:
		return false
	}
}

// ObjectNameIndex returns the index of the object name that follows the kind
// at kindIndex, past the rest of a multi-word kind and IF NOT EXISTS.
func ObjectNameIndex(tokens []lexer.Token, kindIndex int) int {
	index := kindIndex + 1
	switch NormalizedIdentifier(tokens[kindIndex]) {
	case "ACCESS":
		index = skipSequence(tokens, index, "METHOD")
	case "AVAILABILITY":
		index = skipSequence(tokens, index, "GROUP")
	case "EVENT":
		index = skipSequence(tokens, index, "TRIGGER")
	case "EXTERNAL":
		index = skipSequence(tokens, index, "TABLE")
	case "FOREIGN":
		index = skipSequence(tokens, index, "TABLE", "DATA", "WRAPPER")
	case "FULLTEXT", "UNIQUE", "CLUSTERED", "NONCLUSTERED":
		index = skipSequence(tokens, index, "INDEX")
	case "LIVE", "MATERIALIZED", "WINDOW":
		index = skipSequence(tokens, index, "VIEW")
	case "LOGFILE":
		index = skipSequence(tokens, index, "GROUP")
	case "NAMED":
		index = skipSequence(tokens, index, "COLLECTION")
	case "RESOURCE":
		index = skipSequence(tokens, index, "GROUP")
	case "ROW":
		index = skipSequence(tokens, index, "POLICY")
	case "SERVER":
		index = skipSequence(tokens, index, "ROLE", "AUDIT")
	case "SETTINGS":
		index = skipSequence(tokens, index, "PROFILE")
	case "SPATIAL":
		if TokenSequenceAt(tokens, index, "INDEX") {
			index++
		} else {
			index = skipSequence(tokens, index, "REFERENCE", "SYSTEM")
		}
	case "TEXT":
		index = skipSequence(tokens, index, "SEARCH")
	case "UNDO":
		index = skipSequence(tokens, index, "TABLESPACE")
	}
	return SkipNameModifiers(tokens, index)
}

func skipSequence(tokens []lexer.Token, index int, alternatives ...string) int {
	for index < len(tokens) && containsString(alternatives, NormalizedIdentifier(tokens[index])) {
		index++
	}
	return index
}

// SkipNameModifiers returns the index of the first token from index on that is
// not IF, NOT, EXISTS, ONLY or CONCURRENTLY.
func SkipNameModifiers(tokens []lexer.Token, index int) int {
	for index < len(tokens) {
		switch NormalizedIdentifier(tokens[index]) {
		case "IF", "NOT", "EXISTS", "ONLY", "CONCURRENTLY":
			index++
		default:
			return index
		}
	}
	return index
}

func targetAfterKeyword(tokens []lexer.Token, keyword string, start int) [][]string {
	index := FindIdentifier(tokens, keyword, start)
	if index == MutationTargetNotFound {
		return nil
	}
	return qualifiedTargetAt(tokens, SkipNameModifiers(tokens, index+1))
}

func targetAfterModifiers(tokens []lexer.Token, index int, modifiers ...string) [][]string {
	for index < len(tokens) && containsString(modifiers, NormalizedIdentifier(tokens[index])) {
		index++
	}
	return qualifiedTargetAt(tokens, index)
}

func qualifiedTargetAt(tokens []lexer.Token, index int) [][]string {
	parts, _ := QualifiedIdentifierAt(tokens, index)
	if len(parts) == 0 {
		return nil
	}
	return [][]string{parts}
}

// QualifiedIdentifierAt reads the dotted name that begins at index, as its
// unquoted parts, and returns the index after it. A token that is no name
// reads as no parts.
func QualifiedIdentifierAt(tokens []lexer.Token, index int) ([]string, int) {
	if index < 0 || index >= len(tokens) || NormalizedIdentifier(tokens[index]) == "" {
		return nil, index
	}
	parts := []string{IdentifierValue(tokens[index])}
	index++
	for index+1 < len(tokens) && tokens[index].MatchOperatorValue(".") {
		value := IdentifierValue(tokens[index+1])
		if value == "" {
			break
		}
		parts = append(parts, value)
		index += 2
	}
	return parts, index
}

// IdentifierValue returns an identifier or string token without its quotes and
// with doubled quote characters undone, in its own case. Any other token reads
// as "".
func IdentifierValue(token lexer.Token) string {
	value := token.Value
	switch token.Type {
	case lexer.TokenIdentifier, lexer.TokenString:
	default:
		return ""
	}
	value = strings.TrimPrefix(value, "`")
	value = strings.TrimSuffix(value, "`")
	value = strings.TrimPrefix(value, `"`)
	value = strings.TrimSuffix(value, `"`)
	value = strings.TrimPrefix(value, "'")
	value = strings.TrimSuffix(value, "'")
	value = strings.TrimPrefix(value, "[")
	value = strings.TrimSuffix(value, "]")
	value = strings.ReplaceAll(value, "``", "`")
	value = strings.ReplaceAll(value, `""`, `"`)
	value = strings.ReplaceAll(value, "''", "'")
	value = strings.ReplaceAll(value, "]]", "]")
	return value
}

// TokenSequenceAt reports whether the tokens from index on read as values, word
// by word.
func TokenSequenceAt(tokens []lexer.Token, index int, values ...string) bool {
	if index < 0 || index+len(values) > len(tokens) {
		return false
	}
	for offset, value := range values {
		if NormalizedIdentifier(tokens[index+offset]) != value {
			return false
		}
	}
	return true
}

// ContainsTokenSequence reports whether values appear in tokens, word by word,
// anywhere.
func ContainsTokenSequence(tokens []lexer.Token, values ...string) bool {
	for index := range len(tokens) {
		if TokenSequenceAt(tokens, index, values...) {
			return true
		}
	}
	return false
}

// FindIdentifier returns the index of the first token from start on that reads
// as value, or [MutationTargetNotFound].
func FindIdentifier(tokens []lexer.Token, value string, start int) int {
	for index := max(start, 0); index < len(tokens); index++ {
		if NormalizedIdentifier(tokens[index]) == value {
			return index
		}
	}
	return MutationTargetNotFound
}

func containsString(values []string, target string) bool {
	return slices.Contains(values, target)
}

// DescribeStatementObject names what a statement does in a refusal: its first
// word and the kind of object it acts on, such as "CREATE ROLE".
func DescribeStatementObject(tokens []lexer.Token) string {
	kindIndex := StatementObjectKindIndex(tokens)
	if kindIndex == MutationTargetNotFound {
		return NormalizedIdentifier(tokens[0])
	}
	return NormalizedIdentifier(tokens[0]) + " " + NormalizedIdentifier(tokens[kindIndex])
}

// TokenAt returns the token at index, or the zero token past either end.
func TokenAt(tokens []lexer.Token, index int) lexer.Token {
	if index < 0 || index >= len(tokens) {
		return lexer.Token{}
	}
	return tokens[index]
}
