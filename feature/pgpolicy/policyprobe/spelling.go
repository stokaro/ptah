package policyprobe

import (
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/lexer"
)

// observedSpelling answers for a declaration no probe put through the server: a
// clause that means what the database's clause means takes the database's
// spelling, so the comparison finds nothing to change. A clause that reads
// differently keeps its own, and a comparison reports the difference. It answers
// nil when neither clause takes the server's spelling. A role keyword only a
// server can resolve makes an answer the model refuses, which leaves the
// declaration as it is.
//
// CockroachDB is where this matters. It refuses the temporary table without an
// experimental setting, and with the setting `CREATE POLICY` still fails in the
// same transaction: `only implemented in the declarative schema changer`. It
// also rewrites what it stores: measured on v26.3.5, `owner = current_user` reads
// back as `owner = current_user()`, and `n > 0 AND n < 5` as `(n > 0:::INT8) AND
// (n < 5:::INT8)`.
func observedSpelling(request schemaext.NormalizationRequest, probe probe) *pgpolicy.NormalizedPolicy {
	declared := probe.value
	held, found, err := request.Current.Objects.Get(probe.object.Ref)
	if err != nil || !found {
		return nil
	}
	observed, ok := held.Value.(*pgpolicy.ObservedPolicy)
	if !ok {
		return nil
	}
	table := probe.object.Ref.Parent
	using, withCheck := clauseSpelling(declared.Using, observed.Using, table), clauseSpelling(declared.WithCheck, observed.WithCheck, table)
	if using == declared.Using && withCheck == declared.WithCheck {
		return nil
	}
	return &pgpolicy.NormalizedPolicy{Roles: declared.EffectiveRoles(), Using: using, WithCheck: withCheck}
}

// clauseSpelling is observed when it means what declared means, and declared
// otherwise.
func clauseSpelling(declared, observed *string, table objectidentity.Part) *string {
	if declared == nil || observed == nil || canonicalClause(*declared, table) != canonicalClause(*observed, table) {
		return declared
	}
	return observed
}

// canonicalClause is a clause with what a server rewrites without changing its
// meaning taken out, so two spellings of one condition give one string and two
// conditions give two. Its AND, OR and NOT keep their structure, so `a AND (b OR
// c)` and `a AND b OR c` stay apart. It reads no more than that: where a server
// rewrites anything else, such as `n = 1 + 1` into `n = (1:::INT8 + 1:::INT8)`,
// the two stay different and the plan changes the policy, which is the safe way
// to be wrong.
func canonicalClause(clause string, table objectidentity.Part) string {
	return canonicalCondition(clauseTokens(clause, table))
}

// sqlValueKeywords maps each keyword a server stores as a call, measured on
// CockroachDB v26.3.5, to the one name its call is stored under.
var sqlValueKeywords = map[string]string{
	"current_catalog":   "current_database",
	"current_database":  "current_database",
	"current_date":      "current_date",
	"current_role":      "current_user",
	"current_schema":    "current_schema",
	"current_timestamp": "current_timestamp",
	"current_user":      "current_user",
	"localtimestamp":    "localtimestamp",
	"session_user":      "session_user",
	"user":              "current_user",
}

// clauseTokens reads a clause into comparable tokens: identifiers in lower case
// and without the quotes a plain name does not need, one spelling of not-equal,
// each SQL value keyword as its call, and no type annotation, no cast of a string
// literal to text, and no prefix naming the policy's own table.
func clauseTokens(clause string, table objectidentity.Part) []string {
	var raw []lexer.Token
	scanner := lexer.NewLexerWithOptions(clause, lexer.Options{StandardStrings: true})
	for token := scanner.NextToken(); token.Type != lexer.TokenEOF; token = scanner.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			raw = append(raw, token)
		}
	}
	tokens := make([]string, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		switch {
		case startsOperator(raw, i, ":::"):
			i = afterTypeName(raw, i+3) - 1
		case raw[i].Type == lexer.TokenString && startsOperator(raw, i+1, "::") && isTextType(raw, i+3):
			tokens = append(tokens, plainName(raw[i].Value))
			i += 3
		case startsOperator(raw, i, "<>"), startsOperator(raw, i, "!="):
			tokens = append(tokens, "!=")
			i++
		case startsOperator(raw, i+1, ".") && isTableName(raw[i], table):
			i++
		case raw[i].Type == lexer.TokenIdentifier:
			name := strings.ToLower(raw[i].Value)
			call, keyword := sqlValueKeywords[name]
			if !keyword || (i > 0 && raw[i-1].Value == ".") {
				tokens = append(tokens, name)
				continue
			}
			tokens = append(tokens, call+"()")
			if startsOperator(raw, i+1, "()") {
				i += 2
			}
		default:
			tokens = append(tokens, plainName(raw[i].Value))
		}
	}
	return tokens
}

// startsOperator reports whether the tokens at i spell operator, one character
// to a token, as the lexer reads them.
func startsOperator(tokens []lexer.Token, i int, operator string) bool {
	if i+len(operator) > len(tokens) {
		return false
	}
	for offset := range len(operator) {
		token := tokens[i+offset]
		if token.Type != lexer.TokenOperator || token.Value != operator[offset:offset+1] {
			return false
		}
	}
	return true
}

// afterTypeName is the index after the type name at i, with its modifier and its
// array brackets: INT8, VARCHAR(10), STRING[].
func afterTypeName(tokens []lexer.Token, i int) int {
	if i < len(tokens) && tokens[i].Type == lexer.TokenIdentifier {
		i++
	}
	if startsOperator(tokens, i, "(") {
		for i < len(tokens) && tokens[i].Value != ")" {
			i++
		}
		i++
	}
	for startsOperator(tokens, i, "[]") {
		i += 2
	}
	return i
}

// isTextType reports whether the token at i names the type a string literal
// already has.
func isTextType(tokens []lexer.Token, i int) bool {
	if i >= len(tokens) || tokens[i].Type != lexer.TokenIdentifier {
		return false
	}
	switch strings.ToLower(tokens[i].Value) {
	case "text", "string":
		return true
	}
	return false
}

// isTableName reports whether token names the policy's table, quoted or not.
func isTableName(token lexer.Token, table objectidentity.Part) bool {
	switch token.Type {
	case lexer.TokenIdentifier:
		return strings.ToLower(token.Value) == table.Normalized
	case lexer.TokenString:
		return strings.HasPrefix(token.Value, `"`) && strings.Trim(token.Value, `"`) == table.Source
	}
	return false
}

// plainName is a double-quoted identifier a server would read the same without
// its quotes, without them, and any other token as it is. Taking them off cannot
// make it a keyword here, because [sqlValueKeywords] stores each keyword as a
// call.
func plainName(value string) string {
	if !strings.HasPrefix(value, `"`) {
		return value
	}
	name := strings.Trim(value, `"`)
	for i, c := range name {
		letter := (c >= 'a' && c <= 'z') || c == '_'
		if !letter && (i == 0 || c < '0' || c > '9') {
			return value
		}
	}
	return name
}

// canonicalCondition writes tokens with the structure of their AND, OR and NOT
// made explicit and every parenthesis around a whole condition gone. A server
// adds them operand by operand, and they say nothing once the structure is
// explicit. Nested groups of one operator flatten, since each operator is
// associative, and operands keep their order.
func canonicalCondition(tokens []string) string {
	tokens = withoutEnclosingParentheses(tokens)
	for _, operator := range []string{"or", "and"} {
		if operands := splitOperands(tokens, operator); len(operands) > 1 {
			return groupOperands(operator, operands)
		}
	}
	if len(tokens) > 0 && tokens[0] == "not" {
		return "not{" + canonicalCondition(tokens[1:]) + "}"
	}
	return strings.Join(tokens, " ")
}

// groupOperands writes one group, taking in an operand that is the same group.
func groupOperands(operator string, operands [][]string) string {
	prefix := operator + "{"
	written := make([]string, 0, len(operands))
	for _, operand := range operands {
		condition := canonicalCondition(operand)
		if inner, nested := strings.CutPrefix(condition, prefix); nested {
			condition = strings.TrimSuffix(inner, "}")
		}
		written = append(written, condition)
	}
	return prefix + strings.Join(written, "; ") + "}"
}

// withoutEnclosingParentheses is tokens with each pair around all of them gone.
func withoutEnclosingParentheses(tokens []string) []string {
	for len(tokens) >= 2 && tokens[0] == "(" && closingParenthesis(tokens) == len(tokens)-1 {
		tokens = tokens[1 : len(tokens)-1]
	}
	return tokens
}

// closingParenthesis is the index of the parenthesis that closes the one at the
// start of tokens, or -1.
func closingParenthesis(tokens []string) int {
	depth := 0
	for i, token := range tokens {
		switch token {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// splitOperands splits tokens at each operator outside parentheses, brackets and
// CASE, and leaves the AND of a BETWEEN where it is.
func splitOperands(tokens []string, operator string) [][]string {
	var operands [][]string
	depth, start, between := 0, 0, false
	for i, token := range tokens {
		switch token {
		case "(", "[", "case":
			depth++
		case ")", "]", "end":
			depth--
		case "between":
			between = depth == 0
		case "and":
			if depth == 0 && between {
				between = false
				continue
			}
		}
		if depth == 0 && token == operator {
			operands = append(operands, tokens[start:i])
			start = i + 1
		}
	}
	return append(operands, tokens[start:])
}
