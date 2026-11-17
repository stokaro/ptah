package postgres

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/rolepassword"
)

// A declared role password reaches a PostgreSQL-family server only in the
// statement this writer sends. The renderer writes `PASSWORD
// $PTAH_SECRET_PASSWORD_<ROLE>` where the password stands (see
// [rolepassword.Reference]), which PostgreSQL refuses as written (`syntax
// error at or near "$"`), so a migration file run without Ptah sets no other
// password. The writer replaces the reference with the value as a string
// literal right before it sends the statement, reading the value through
// [rolepassword.Lookup], and takes the value out of any error the server
// answers with. PostgreSQL takes a password hash in the same clause, so the
// value is written as it is.

// errPasswordReference is the error a statement is refused with when it names
// a password variable anywhere but where CREATE ROLE or ALTER ROLE takes a
// password: the value would reach something other than a password.
var errPasswordReference = errors.New("a password variable is referred to outside the password of CREATE ROLE or ALTER ROLE")

// passwordExpansion is a statement with the passwords it names written in,
// and the values it wrote, so an error can be stripped of them.
type passwordExpansion struct {
	text   string
	values []string
}

// redact returns err with every value the expansion wrote taken out of its
// text; errors.Is and errors.As still reach err.
func (e passwordExpansion) redact(err error) error {
	if err == nil || len(e.values) == 0 {
		return err
	}
	return redactedPasswordError{err: err, text: redactValues(err.Error(), e.values), values: e.values}
}

// redactValues replaces every value in text, longest first as the expansion
// sorts them, so a value that holds a shorter one is taken out whole.
func redactValues(text string, values []string) string {
	for _, value := range values {
		if value != "" {
			text = strings.ReplaceAll(text, value, "[secret]")
		}
	}
	return text
}

// redactedPasswordError is the failure of a statement this writer wrote a
// password into. Its message has the password taken out; the driver error it
// wraps does not, so it also takes the password out of the server's own fields
// for a reader that reads them (see [ptah.run/internal/serverdiag.Redactor]).
type redactedPasswordError struct {
	err    error
	text   string
	values []string
}

func (e redactedPasswordError) Error() string { return e.text }
func (e redactedPasswordError) Unwrap() error { return e.err }

// RedactServerText takes every password the statement was sent with out of a
// text the server answered with.
func (e redactedPasswordError) RedactServerText(text string) string {
	return redactValues(text, e.values)
}

// expandPasswords writes, in statement, the value of every password variable
// it names after PASSWORD in a CREATE or ALTER of a ROLE, USER or GROUP, read
// through lookup, and returns statement unchanged when it names none. A
// reference anywhere else, one outside the PTAH_SECRET_ prefix's exact
// spelling, and a variable lookup does not find refuse the statement, naming
// the variable and never a value.
func expandPasswords(statement string, lookup func(string) (string, bool)) (passwordExpansion, error) {
	if !strings.Contains(strings.ToUpper(statement), rolepassword.ReferencePrefix) {
		return passwordExpansion{text: statement}, nil
	}
	tokens := significantTokens(statement)
	positions := passwordPositions(tokens)
	var b strings.Builder
	var values []string
	last := 0
	for i, token := range tokens {
		variable, isReference := rolepassword.ReferenceVariable(token.Value)
		if token.Type != lexer.TokenIdentifier || !isReference {
			continue
		}
		if !positions[i] {
			return passwordExpansion{}, fmt.Errorf("%w: %s at offset %d; Ptah writes a password only there",
				errPasswordReference, token.Value, token.Start)
		}
		if !strings.HasPrefix(variable, strings.TrimPrefix(rolepassword.ReferencePrefix, "$")) {
			return passwordExpansion{}, fmt.Errorf("%w: %s is not spelled with the PTAH_SECRET_ prefix in upper case",
				errPasswordReference, token.Value)
		}
		value, set := lookup(variable)
		if !set {
			return passwordExpansion{}, fmt.Errorf("a role's password comes from environment variable %s, which is not set",
				variable)
		}
		literal := passwordLiteral(value)
		quoted := strconv.Quote(value)
		values = append(values, value, literal[2:len(literal)-1], quoted[1:len(quoted)-1])
		b.WriteString(statement[last:token.Start])
		b.WriteString(literal)
		last = token.End
	}
	if len(values) == 0 {
		return passwordExpansion{text: statement}, nil
	}
	b.WriteString(statement[last:])
	slices.SortFunc(values, func(a, b string) int { return len(b) - len(a) })
	return passwordExpansion{text: b.String(), values: values}, nil
}

// passwordLiteral writes value as an escape string literal, E'...', whose
// backslashes and quotes PostgreSQL reads the same way whatever
// standard_conforming_strings is set to.
func passwordLiteral(value string) string {
	return "E'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
}

// significantTokens reads statement as PostgreSQL, leaving out whitespace and
// comments.
func significantTokens(statement string) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.Postgres))
	var tokens []lexer.Token
	for {
		token := lexr.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		}
		tokens = append(tokens, token)
	}
}

// passwordPositions reports the tokens that stand right after PASSWORD in a
// statement that starts CREATE or ALTER, then ROLE, USER or GROUP. A statement
// is the tokens between two semicolons outside parentheses.
func passwordPositions(tokens []lexer.Token) map[int]bool {
	positions := make(map[int]bool)
	word := func(token lexer.Token, words ...string) bool {
		return token.Type == lexer.TokenIdentifier && slices.ContainsFunc(words, func(w string) bool { return strings.EqualFold(token.Value, w) })
	}
	start, depth := 0, 0
	for i := 0; i <= len(tokens); i++ {
		end := i == len(tokens)
		if !end {
			switch {
			case tokens[i].MatchOperatorValue("("):
				depth++
			case tokens[i].MatchOperatorValue(")"):
				depth--
			}
		}
		if !end && (tokens[i].Type != lexer.TokenSemicolon || depth != 0) {
			continue
		}
		statement := tokens[start:i]
		if len(statement) > 2 && word(statement[0], "CREATE", "ALTER") && word(statement[1], "ROLE", "USER", "GROUP") {
			for k := 2; k+1 < len(statement); k++ {
				if word(statement[k], "PASSWORD") {
					positions[start+k+1] = true
				}
			}
		}
		start = i + 1
	}
	return positions
}
