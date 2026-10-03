package sqlreach

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// yqlWritingKeywords are the YQL words that start or carry a statement other
// than a read: data changes (INSERT, UPSERT, REPLACE, UPDATE, DELETE, BATCH
// and the INTO they share with INTO RESULT), schema and access changes,
// transaction control, actions and their evaluation (DEFINE, DO, EVALUATE),
// PROCESS, REDUCE and COMBINE, which run a UDF over every row, and the
// statements that change the query around them (PRAGMA, DECLARE, IMPORT,
// EXPORT, USE).
//
// The list is matched against bare words anywhere in the statement, not only
// at its start, because an action body, a lambda or a subquery puts a
// statement where no semicolon precedes it. A column whose name is one of
// these words is refused too unless it is quoted with backticks, which is the
// price of reading the text rather than parsing it. RETURNING is not here:
// JSON_VALUE(... RETURNING Int32) is a read, and a DML RETURNING is refused
// by its INSERT, UPDATE or DELETE.
var yqlWritingKeywords = []string{
	"INSERT", "UPSERT", "REPLACE", "UPDATE", "DELETE", "BATCH", "INTO",
	"CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "ANALYZE", "BACKUP", "RESTORE",
	"COMMIT", "ROLLBACK", "DISCARD",
	"DEFINE", "DO", "EVALUATE", "PROCESS", "REDUCE", "COMBINE",
	"PRAGMA", "DECLARE", "IMPORT", "EXPORT", "USE",
}

const yqlReadOnlyRefusal = "a YDB statement is proved read-only only when it is one SELECT"

// YQLReadOnly proves that statement, read as YQL, is one read-only SELECT, and
// returns an error naming the first construct that stands in the way.
//
// It is default-deny: what it cannot read as part of a single SELECT is
// refused rather than allowed. The statement must start with SELECT, hold no
// second statement, and carry none of [yqlWritingKeywords]. A semicolon
// anywhere but at the end is a second statement, which also refuses a lambda
// written with a braced body; the parenthesized form `($x) -> ($x + 1)` is
// read. A colon is refused too: YQL writes one in a cluster prefix,
// `cluster:dir.t`, and in struct and dict literals, and refusing the literals
// is the cost of refusing the prefix. So is a number written against a word,
// as in `1uFROM`, which reads correctly only by the server's number grammar.
//
// What a read may still reach -- a UDF module, an external source, a pragma,
// a translation setting -- is [Scan]'s question, and a caller that proves a
// statement read-only asks both. The text is the whole proof: no read-only
// YDB session backs it up.
func YQLReadOnly(statement string) error {
	tokens := SignificantTokens(statement, true, platform.YDB)
	if len(tokens) == 0 || !IsKeyword(tokens[0], "SELECT") {
		return errors.New(yqlReadOnlyRefusal)
	}
	for i, token := range tokens {
		if err := yqlReadOnlyToken(tokens, i); err != nil {
			return err
		}
		if token.Type != lexer.TokenIdentifier {
			continue
		}
		if keyword := strings.ToUpper(token.Value); slices.Contains(yqlWritingKeywords, keyword) {
			return fmt.Errorf("%s, and %s starts or carries a statement that is not a read", yqlReadOnlyRefusal, keyword)
		}
	}
	return nil
}

// yqlReadOnlyToken refuses the token at i when its shape, rather than its
// word, stands in the way of the proof.
func yqlReadOnlyToken(tokens []lexer.Token, i int) error {
	token := tokens[i]
	switch {
	case token.Type == lexer.TokenSemicolon && i != len(tokens)-1:
		return fmt.Errorf("%s, and a semicolon inside it starts another statement or a lambda body", yqlReadOnlyRefusal)
	case token.MatchOperatorValue(":"):
		return fmt.Errorf("%s, and a colon inside it may name a cluster", yqlReadOnlyRefusal)
	case isYQLNumber(token) && i+1 < len(tokens) && tokens[i+1].Start == token.End &&
		(tokens[i+1].Type == lexer.TokenIdentifier || tokens[i+1].Type == lexer.TokenString):
		return fmt.Errorf("%s, and %s is written against the number %s", yqlReadOnlyRefusal, tokens[i+1].Value, token.Value)
	default:
		return nil
	}
}

// isYQLNumber reports whether token is a number, which the lexer emits as an
// identifier starting with a digit.
func isYQLNumber(token lexer.Token) bool {
	return token.Type == lexer.TokenIdentifier && token.Value != "" && token.Value[0] >= '0' && token.Value[0] <= '9'
}
