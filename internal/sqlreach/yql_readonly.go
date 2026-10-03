package sqlreach

import (
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
// PROCESS and REDUCE, which run a UDF over every row, and the statements that
// change the query around them (PRAGMA, DECLARE, IMPORT, EXPORT, USE).
//
// The list is matched against bare words anywhere in the statement, not only
// at its start, because an action body, a lambda or a subquery puts a
// statement where no semicolon precedes it. A column whose name is one of
// these words is refused too unless it is quoted with backticks, which is the
// price of reading the text rather than parsing it.
var yqlWritingKeywords = []string{
	"INSERT", "UPSERT", "REPLACE", "UPDATE", "DELETE", "BATCH", "INTO", "RETURNING",
	"CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "ANALYZE", "BACKUP", "RESTORE",
	"COMMIT", "ROLLBACK", "DISCARD",
	"DEFINE", "DO", "EVALUATE", "PROCESS", "REDUCE",
	"PRAGMA", "DECLARE", "IMPORT", "EXPORT", "USE",
}

// YQLReadOnly proves that statement, read as YQL, is one read-only SELECT, and
// returns an error naming the first construct that stands in the way.
//
// It is default-deny: what it cannot read as part of a single SELECT is
// refused rather than allowed. The statement must start with SELECT, hold no
// second statement, and carry none of [yqlWritingKeywords]. A semicolon
// anywhere but at the end is a second statement, which also refuses a lambda
// written with a braced body; the parenthesized form `($x) -> ($x + 1)` is
// read. What a read may still reach -- a UDF module, an external source, a
// pragma, a translation setting -- is [Scan]'s question, and a caller that
// proves a statement read-only asks both.
//
// YDB itself enforces nothing here yet: a read-only YDB transaction needs the
// YDB driver, which is phase 4 of stokaro/ptah#4015. Until then this text is
// the whole proof.
func YQLReadOnly(statement string) error {
	tokens := SignificantTokens(statement, true, platform.YDB)
	if len(tokens) == 0 || !IsKeyword(tokens[0], "SELECT") {
		return fmt.Errorf("a YDB statement is proved read-only only when it is one SELECT")
	}
	for i, token := range tokens {
		if token.Type == lexer.TokenSemicolon && i != len(tokens)-1 {
			return fmt.Errorf("a YDB statement is proved read-only only when it is one SELECT, " +
				"and a semicolon inside it starts another statement or a lambda body")
		}
		if token.Type != lexer.TokenIdentifier {
			continue
		}
		if keyword := strings.ToUpper(token.Value); slices.Contains(yqlWritingKeywords, keyword) {
			return fmt.Errorf("a YDB statement is proved read-only only when it is one SELECT, "+
				"and %s starts or carries a statement that is not a read", keyword)
		}
	}
	return nil
}
