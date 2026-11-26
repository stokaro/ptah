package chkey

import (
	"fmt"
	"strings"

	"ptah.run/core/ptaherr"
)

// QuotedNot returns the first name in expression that ClickHouse reads as a
// name spelled not, in any case, and reports whether there is one: a quoted
// name, since a bare not is the operator, that no opening parenthesis follows,
// since `not`(x) is the function.
//
// Releases before 26.11 cannot keep such a name. Measured on ClickHouse
// 24.10 and 26.10, the server writes it unquoted, as the bare word not,
// into the definition it stores, so SHOW CREATE TABLE and every read of the
// schema then fail with code 62. That holds for the column alone and nested in
// a tuple or a call, in ORDER BY, PRIMARY KEY, PARTITION BY, SAMPLE BY and
// TTL, in a skipping index, a CHECK, a MATERIALIZED or ALIAS column, and in the
// body of a view or a materialized view, where a table alias spelled not
// fails the same way, and in the arguments of a table engine; 25.8, 26.3,
// 26.7, 26.8 and 26.9 store a sorting key and a view body the same way. The
// filter of a row policy is written back the same way, and the server refuses
// its own text, so CREATE ROW POLICY fails with code 62. `not` + 1 reads back as NOT 1, another expression, in
// ORDER BY, in a DEFAULT and in a view body, which is what a view that
// aliases a column as not and then names the alias stores. ClickHouse 26.11
// keeps the quotes and reads every one of these back, but a renderer does not
// know the release it writes for. Other words the server reads as operators,
// such as and, or, in and like, read back on every release measured.
func QuotedNot(expression string) (string, bool) {
	tokens := significantTokens(expression)
	for i, token := range tokens {
		if !isIdentifier(token) || !strings.HasPrefix(token.Value, "`") && !strings.HasPrefix(token.Value, `"`) {
			continue
		}
		if i+1 < len(tokens) && tokens[i+1].MatchOperatorValue("(") {
			continue
		}
		if strings.EqualFold(unquote(token.Value), "not") {
			return token.Value, true
		}
	}
	return "", false
}

// RefuseQuotedNot refuses expression, the text written in subject, such as
// "the ORDER BY of table events", when it names not; see [QuotedNot]. The
// error wraps [ptaherr.ErrUnsupportedFeature] and names the name as it is
// written.
func RefuseQuotedNot(subject, expression string) error {
	name, found := QuotedNot(expression)
	if !found {
		return nil
	}
	return fmt.Errorf("%w: ClickHouse: %s names %s; releases before 26.11 store that name as the bare word not, "+
		"the NOT operator, so the statement or every later read of the schema fails, or the definition means another expression; "+
		"rename it", ptaherr.ErrUnsupportedFeature, subject, name)
}
