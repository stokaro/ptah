package compare

import (
	"strings"

	"ptah.run/internal/lexer"
)

// MaterializedViewQuery is a materialized view's body without a trailing
// WITH DATA or WITH NO DATA clause: the query the server stores.
//
// The clause says whether CREATE MATERIALIZED VIEW fills the view, and the
// server keeps no trace of it in the definition it reports, which is the query
// alone. Measured on PostgreSQL 15.19 and 18.6: a view declared
// `SELECT sum(amount) AS total FROM orders WITH NO DATA` reads back from
// pg_matviews as `SELECT sum(amount) AS total FROM orders;` (15.19 qualifies the
// column, `sum(orders.amount)`), with ispopulated false. Compared with the
// clause, the declaration never matches its own view, and every plan drops and
// creates the view and everything that reads it (stokaro/ptah#5006). The
// declaration keeps the clause, so a view the plan creates is created as
// declared.
//
// The clause is recognized as the last words of the code, so a comment or a
// semicolon after it does not hide it, and the words inside a string, a comment
// or a quoted name are not the clause. A body without the clause is returned
// unchanged.
func MaterializedViewQuery(body string) string {
	var words []lexer.Token
	l := lexer.NewLexer(body)
	for token := l.NextToken(); token.Type != lexer.TokenEOF; token = l.NextToken() {
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenSemicolon:
			continue
		}
		words = append(words, token)
	}
	if cut, ok := dataClauseStart(words); ok {
		return strings.TrimSpace(body[:cut])
	}
	return body
}

// dataClauseStart is the offset where a trailing WITH [NO] DATA starts in the
// body words were read from.
func dataClauseStart(words []lexer.Token) (int, bool) {
	is := func(fromEnd int, word string) bool {
		index := len(words) - fromEnd
		return index >= 0 && words[index].Type == lexer.TokenIdentifier && strings.EqualFold(words[index].Value, word)
	}
	switch {
	case is(1, "DATA") && is(2, "WITH"):
		return words[len(words)-2].Start, true
	case is(1, "DATA") && is(2, "NO") && is(3, "WITH"):
		return words[len(words)-3].Start, true
	}
	return 0, false
}
