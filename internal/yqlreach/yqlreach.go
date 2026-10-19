// Package yqlreach proves from the text of a YQL query what it reads and that
// it writes nothing: the read-only proof the YDB migrator's checks ask of an
// assertion, and the names a YDB reader asks its catalog about before a
// query runs. What a query may reach outside the database, such as a UDF
// module or an external source, is internal/sqlreach's question, and a caller
// asks both.
//
// It serves YDB alone, so it reads every text as YQL.
package yqlreach

import (
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlreach"
)

// yqlTokens are the significant tokens of a YQL text, read by the YQL lexer.
// The lexer ignores the escape mode in YQL, where a backslash always escapes.
func yqlTokens(text string) []lexer.Token {
	return sqlreach.SignificantTokens(text, true, platform.YDB)
}
