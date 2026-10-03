package parser_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/parser"
	"ptah.run/internal/ydbgap"
)

// YDB declares its key, its indexes and its options in clauses no other
// dialect has. The parser reads none of them, so a YDB schema file is refused
// rather than read by another dialect's rules, which take `INDEX i GLOBAL ON
// (v)` for MySQL's table-level index and stop at GLOBAL.
func TestParse_RefusesYDB(t *testing.T) {
	for _, dialect := range []string{"ydb", "ydbs"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(
				"CREATE TABLE t (id Int64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX i GLOBAL ON (v));",
				parser.WithDialect(dialect),
			).Parse()

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(ydbgap.SchemaFiles.Message()))
			c.Assert(statements, qt.IsNil)
		})
	}
}
