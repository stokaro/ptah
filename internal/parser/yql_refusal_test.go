package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/parser"
)

// TestParse_RefusesYQL refuses a YDB document in every spelling of the
// dialect: YQL is not the shared grammar, and the YDB owner reads it.
func TestParse_RefusesYQL(t *testing.T) {
	for _, dialect := range []string{"ydb", "ydbs"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser("CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id));", parser.WithDialect(dialect)).Parse()
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `unsupported feature: the shared SQL grammar does not read ydb documents; the owner of the target reads them`)
			c.Assert(statements, qt.IsNil)
		})
	}
}
