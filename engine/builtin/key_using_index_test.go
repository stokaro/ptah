package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/parser"
)

// TestRenderSQL_AKeyUsingIndex_FailurePath pins that a parse of PostgreSQL's
// `ADD CONSTRAINT ... UNIQUE USING INDEX` does not render. The key's columns
// are the index's, which the node does not carry, and rendered as it stands
// the key would be written over no columns. Every target refuses it, the
// PostgreSQL family included.
func TestRenderSQL_AKeyUsingIndex_FailurePath(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.CockroachDB, platform.MySQL} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser("ALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a;",
				parser.WithDialect(platform.Postgres)).Parse()
			c.Assert(err, qt.IsNil)

			sql, err := builtin.RenderSQL(dialect, statements.Statements[0])

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches,
				`.*a UNIQUE constraint made of the existing index t_a lists no columns; render the key from a schema that holds it`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
