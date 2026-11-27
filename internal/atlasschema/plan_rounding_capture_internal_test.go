package atlasschema

// White-box testing required: this test reads the saved-plan capture boundary. A comparison's
// session-resolved type and the renderer's literal type enter it separately.
// SQLite cannot expose CockroachDB's integer-width setting through the public
// planning entry point. The live test covers that entry point on CockroachDB.

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin"
	"ptah.run/internal/roundingscan"
)

func TestGeneratedPlanRoundingsKeepTheRenderedType(t *testing.T) {
	rows := []struct {
		name     string
		resolved string
		declared string
	}{
		{"default eight-byte INT", "int8", "INT"},
		{"four-byte INTEGER setting", "int4", "INTEGER"},
		{"explicit integer width", "int8", "int8"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQL("cockroachdb", &ast.AlterTableNode{
				Name: "prices", Operations: []ast.AlterOperation{&ast.AlterColumnOperation{
					ColumnName: "amount", Action: ast.AlterColumnSetType, Type: row.declared,
				}},
			})
			c.Assert(err, qt.IsNil)
			changes := []roundingscan.Change{{Table: "prices", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: row.resolved}}

			recorded, err := planRoundings(changes, "cockroachdb", []string{sql})

			c.Assert(err, qt.IsNil)
			c.Assert(recorded, qt.HasLen, 1)
			c.Assert(recorded[0].DeclaredType, qt.Equals, row.declared)
			c.Assert(changes[0].DeclaredType, qt.Equals, row.resolved)
			contents, err := json.Marshal(PlanFile{
				FormatVersion: PlanFormatVersion, Dialect: "cockroachdb", FromFingerprint: "sha256:ab",
				Roundings: recorded, Statements: []PlanStatement{{SQL: sql}},
			})
			c.Assert(err, qt.IsNil)
			decoded, _, err := DecodePlanFile(contents, "generated.plan.json")
			c.Assert(err, qt.IsNil)
			c.Assert(decoded.Roundings, qt.DeepEquals, recorded)
		})
	}
}
