package atlasschema_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/planner"
)

// TestCaptureApplyPlan_KeepsANoteNoStatementCarries_HappyPath pins where a
// planner's comment-only output goes when it renders as SQL. After a statement
// it rides on that statement. With no statement before it, it is the only
// thing the plan says about a difference it plans nothing for, so it becomes
// an annotation: dropped, the plan reads as empty and apply reports the
// schema synced (stokaro/ptah#6160).
func TestCaptureApplyPlan_KeepsANoteNoStatementCarries_HappyPath(t *testing.T) {
	const warning = "WARNING: Generated column tasks.code changed; manual migration required."
	tests := []struct {
		name      string
		fragments []string
		wantUnits []string
		wantNotes []renderer.Annotation
	}{
		{
			name:      "a note with no statement before it",
			fragments: []string{"-- " + warning + "\n"},
			wantNotes: []renderer.Annotation{{Before: 0, Text: warning}},
		},
		{
			name:      "a note that follows a statement",
			fragments: []string{"CREATE TABLE tasks (id INTEGER);\n", "-- " + warning + "\n"},
			wantUnits: []string{"CREATE TABLE tasks (id INTEGER)\n-- " + warning},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectSQLite(c, filepath.Join(c.TempDir(), "target.db"))
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			fragments := make([]renderer.Fragment, len(test.fragments))
			nodes := make([]ast.Node, len(test.fragments))
			for i, sql := range test.fragments {
				fragments[i] = renderer.Fragment{SQL: sql}
				nodes[i] = ast.NewRawSQL(sql)
			}
			rendered := planner.RenderedPlan{
				Request: renderer.Request{Target: "sqlite", Capabilities: conn.Info().Capabilities, Nodes: nodes},
				Result:  renderer.Result{Complete: true, Fragments: fragments},
				Dialect: "sqlite",
			}

			plan, err := atlasschema.CaptureApplyPlan(conn.Info(), rendered)

			c.Assert(err, qt.IsNil)
			units, err := plan.Units()
			c.Assert(err, qt.IsNil)
			var sql []string
			for _, unit := range units {
				sql = append(sql, unit.SQL)
			}
			c.Assert(sql, qt.DeepEquals, test.wantUnits)
			c.Assert(plan.HasChanges(), qt.Equals, len(test.wantUnits) > 0)
			c.Assert(plan.Annotations(), qt.DeepEquals, test.wantNotes)
		})
	}
}
