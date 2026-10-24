package datadiff_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/core/platform/capability"
	"ptah.run/migration/datadiff"
)

// spelledWrites records the writes it is asked for and answers each with a
// statement naming its position, as an owner that spells its writes does.
type spelledWrites struct {
	writes []dataext.RowWrite
	caps   capability.Capabilities
	answer func(count int) ([]string, error)
}

func (s *spelledWrites) RenderWrites(_ context.Context, caps capability.Capabilities, writes []dataext.RowWrite) ([]string, error) {
	s.writes, s.caps = writes, caps
	return s.answer(len(writes))
}

func numbered(count int) ([]string, error) {
	statements := make([]string, count)
	for i := range statements {
		statements[i] = fmt.Sprintf("W%d;", i)
	}
	return statements, nil
}

func ownedDiff() *datadiff.DataDiff {
	return &datadiff.DataDiff{
		Schema: "app", Table: "regions", Keys: []string{"code"},
		ColumnTypes: map[string]string{"code": "TEXT", "name": "TEXT", "other": "BLOB"},
		Inserts:     []datadiff.Row{{"code": "A", "name": "Alpha"}, {"code": "D", "name": "Delta"}},
		Updates: []datadiff.RowUpdate{{Key: map[string]any{"code": "B"},
			Desired: datadiff.Row{"code": "B", "name": "Beta"}, Live: datadiff.Row{"code": "B", "name": "beta"}}},
		Deletes: []datadiff.Row{{"code": "C", "name": nil}, {"code": "E", "name": "Echo"}},
	}
}

// An owner that spells its writes gets the up writes and then the down writes
// in one batch, in the order the statements of RenderStatements follow, and
// its statements come back split the same way.
func TestRenderStatementsContext_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &spelledWrites{answer: numbered}
	caps := capability.Of(map[capability.Capability]bool{capability.ReturningClause: true})

	up, down, err := datadiff.RenderStatementsContext(c.Context(), ownedDiff(), "custom", dataext.Services{Writes: owner}, caps)

	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.DeepEquals, []string{"W0;", "W1;", "W2;", "W3;", "W4;"})
	c.Assert(down, qt.DeepEquals, []string{"W5;", "W6;", "W7;", "W8;", "W9;"})
	c.Assert(owner.caps.Equal(caps), qt.IsTrue)
	types := map[string]string{"code": "TEXT", "name": "TEXT"}
	keyTypes := map[string]string{"code": "TEXT"}
	write := func(mode dataext.WriteMode, columns []string, kinds map[string]string, values ...any) dataext.RowWrite {
		return dataext.RowWrite{Mode: mode, Schema: "app", Table: "regions", Columns: columns, Keys: []string{"code"}, Values: values, ColumnTypes: kinds}
	}
	both, key := []string{"code", "name"}, []string{"code"}
	c.Assert(owner.writes, qt.DeepEquals, []dataext.RowWrite{
		write(dataext.WriteInsert, both, types, "A", "Alpha"),
		write(dataext.WriteInsert, both, types, "D", "Delta"),
		write(dataext.WriteUpdateByKey, both, types, "B", "Beta"),
		write(dataext.WriteDeleteByKey, key, keyTypes, "C"),
		write(dataext.WriteDeleteByKey, key, keyTypes, "E"),
		write(dataext.WriteInsert, both, types, "E", "Echo"),
		write(dataext.WriteInsert, both, types, "C", nil),
		write(dataext.WriteUpdateByKey, both, types, "B", "beta"),
		write(dataext.WriteDeleteByKey, key, keyTypes, "D"),
		write(dataext.WriteDeleteByKey, key, keyTypes, "A"),
	})
}

// Without a write service the context entry is RenderStatements, and an empty
// diff asks the owner nothing.
func TestRenderStatementsContext_HappyPath_WithoutOwnerOrChanges(t *testing.T) {
	c := qt.New(t)
	diff := &datadiff.DataDiff{Table: "regions", Keys: []string{"code"}, Inserts: []datadiff.Row{{"code": "A"}}}
	up, down, err := datadiff.RenderStatementsContext(c.Context(), diff, "postgres", dataext.Services{}, capability.Capabilities{})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.DeepEquals, []string{`INSERT INTO "regions" ("code") VALUES ('A');`})
	c.Assert(down, qt.DeepEquals, []string{`DELETE FROM "regions" WHERE "code" = 'A';`})

	owner := &spelledWrites{answer: numbered}
	up, down, err = datadiff.RenderStatementsContext(c.Context(), &datadiff.DataDiff{Table: "regions", Keys: []string{"code"}},
		"custom", dataext.Services{Writes: owner}, capability.Capabilities{})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.IsNil)
	c.Assert(down, qt.IsNil)
	c.Assert(owner.writes, qt.IsNil)
}

func TestRenderStatementsContext_FailurePath(t *testing.T) {
	refused := errors.New("the owner refuses the batch")
	tests := []struct {
		name    string
		answer  func(int) ([]string, error)
		diff    *datadiff.DataDiff
		wantErr string
	}{
		{name: "the owner refuses", answer: func(int) ([]string, error) { return nil, refused }, diff: ownedDiff(),
			wantErr: `datadiff: render the row writes of regions: the owner refuses the batch`},
		{name: "a statement short", answer: func(count int) ([]string, error) { return numbered(count - 1) }, diff: ownedDiff(),
			wantErr: `datadiff: the owner rendered 9 statements for 10 row writes`},
		{name: "an update with nothing to set", answer: numbered, diff: &datadiff.DataDiff{Table: "regions", Keys: []string{"code"},
			Updates: []datadiff.RowUpdate{{Key: map[string]any{"code": "B"}, Desired: datadiff.Row{"code": "B"}, Live: datadiff.Row{"code": "B"}}}},
			wantErr: `datadiff: cannot render UPDATE with no non-key columns to set`},
		{name: "a delete without its key", answer: numbered, diff: &datadiff.DataDiff{Table: "regions", Keys: []string{"code"},
			Deletes: []datadiff.Row{{"name": "x"}}},
			wantErr: `datadiff: missing key column "code"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			up, down, err := datadiff.RenderStatementsContext(c.Context(), test.diff, "custom",
				dataext.Services{Writes: &spelledWrites{answer: test.answer}}, capability.Capabilities{})
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(up, qt.IsNil)
			c.Assert(down, qt.IsNil)
		})
	}
}

// The offline entries cannot reach an owner that spells its writes, so they
// refuse its diff rather than write the host's own statement text.
func TestRenderStatements_FailurePath_OwnerSpellsWrites(t *testing.T) {
	c := qt.New(t)
	owner := &spelledWrites{answer: numbered}
	up, down, err := datadiff.RenderStatements(ownedDiff(), "custom", dataext.Services{Writes: owner})
	c.Assert(err, qt.ErrorMatches, `datadiff: the owner of custom renders its row writes; render through RenderStatementsContext`)
	c.Assert(up, qt.IsNil)
	c.Assert(down, qt.IsNil)
	c.Assert(owner.writes, qt.IsNil)
}
