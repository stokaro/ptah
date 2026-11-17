package atlasschema_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/migration/migrator"
)

// inputRuntime is the built-in runtime with every unit it renders reading one
// approved input. A plan of such units is rehearsed as a plan that holds a
// native operation is: made again on the dev database and executed through
// the apply's executor.
type inputRuntime struct{ *engine.Runtime }

func (r inputRuntime) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	result, err := r.Runtime.Render(ctx, request)
	if err != nil {
		return renderer.Result{}, err
	}
	result.ExecutableUnits = true
	for i := range result.Fragments {
		result.Fragments[i].Inputs = []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "PTAH_REHEARSAL_INPUT"}}
	}
	return result, nil
}

// TestNativeRehearsalPlansOnTheRebuiltDevDatabase_FailurePath rehearses a plan
// that adds a column to one of two tables the target holds. The rehearsal
// builds both tables on the dev database first, so the plan it makes there is
// the one statement the target's plan is. On an empty dev database it would
// create both tables, and the rehearsal would refuse a plan of two units
// where the target's has one. The SQLite executor runs no approved input, so
// the rehearsal stops when it executes the plan, and hands the dev database
// back empty.
func TestNativeRehearsalPlansOnTheRebuiltDevDatabase_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	conn := connectSQLite(c, filepath.Join(dir, "target.db"))
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	for _, statement := range []string{"CREATE TABLE notes (id INTEGER PRIMARY KEY)", "CREATE TABLE tags (id INTEGER PRIMARY KEY)"} {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{
		Runtime: inputRuntime{Runtime: must.Must(builtin.New())},
		TxMode:  migrator.MigrationTxModeNone,
		Desired: &schemamodel.Database{
			Tables: []schemamodel.Table{{Name: "notes", StructName: "Notes"}, {Name: "tags", StructName: "Tags"}},
			Fields: []schemamodel.Field{
				{StructName: "Notes", Name: "id", Type: "INTEGER", Primary: true},
				{StructName: "Notes", Name: "body", Type: "TEXT", Nullable: true},
				{StructName: "Tags", Name: "id", Type: "INTEGER", Primary: true},
			},
		},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasInputReferences(), qt.IsTrue)
	devPath := filepath.Join(dir, "dev.db")

	err = plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{DevURL: atlasurl.SQLiteURLFromPath(devPath)})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `dev database simulation failed during plan: unsupported feature: executor does not support approved execution inputs; `+
		`the plan was not applied to the target database`)
	observed, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables, qt.HasLen, 2)
	c.Assert(observed.Tables[0].Columns, qt.HasLen, 1)
	c.Assert(observed.Tables[1].Columns, qt.HasLen, 1)
	dev := connectSQLite(c, devPath)
	c.Cleanup(func() { c.Check(dev.Close(), qt.IsNil) })
	rehearsed, err := dev.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(rehearsed.Tables, qt.HasLen, 0)
}
