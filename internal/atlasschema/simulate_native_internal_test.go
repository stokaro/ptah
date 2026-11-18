package atlasschema

// White-box testing required: a plan records where it placed the names the
// desired schema writes without a schema, and only a realm-scoped pair of
// PostgreSQL URLs gives a plan a placement read from the dev database. No
// fixture provider can serve such a pair, so the native rehearsal's check of
// that placement is driven here with the naming set on the plan.

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasurl"
	"ptah.run/migration/migrator"
)

// TestSimulateNativeOnDevChecksTheDevNaming_FailurePath rehearses a plan
// whose unit reads an approved input, so it takes the native rehearsal, and
// whose names were placed in public where the dev database resolved them. The
// dev database it is rehearsed on resolves a name elsewhere, so the rehearsal
// refuses the plan before it resets the dev database, as the rehearsal of SQL
// text does.
func TestSimulateNativeOnDevChecksTheDevNaming_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	devPath := filepath.Join(dir, "dev.db")
	plan := ApplyRuntimePlan{
		runtime: must.Must(builtin.New()),
		plan: ApplyPlan{units: []renderer.Fragment{{
			SQL:    "CREATE TABLE notes (id INTEGER);",
			Inputs: []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "PTAH_REHEARSAL_INPUT"}},
		}}},
		conn:      conn,
		txMode:    migrator.MigrationTxModeNone,
		rehearsal: nativeRehearsal{planning: ApplyOptions{naming: desiredNaming{resolved: true, schema: "public", fromDev: true}}},
	}

	err = plan.SimulateOnDev(c.Context(), SimulateOptions{DevURL: atlasurl.SQLiteURLFromPath(devPath)})

	c.Assert(err, qt.ErrorMatches, `the plan places a name the desired schema writes without a schema in "public", `+
		`where the dev database resolved it, and the dev database now resolves it to ""; plan again`)
	dev, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(devPath))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(dev.Close(), qt.IsNil) })
	rehearsed, err := dev.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(rehearsed.Tables, qt.HasLen, 0)
}
