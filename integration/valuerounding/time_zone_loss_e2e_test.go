//go:build integration

package valuerounding_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	zoneLossRefusal = "refusing a type change that removes a time zone or offset"
	zonedTable      = "CREATE TABLE events (id int PRIMARY KEY, at timetz(6));\n"
	zoneFreeTable   = "CREATE TABLE events (id int PRIMARY KEY, at time(6));\n"
	zonedRow        = "INSERT INTO events VALUES (1, '10:00:00+02');"
	zonedRead       = "SELECT at::text FROM events WHERE id = 1"
)

// Equal fractional digits do not make removing the zone lossless.
func TestSchemaApplyRefusesTimeZoneLossWithoutFractionsE2E(t *testing.T) {
	for _, engine := range zoneEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, append(append([]string{engine.table}, engine.setup...), engine.whole)...)
			held := read(c, target, engine.readBack)

			applied := ptah(c, "schema", "apply", "--db-url", target, "--schema-file", schemaFile(c, engine.desired), "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 2, outcome(applied))
			c.Assert(applied.Stderr, qt.Contains, zoneLossRefusal)
			c.Assert(applied.Stderr, qt.Not(qt.Contains), "with more digits")
			c.Assert(read(c, target, engine.readBack), qt.Equals, held)
		})
	}
}

func TestSchemaApplyTimeZoneLossAllowsEmptyAndNullColumnsPostgresE2E(t *testing.T) {
	tests := []struct {
		name string
		rows []string
	}{
		{"empty", nil},
		{"null", []string{"INSERT INTO events VALUES (1, NULL)"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := postgresServer.scratch(c, append([]string{zonedTable}, test.rows...)...)
			applied := ptah(c, "schema", "apply", "--db-url", target, "--schema-file", schemaFile(c, zoneFreeTable), "--auto-approve")
			c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
			c.Assert(read(c, target, "SELECT count(*)::text FROM events WHERE at IS NOT NULL"), qt.Equals, "0")
		})
	}
}

// The saved plan must retain the guard while its column holds NULL, then
// re-read a value updated without changing the schema or the row count.
func TestSavedPlanRefusesTimeZoneLossAfterPlanningPostgresE2E(t *testing.T) {
	c := qt.New(t)
	target := postgresServer.scratch(c, zonedTable, "INSERT INTO events VALUES (1, NULL)")
	plan := filepath.Join(c.TempDir(), "zone.plan.json")
	planned := ptah(c, "schema", "plan", "--db-url", target, "--schema-file", schemaFile(c, zoneFreeTable), "--save", "--output", plan)
	c.Assert(planned.ExitCode, qt.Equals, 0, outcome(planned))
	run(c, target, "UPDATE events SET at = '10:00:00+02' WHERE id = 1")

	applied := ptah(c, "schema", "apply", "--db-url", target, "--plan", plan, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 2, outcome(applied))
	c.Assert(applied.Stderr, qt.Contains, zoneLossRefusal)
	c.Assert(read(c, target, zonedRead), qt.Equals, "10:00:00+02")
}

// Each executor reads the file's time-zone check against rows that were not
// present when generation ran. Explicit permission is required to lose them.
func TestGeneratedFileRefusesTimeZoneLossAfterGenerationPostgresE2E(t *testing.T) {
	for _, surface := range rollbackSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			target := postgresServer.scratch(c, zonedTable)
			dir, body := generated(c, roundingEngine{desired: zoneFreeTable}, target)
			c.Assert(body, qt.Contains, "-- +ptah check kind=time-zone-loss ")
			hashRollbackDirectory(c, dir)
			run(c, target, zonedRow)

			refused := surface.run(c, target, dir, "", surface.apply, nil)

			c.Assert(refused.ExitCode, qt.Equals, surface.failure, outcome(refused))
			c.Assert(refused.Stderr, qt.Contains, "the migration removes a time zone or offset")
			c.Assert(refused.Stderr, qt.Not(qt.Contains), "rerun with --skip-checks")
			c.Assert(read(c, target, zonedRead), qt.Equals, "10:00:00+02")

			allowed := surface.run(c, target, dir, "", append(surface.apply, surface.allow...), surface.env)

			c.Assert(allowed.ExitCode, qt.Equals, 0, outcome(allowed))
			c.Assert(allowed.Stderr, qt.Contains, "removes a time zone or offset without a scan")
			c.Assert(read(c, target, zonedRead), qt.Equals, "10:00:00")
		})
	}
}

// Adding a zone is allowed. The derived down file has to protect offsets in
// rows inserted after that up file has run.
func TestGeneratedRollbackRefusesTimeZoneLossPostgresE2E(t *testing.T) {
	for _, surface := range rollbackSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			target := postgresServer.scratch(c, zoneFreeTable)
			dir, _ := generated(c, roundingEngine{desired: zonedTable}, target)
			hashRollbackDirectory(c, dir)
			applied := surface.run(c, target, dir, "", surface.apply, nil)
			c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
			run(c, target, zonedRow)

			refused := surface.run(c, target, dir, "", surface.down, nil)

			c.Assert(refused.ExitCode, qt.Equals, surface.failure, outcome(refused))
			c.Assert(refused.Stderr, qt.Contains, "the migration removes a time zone or offset")
			c.Assert(read(c, target, zonedRead), qt.Equals, "10:00:00+02")
		})
	}
}
