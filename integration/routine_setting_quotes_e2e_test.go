//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// quotedSettingsRoutine declares a function whose settings need quoting when
// they are written back: PostgreSQL keeps `'8MB'` as 8MB and `'5s'` as 5s,
// and `SET work_mem = 8MB` is refused with "trailing junk after numeric
// literal".
const quotedSettingsRoutine = "CREATE FUNCTION f(x int) RETURNS int LANGUAGE sql " +
	"SET work_mem = '8MB' SET statement_timeout = '5s' SET search_path = public, pg_catalog AS $$ SELECT x $$;\n"

// TestCompatMigrateDiffRollsBackQuotedRoutineSettingsE2E diffs a function
// with settings that need quoting, then a change that removes it or changes its
// body, and runs the rollback on PostgreSQL. The down file creates the function
// again with every setting quoted, the function holds its settings, and a
// comparison with the first declaration finds nothing to do: the declared
// `'8MB'` and the catalog's 8MB compare equal.
func TestCompatMigrateDiffRollsBackQuotedRoutineSettingsE2E(t *testing.T) {
	const keep = "CREATE TABLE keep (id bigint PRIMARY KEY);\n"
	tests := []struct {
		name   string
		second string
	}{
		{name: "a removed function", second: keep},
		{name: "a function whose body changes", second: keep + "CREATE FUNCTION f(x int) RETURNS int LANGUAGE sql " +
			"SET work_mem = '8MB' SET statement_timeout = '5s' SET search_path = public, pg_catalog AS $$ SELECT x + 1 $$;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
			first := keep + quotedSettingsRoutine
			pairs := readMigrationPairs(c, diffStatementStyleSteps(c, []statementStyleStep{
				{name: "s1", schema: first}, {name: "s2", schema: test.second},
			}))
			targetURL := postgresScratchDevURL(c, "ptah_setting_target")
			target, err := sql.Open("pgx", targetURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(target.Close(), qt.IsNil) })
			for _, statement := range []string{pairs[0].Up, pairs[1].Up, pairs[1].Down} {
				_, err := target.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}
			schema := filepath.Join(c.TempDir(), "s1.sql")
			c.Assert(os.WriteFile(schema, []byte(first), 0o600), qt.IsNil)

			var config string
			c.Assert(target.QueryRowContext(c.Context(),
				`SELECT array_to_string(proconfig, '|') FROM pg_proc WHERE proname = 'f'`).Scan(&config), qt.IsNil)
			diff, diffErr := runCompatVerb("schema", "diff", "--from", targetURL, "--to", "file://"+filepath.ToSlash(schema),
				"--dev-url", postgresScratchDevURL(c, "ptah_setting_dev"))

			c.Assert(config, qt.Equals, "work_mem=8MB|statement_timeout=5s|search_path=public, pg_catalog")
			c.Assert(diffErr, qt.IsNil, qt.Commentf("%s", diff))
			c.Assert(diff, qt.Contains, "Schemas are synced")
		})
	}
}
