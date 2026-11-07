//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// writeEnumRebuildSchema writes the declaration that makes the plan rebuild the enum
// and returns its URL.
func writeEnumRebuildSchema(c *qt.C, desired string) string {
	c.Helper()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	return "file://" + filepath.ToSlash(schema)
}

// TestCompatPlanningVerbsE2E_AnEnumRebuildLeavesTheOtherDropsWithoutAGuard holds the
// verbs of the compatibility surface that plan against a database to what the native
// verbs write; see
// TestNativePlanningVerbsE2E_AnEnumRebuildLeavesTheOtherDropsWithoutAGuard. Ptah has
// one statement style for both surfaces, and the rebuild is Ptah's own statement.
func TestCompatPlanningVerbsE2E_AnEnumRebuildLeavesTheOtherDropsWithoutAGuard(t *testing.T) {
	tests := []struct {
		name string
		args func(target, schema, dev string) []string
	}{
		{name: "schema diff", args: func(target, schema, dev string) []string {
			return []string{"schema", "diff", "--from", target, "--to", schema, "--dev-url", dev}
		}},
		{name: "schema apply --dry-run", args: func(target, schema, dev string) []string {
			return []string{"schema", "apply", "--url", target, "--to", schema, "--dev-url", dev, "--dry-run"}
		}},
		{name: "schema plan", args: func(target, schema, dev string) []string {
			return []string{"schema", "plan", "--from", target, "--to", schema, "--dev-url", dev, "--name", "p", "--dry-run"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, enumRebuildBuilt)
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb(test.args(target, writeEnumRebuildSchema(c, enumRebuildDesired), dev)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, `ALTER TYPE "e" RENAME TO "e__ptah_old";`)
			c.Assert(out, qt.Contains, `DROP VIEW "v";`)
			c.Assert(out, qt.Contains, `DROP INDEX "i";`)
			c.Assert(out, qt.Contains, `ALTER TABLE "t" DROP CONSTRAINT "t_a_check";`)
			c.Assert(out, qt.Not(qt.Contains), "IF EXISTS")
		})
	}
}

// TestCompatMigrateDiffE2E_AnEnumRebuildLeavesTheOtherDropsWithoutAGuard is the row
// above for the verb that plans from a directory. A format with no rollback file
// plans one direction, and a format with one plans both through another call.
func TestCompatMigrateDiffE2E_AnEnumRebuildLeavesTheOtherDropsWithoutAGuard(t *testing.T) {
	tests := []struct {
		name string
		// dirQuery selects the directory's format.
		dirQuery string
		// initial is the directory's one file.
		initial string
		built   string
		desired string
		// reads is what the assertions read, as a glob under the directory.
		reads string
		want  []string
	}{
		{
			name:    "a format with no rollback file",
			initial: "20200101000000_init.sql",
			built:   enumRebuildBuilt,
			desired: enumRebuildDesired,
			reads:   "*_change.sql",
			want: []string{
				`DROP VIEW "v";`,
				`DROP INDEX "i";`,
				`ALTER TABLE "t" DROP CONSTRAINT "t_a_check";`,
			},
		},
		{
			name:     "the forward file of a format with a rollback file",
			dirQuery: "?format=flyway",
			initial:  "V20200101000000__init.sql",
			built:    enumRebuildBuilt,
			desired:  enumRebuildDesired,
			reads:    "V*__change.sql",
			want: []string{
				`DROP VIEW "v";`,
				`DROP INDEX "i";`,
				`ALTER TABLE "t" DROP CONSTRAINT "t_a_check";`,
			},
		},
		{
			name:     "the rollback file of a format with a rollback file",
			dirQuery: "?format=golang-migrate",
			initial:  "20200101000000_init.up.sql",
			built:    enumValueAddedBuilt,
			desired:  enumValueAddedDesired,
			reads:    "*.down.sql",
			want:     []string{`DROP INDEX "i";`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, test.initial), []byte(test.built), 0o600), qt.IsNil)
			dirURL := "file://" + filepath.ToSlash(dir) + test.dirQuery
			hashed, err := runCompatVerb("migrate", "hash", "--dir", dirURL)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", hashed))
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb("migrate", "diff", "change", "--dir", dirURL,
				"--to", writeEnumRebuildSchema(c, test.desired), "--dev-url", dev)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			written := filesMatching(c, dir, test.reads)
			c.Assert(written, qt.Contains, `ALTER TYPE "e" RENAME TO "e__ptah_old";`, qt.Commentf("%s", out))
			for _, want := range test.want {
				c.Assert(written, qt.Contains, want)
			}
			c.Assert(written, qt.Not(qt.Contains), "IF EXISTS")
		})
	}
}

// TestCompatMigrateDownE2E_ADerivedRollbackThatRebuildsAnEnumLeavesItsDropWithoutAGuard
// is the row above for the verb that forwards to the native command; see
// TestMigrationsDownE2E_ADerivedRollbackThatRebuildsAnEnumLeavesItsDropWithoutAGuard.
func TestCompatMigrateDownE2E_ADerivedRollbackThatRebuildsAnEnumLeavesItsDropWithoutAGuard(t *testing.T) {
	c := qt.New(t)
	dir, target, dev := derivedRollbackDir(c)

	out, err := runCompatVerb("migrate", "down", "--url", target, "--dir", "file://"+filepath.ToSlash(dir),
		"--dev-url", dev, "--to-version", "20200101000000", "--plan", "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `ALTER TYPE "e" RENAME TO "e__ptah_old"`)
	c.Assert(out, qt.Contains, `DROP INDEX "i"`)
	c.Assert(out, qt.Not(qt.Contains), "IF EXISTS")
}

// TestCompatSchemaApplyE2E_AnEnumRebuildRunsWithTheOtherDrops runs that plan on the
// server, and a second plan finding nothing proves it converged. Nothing ahead of a
// drop took its object, or the drop without a guard would stop the plan.
func TestCompatSchemaApplyE2E_AnEnumRebuildRunsWithTheOtherDrops(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, enumRebuildBuilt)
	dev, _ := scratchReplayDatabase(c)
	schema := writeEnumRebuildSchema(c, enumRebuildDesired)

	out, err := runCompatVerb("schema", "apply", "--url", target, "--to", schema, "--dev-url", dev, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	for query, want := range enumRebuildReadBack {
		c.Assert(readInt(c, target, query), qt.Equals, want, qt.Commentf("%s", query))
	}
	again, err := runCompatVerb("schema", "diff", "--from", target, "--to", schema, "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", again))
	c.Assert(again, qt.Not(qt.Contains), "ALTER")
	c.Assert(again, qt.Not(qt.Contains), "DROP")
}

// TestCompatMigrateApplyE2E_AFileWithAnEnumRebuildStopsOnAPartlyAppliedDatabase is what
// a guard behind a rebuild would hide. The index is gone by the time the file `migrate
// diff` wrote runs. `migrate apply` stops at its drop, and the file's one transaction
// takes the rebuild back, so the enum keeps the value.
func TestCompatMigrateApplyE2E_AFileWithAnEnumRebuildStopsOnAPartlyAppliedDatabase(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20200101000000_init.sql"), []byte(enumRebuildBuilt), 0o600), qt.IsNil)
	dirURL := "file://" + filepath.ToSlash(dir)
	hashed, err := runCompatVerb("migrate", "hash", "--dir", dirURL)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", hashed))
	target, _ := scratchReplayDatabase(c)
	dev, _ := scratchReplayDatabase(c)
	applied, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", dirURL)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	written, err := runCompatVerb("migrate", "diff", "change", "--dir", dirURL,
		"--to", writeEnumRebuildSchema(c, enumRebuildDesired), "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", written))
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "DROP INDEX i")
	c.Assert(err, qt.IsNil)

	out, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", dirURL)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `index "i" does not exist`)
	c.Assert(readInt(c, target, "SELECT count(*) FROM pg_enum WHERE enumlabel = 'y'"), qt.Equals, 1)
	c.Assert(readInt(c, target, "SELECT count(*) FROM pg_type WHERE typname = 'e__ptah_old'"), qt.Equals, 0)
}
