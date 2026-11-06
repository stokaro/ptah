//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestCompatVerbsE2E_KeepTheirGuardsAfterAnEnumRebuild holds every verb of the
// compatibility surface that plans against a database to the statements it writes
// behind an enum rebuild, where the native verbs write none; see
// TestNativePlanningVerbsE2E_AnEnumRebuildLeavesTheOtherDropsWithoutAGuard. The
// choice is [ptah.run/core/planext.Options.EnumRebuildKeepsGuards], so each row
// reddens when its verb's planning call leaves it out.
func TestCompatVerbsE2E_KeepTheirGuardsAfterAnEnumRebuild(t *testing.T) {
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
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(enumRebuildDesired), 0o600), qt.IsNil)

			out, err := runCompatVerb(test.args(target, "file://"+filepath.ToSlash(schema), dev)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, `ALTER TYPE "e" RENAME TO "e__ptah_old";`)
			c.Assert(out, qt.Contains, `DROP VIEW IF EXISTS "v";`)
			c.Assert(out, qt.Contains, `DROP INDEX IF EXISTS "i";`)
			c.Assert(out, qt.Contains, `ALTER TABLE "t" DROP CONSTRAINT IF EXISTS "t_a_check";`)
		})
	}
}

// TestCompatMigrateDiffE2E_KeepsItsGuardsAfterAnEnumRebuild is the row above for the
// verb that plans from a directory. A format with no rollback file plans one
// direction, and a format with one plans both through another call.
func TestCompatMigrateDiffE2E_KeepsItsGuardsAfterAnEnumRebuild(t *testing.T) {
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
			name:    "a format with no down file",
			initial: "20200101000000_init.sql",
			built:   enumRebuildBuilt,
			desired: enumRebuildDesired,
			reads:   "*_change.sql",
			want: []string{
				`DROP VIEW IF EXISTS "v";`,
				`DROP INDEX IF EXISTS "i";`,
				`ALTER TABLE "t" DROP CONSTRAINT IF EXISTS "t_a_check";`,
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
				`DROP VIEW IF EXISTS "v";`,
				`DROP INDEX IF EXISTS "i";`,
				`ALTER TABLE "t" DROP CONSTRAINT IF EXISTS "t_a_check";`,
			},
		},
		{
			name:     "the rollback file of a format with a rollback file",
			dirQuery: "?format=golang-migrate",
			initial:  "20200101000000_init.up.sql",
			built:    enumValueAddedBuilt,
			desired:  enumValueAddedDesired,
			reads:    "*.down.sql",
			want:     []string{`DROP INDEX IF EXISTS "i";`},
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
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.desired), 0o600), qt.IsNil)
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb("migrate", "diff", "change", "--dir", dirURL,
				"--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			written := filesMatching(c, dir, test.reads)
			c.Assert(written, qt.Contains, `ALTER TYPE "e" RENAME TO "e__ptah_old";`, qt.Commentf("%s", out))
			for _, want := range test.want {
				c.Assert(written, qt.Contains, want)
			}
		})
	}
}

// TestCompatMigrateDownE2E_ADerivedRollbackKeepsItsGuardsAfterAnEnumRebuild is the row
// above for the verb that forwards to the native command: see
// TestMigrationsDownE2E_ADerivedRollbackThatRebuildsAnEnumLeavesItsDropWithoutAGuard.
func TestCompatMigrateDownE2E_ADerivedRollbackKeepsItsGuardsAfterAnEnumRebuild(t *testing.T) {
	c := qt.New(t)
	dir, target, dev := derivedRollbackDir(c)

	out, err := runCompatVerb("migrate", "down", "--url", target, "--dir", "file://"+filepath.ToSlash(dir),
		"--dev-url", dev, "--to-version", "20200101000000", "--plan", "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `ALTER TYPE "e" RENAME TO "e__ptah_old"`)
	c.Assert(out, qt.Contains, `DROP INDEX IF EXISTS "i"`)
}
