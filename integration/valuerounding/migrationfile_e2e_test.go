//go:build integration

package valuerounding_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A migration file is generated against one database and applied later, often
// to another. `migrations plan` and `migrations generate` read the rows of the
// database they compare with, and the up file carries a check of kind
// rounded-values for each column, which `migrations up` and `ptah-compat
// migrate apply` evaluate against the database the file runs on. These runs
// drive the shipped binaries over each engine of roundingEngines and read the
// stored values back.

// roundingCheck is how the up file declares a check of a column.
const roundingCheck = "-- +ptah check kind=rounded-values "

// checkRefusal is what a failed check of kind rounded-values says.
const checkRefusal = "the migration converts a column to a type that keeps fewer digits after the point while the " +
	"table holds a value with more digits, which the server rounds or cuts without an error"

// unqualified is what the migration commands say of a column a refusal of
// `schema apply` names: they compare a table of the schema the connection
// resolves under its bare name, the name the generated statements use.
func unqualified(column string) string {
	return strings.Replace(column, `of table "public.`, `of table "`, 1)
}

// generate runs `migrations generate` from target to engine's desired schema
// into a migration directory of its own, with the extra arguments, and returns
// the run and the directory.
func generate(c *qt.C, engine roundingEngine, target string, extra ...string) (clirun.Result, string) {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.Mkdir(dir, 0o750), qt.IsNil)
	args := append([]string{
		"migrations", "generate", "--db-url", target, "--schema-file", schemaFile(c, engine.desired),
		"--migrations-dir", dir, "--name", "lower_scale",
	}, extra...)
	return ptah(c, args...), dir
}

// generated is the directory `migrations generate` wrote while no row of target
// would change, and the text of its up file.
func generated(c *qt.C, engine roundingEngine, target string) (dir, text string) {
	c.Helper()
	result, dir := generate(c, engine, target)
	c.Assert(result.ExitCode, qt.Equals, 0, outcome(result))
	return dir, upFile(c, dir)
}

// upFile is the text of the one up file dir holds.
func upFile(c *qt.C, dir string) string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	text, err := os.ReadFile(files[0])
	c.Assert(err, qt.IsNil)
	return string(text)
}

// holdDigits replaces the rows of target with engine's rows of digits.
func holdDigits(c *qt.C, engine roundingEngine, target string) {
	c.Helper()
	run(c, target, "TRUNCATE TABLE prices")
	run(c, target, engine.digits)
}

// up runs `migrations up` over dir on target, with the extra arguments.
func up(c *qt.C, target, dir string, extra ...string) clirun.Result {
	c.Helper()
	return ptah(c, append([]string{"migrations", "up", "--db-url", target, "--migrations-dir", dir}, extra...)...)
}

// compat runs ptah-compat with the environment variables added.
func compat(c *qt.C, env []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), env...)}, args...)
}

// compatDir is how ptah-compat names a directory `migrations generate` wrote.
func compatDir(dir string) string {
	return "file://" + filepath.ToSlash(dir) + "?format=golang-migrate"
}

// hashed hashes dir for ptah-compat and returns the address it reads it at.
func hashed(c *qt.C, dir string) string {
	c.Helper()
	address := compatDir(dir)
	result := compat(c, nil, "migrate", "hash", "--dir", address)
	c.Assert(result.ExitCode, qt.Equals, 0, outcome(result))
	return address
}

// TestMigrationsPlanRefusesToRoundStoredValuesE2E plans the change over rows with
// digits the new types have no room for. The plan names each column, exits 2 and
// prints no SQL; asked with --allow-value-rounding, it warns of each column and
// prints the statements.
func TestMigrationsPlanRefusesToRoundStoredValuesE2E(t *testing.T) {
	for _, engine := range roundingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, engine.table, engine.digits)
			args := []string{"migrations", "plan", "--db-url", target, "--schema-file", schemaFile(c, engine.desired)}

			refused := ptah(c, args...)
			allowed := ptah(c, append(args, "--allow-value-rounding")...)

			c.Assert(refused.ExitCode, qt.Equals, 2, outcome(refused))
			c.Assert(refused.Stderr, qt.Contains, refusal)
			for _, column := range engine.refused {
				c.Assert(refused.Stderr, qt.Contains, unqualified(column))
			}
			c.Assert(refused.Stderr, qt.Contains, "or ask for the change with --allow-value-rounding")
			c.Assert(refused.Stdout, qt.Not(qt.Contains), "=== MIGRATION SQL ===")
			c.Assert(allowed.ExitCode, qt.Equals, 0, outcome(allowed))
			c.Assert(allowed.Stderr, qt.Contains, "which keeps fewer digits after the point, without a scan of the values the table holds")
			c.Assert(allowed.Stdout, qt.Contains, "=== MIGRATION SQL ===")
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.held)
		})
	}
}

// TestMigrationsGenerateRefusesToRoundStoredValuesE2E generates the change over
// rows with digits the new types have no room for. The run names each column,
// exits 2 and writes no file.
func TestMigrationsGenerateRefusesToRoundStoredValuesE2E(t *testing.T) {
	for _, engine := range roundingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, engine.table, engine.digits)

			result, dir := generate(c, engine, target)

			c.Assert(result.ExitCode, qt.Equals, 2, outcome(result))
			c.Assert(result.Stderr, qt.Contains, refusal)
			for _, column := range engine.refused {
				c.Assert(result.Stderr, qt.Contains, unqualified(column))
			}
			c.Assert(result.Stderr, qt.Contains, "or ask for the change with --allow-value-rounding")
			written, err := os.ReadDir(dir)
			c.Assert(err, qt.IsNil)
			c.Assert(written, qt.HasLen, 0)
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.held)
		})
	}
}

// TestMigrationsGenerateWritesTheFileOnRequestE2E asks for the change with
// --allow-value-rounding. The file carries a note for each column in place of
// its check, and `migrations up` applies it and rounds the values.
func TestMigrationsGenerateWritesTheFileOnRequestE2E(t *testing.T) {
	for _, engine := range roundingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, engine.table, engine.digits)

			result, dir := generate(c, engine, target, "--allow-value-rounding")
			c.Assert(result.ExitCode, qt.Equals, 0, outcome(result))
			text := upFile(c, dir)
			applied := up(c, target, dir)

			c.Assert(text, qt.Not(qt.Contains), roundingCheck)
			c.Assert(strings.Count(text, "the rounding was allowed when this file was generated, so no check reads the rows"),
				qt.Equals, len(engine.refused), qt.Commentf("%s", text))
			c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.rounded)
		})
	}
}

// TestMigrationsUpRefusesARowInsertedAfterGenerationE2E generates the file while
// no row would change, and inserts rows with digits before applying it. The
// file's checks read the rows again, the run refuses and names the flag that
// asks for the change, and every value reads back as it was.
func TestMigrationsUpRefusesARowInsertedAfterGenerationE2E(t *testing.T) {
	for _, engine := range roundingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, engine.table, engine.fits)
			dir, text := generated(c, engine, target)
			holdDigits(c, engine, target)

			applied := up(c, target, dir)

			c.Assert(strings.Count(text, roundingCheck), qt.Equals, len(engine.refused), qt.Commentf("%s", text))
			c.Assert(applied.ExitCode, qt.Equals, 2, outcome(applied))
			c.Assert(applied.Stderr, qt.Contains, checkRefusal)
			c.Assert(applied.Stderr, qt.Contains, "or ask for the change with --allow-value-rounding")
			c.Assert(applied.Stderr, qt.Not(qt.Contains), "--skip-checks")
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.held)
		})
	}
}

// TestMigrationsUpAppliesTheFileWhenNoRowRoundsE2E is the control: the same file
// over rows whose digits past the new types are zero applies, and the values
// read back unchanged under the new types.
func TestMigrationsUpAppliesTheFileWhenNoRowRoundsE2E(t *testing.T) {
	for _, engine := range roundingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, engine.table, engine.fits)
			dir, _ := generated(c, engine, target)

			applied := up(c, target, dir)

			c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.fitted)
		})
	}
}

// TestMigrationsUpRoundsStoredValuesOnRequestE2E applies the file over rows with
// digits with --allow-value-rounding. The run warns of each skipped check,
// applies, and the values read back as the server rounded or cut them.
func TestMigrationsUpRoundsStoredValuesOnRequestE2E(t *testing.T) {
	for _, engine := range roundingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, engine.table, engine.fits)
			dir, _ := generated(c, engine, target)
			holdDigits(c, engine, target)

			applied := up(c, target, dir, "--allow-value-rounding")

			c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
			c.Assert(applied.Stderr, qt.Contains, "is skipped, so migration")
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.rounded)
		})
	}
}

// TestCompatMigrateApplyRefusesARowInsertedAfterGenerationE2E applies the same
// file through `ptah-compat migrate apply`, which runs the checks by default and
// exits 1 as the community binary exits on an error. PTAH_ALLOW_VALUE_ROUNDING=1
// is how that surface asks for the change, and the run with it rounds the
// values.
func TestCompatMigrateApplyRefusesARowInsertedAfterGenerationE2E(t *testing.T) {
	for _, engine := range roundingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, engine.table, engine.fits)
			dir, _ := generated(c, engine, target)
			holdDigits(c, engine, target)
			args := []string{"migrate", "apply", "--url", target, "--dir", hashed(c, dir), "--allow-dirty"}

			refused := compat(c, nil, args...)
			c.Assert(refused.ExitCode, qt.Equals, 1, outcome(refused))
			c.Assert(refused.Stderr, qt.Contains, checkRefusal)
			c.Assert(refused.Stderr, qt.Contains, "or ask for the change with PTAH_ALLOW_VALUE_ROUNDING=1")
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.held)
			rounded := compat(c, []string{"PTAH_ALLOW_VALUE_ROUNDING=1"}, args...)

			c.Assert(rounded.ExitCode, qt.Equals, 0, outcome(rounded))
			c.Assert(rounded.Stderr, qt.Contains, "is skipped, so migration")
			c.Assert(rounded.Stdout, qt.Not(qt.Contains), "pre-migration checks of version")
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.rounded)
		})
	}
}

// TestCompatMigrateApplyAppliesTheFileWhenNoRowRoundsE2E is the control on the
// compatibility surface: over rows that fit, the checks hold and the file
// applies.
func TestCompatMigrateApplyAppliesTheFileWhenNoRowRoundsE2E(t *testing.T) {
	for _, engine := range roundingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, engine.table, engine.fits)
			dir, _ := generated(c, engine, target)

			applied := compat(c, nil, "migrate", "apply", "--url", target, "--dir", hashed(c, dir), "--allow-dirty")

			c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.fitted)
		})
	}
}

// timeouts matches the timeout directives of a generated file, which strict
// compatibility refuses on their own.
var timeouts = regexp.MustCompile(`(?m)^-- \+ptah (lock|statement)_timeout=.*\n`)

// TestStrictCompatRefusesTheRoundingCheckE2E runs the generated directory under
// PTAH_ATLAS_STRICT_COMPAT=1, which refuses a Ptah directive where a command
// executes a migration body: the apply refuses, and so does a file whose only
// directive is the check. The checksum-only reads keep the bytes: `migrate hash`
// writes the same atlas.sum as the default surface, and `migrate validate` with
// no dev database and `migrate status` succeed.
func TestStrictCompatRefusesTheRoundingCheckE2E(t *testing.T) {
	c := qt.New(t)
	engine := engineNamed(c, "postgres")
	target := engine.server.scratch(c, engine.table, engine.fits)
	dir, text := generated(c, engine, target)
	address := hashed(c, dir)
	sum, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	strict := []string{"PTAH_ATLAS_STRICT_COMPAT=1"}
	checkOnly := filepath.Join(c.TempDir(), "check-only")
	c.Assert(os.Mkdir(checkOnly, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(checkOnly, "1_lower_scale.sql"), []byte(timeouts.ReplaceAllString(text, "")), 0o600), qt.IsNil)
	hashedCheckOnly := compat(c, nil, "migrate", "hash", "--dir", "file://"+filepath.ToSlash(checkOnly))
	c.Assert(hashedCheckOnly.ExitCode, qt.Equals, 0, outcome(hashedCheckOnly))

	applied := compat(c, strict, "migrate", "apply", "--url", target, "--dir", address, "--allow-dirty")
	appliedCheckOnly := compat(c, strict, "migrate", "apply", "--url", target, "--dir", "file://"+filepath.ToSlash(checkOnly),
		"--allow-dirty")
	rehashed := compat(c, strict, "migrate", "hash", "--dir", address)
	validated := compat(c, strict, "migrate", "validate", "--dir", address)
	status := compat(c, strict, "migrate", "status", "--url", target, "--dir", address)

	c.Assert(applied.ExitCode, qt.Equals, 1, outcome(applied))
	c.Assert(applied.Stderr, qt.Contains, "Atlas Community Edition strict compatibility does not support Ptah")
	c.Assert(appliedCheckOnly.ExitCode, qt.Equals, 1, outcome(appliedCheckOnly))
	c.Assert(appliedCheckOnly.Stderr, qt.Contains,
		"Atlas Community Edition strict compatibility does not support Ptah pre-migration checks in 1_lower_scale.sql")
	c.Assert(read(c, target, "SELECT amount::text FROM prices"), qt.Equals, "1.500")
	c.Assert(rehashed.ExitCode, qt.Equals, 0, outcome(rehashed))
	resummed, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(resummed), qt.Equals, string(sum))
	c.Assert(validated.ExitCode, qt.Equals, 0, outcome(validated))
	c.Assert(status.ExitCode, qt.Equals, 0, outcome(status))
	c.Assert(status.Stdout, qt.Contains, "PENDING")
}

// withoutTimeouts removes the timeout directives from the up file of dir, which
// --tx-mode all refuses on their own, and leaves the check.
func withoutTimeouts(c *qt.C, dir string) {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(os.WriteFile(files[0], []byte(timeouts.ReplaceAllString(upFile(c, dir), "")), 0o600), qt.IsNil)
}

// checkedTarget is a database of engine holding rows with digits, and the
// directory `migrations generate` wrote for it while its rows fit, with the
// timeout directives removed.
func checkedTarget(c *qt.C, engine roundingEngine) (target, dir string) {
	c.Helper()
	target = engine.server.scratch(c, engine.table, engine.fits)
	dir, _ = generated(c, engine, target)
	withoutTimeouts(c, dir)
	holdDigits(c, engine, target)
	return target, dir
}

// skippedCheck is how a run says it skipped a check of kind rounded-values.
const skippedCheck = "is skipped, so migration"

// TestSkippedRoundingCheckWarnsOnEveryPathE2E applies a generated file over rows
// with digits on the paths that skip its check without evaluating it: a tx-mode
// all batch, which evaluates no check, with the rounding allowed on the native
// and the compatibility surface, and --skip-checks. Each run applies, warns of
// the skipped check, and rounds the values. Without an opt-in the batch refuses
// the check and the values are kept, which is the control.
func TestSkippedRoundingCheckWarnsOnEveryPathE2E(t *testing.T) {
	c := qt.New(t)
	engine := engineNamed(c, "postgres")
	nativeTarget, nativeDir := checkedTarget(c, engine)
	rehashed := ptah(c, "migrations", "hash", "--dir", nativeDir)
	c.Assert(rehashed.ExitCode, qt.Equals, 0, outcome(rehashed))
	compatTarget, compatDir := checkedTarget(c, engine)
	address := hashed(c, compatDir)
	skipTarget, skipDir := checkedTarget(c, engine)
	rehashedSkip := ptah(c, "migrations", "hash", "--dir", skipDir)
	c.Assert(rehashedSkip.ExitCode, qt.Equals, 0, outcome(rehashedSkip))

	refused := up(c, nativeTarget, nativeDir, "--tx-mode", "all")
	kept := read(c, nativeTarget, engine.readBack)
	native := up(c, nativeTarget, nativeDir, "--tx-mode", "all", "--allow-value-rounding")
	compatRun := compat(c, []string{"PTAH_ALLOW_VALUE_ROUNDING=1"},
		"migrate", "apply", "--url", compatTarget, "--dir", address, "--allow-dirty", "--tx-mode", "all")
	skipped := up(c, skipTarget, skipDir, "--skip-checks")

	c.Assert(refused.ExitCode, qt.Equals, 2, outcome(refused))
	c.Assert(refused.Stderr, qt.Contains, "declares pre-migration checks, which cannot run with tx-mode all")
	c.Assert(refused.Stderr, qt.Not(qt.Contains), "timeout")
	c.Assert(kept, qt.Equals, engine.held)
	c.Assert(native.ExitCode, qt.Equals, 0, outcome(native))
	c.Assert(native.Stderr, qt.Contains, skippedCheck)
	c.Assert(read(c, nativeTarget, engine.readBack), qt.Equals, engine.rounded)
	c.Assert(compatRun.ExitCode, qt.Equals, 0, outcome(compatRun))
	c.Assert(compatRun.Stderr, qt.Contains, skippedCheck)
	c.Assert(read(c, compatTarget, engine.readBack), qt.Equals, engine.rounded)
	c.Assert(skipped.ExitCode, qt.Equals, 0, outcome(skipped))
	c.Assert(skipped.Stderr, qt.Contains, skippedCheck)
	c.Assert(read(c, skipTarget, engine.readBack), qt.Equals, engine.rounded)
}
