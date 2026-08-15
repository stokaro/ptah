package atlas_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// The set half of the #974 integrity gate. `migrate set` writes revision rows
// declaring a directory's history applied, so running it against a directory
// nothing verified records state derived from files that may have drifted —
// pre-change it did exactly that, exiting 0 and writing the rows.
//
// Measured against the pinned community binary v1.3.0. The fixture writers and
// the expected output constants are shared with
// migrate_status_integrity_test.go so the two verbs cannot drift from one
// another or from `migrate validate`.

const setIntegrityVersion = "20260101000000"

// TestCompatMigrateSet_DriftedDirRefuses is the discriminator for the set half.
//
// Pre-change every drift state exits 0 and prints `Current version is
// 20260101000000 (1 set):` (the removed state prints a removal summary
// instead), leaving revision rows behind. Post-change every one of them exits 1
// before the connection, so the database is never even created.
func TestCompatMigrateSet_DriftedDirRefuses(t *testing.T) {
	for _, drift := range statusIntegrityDrifts() {
		t.Run(drift.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := filepath.Join(tempDir, "m")
			writeStatusIntegrityDrifted(c, dir, drift)
			dbPath := filepath.Join(tempDir, "set.db")

			stdout, stderr, err := runCompat(
				"migrate", "set", setIntegrityVersion,
				"--url", "sqlite://"+dbPath,
				"--dir", "file://"+dir,
			)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, "checksum mismatch")
			c.Assert(stdout, qt.Equals, drift.wantStdout)
			c.Assert(stderr, qt.Equals, atlasChecksumMismatchErr)
			// Nothing was written: the refusal precedes the connection, so no
			// revision table and no database file exist.
			assertIntegrityTargetUntouched(c, dbPath)
		})
	}
}

// TestCompatMigrateSet_NeverHashedDirRefuses is the set counterpart of the
// missing-sum refusal, which is a different message from the drift states above
// and reaches the gate on a directory nothing ever hashed.
func TestCompatMigrateSet_NeverHashedDirRefuses(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m")
	writeStatusIntegrityUnhashed(c, dir)
	dbPath := filepath.Join(tempDir, "set.db")

	stdout, stderr, err := runCompat(
		"migrate", "set", setIntegrityVersion,
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum file not found")
	c.Assert(stdout, qt.Equals, atlasChecksumGuidance)
	c.Assert(stderr, qt.Equals, atlasChecksumNotFoundErr)
	assertIntegrityTargetUntouched(c, dbPath)
}

// TestCompatMigrateSet_RefusalPrecedesArityCheck pins the diagnostic ORDER the
// gate changes, which is measured rather than preserved from before: on the
// community binary a `migrate set` with the wrong number of positionals against
// an unhashed directory prints the checksum refusal, not an arity error.
//
// Output-only discriminator. Both rows already exited 1 pre-change, with
// `Error: accepts 1 arg(s), received N` on stderr and nothing on stdout, so an
// exit-code assertion here would prove nothing.
func TestCompatMigrateSet_RefusalPrecedesArityCheck(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "zero positionals", args: nil},
		{name: "two positionals", args: []string{"1", "2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := filepath.Join(tempDir, "m")
			writeStatusIntegrityUnhashed(c, dir)

			args := append([]string{"migrate", "set"}, tt.args...)
			args = append(args,
				"--url", "sqlite://"+filepath.Join(tempDir, "arity.db"),
				"--dir", "file://"+dir,
			)
			stdout, stderr, err := runCompat(args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, atlasChecksumGuidance)
			c.Assert(stderr, qt.Equals, atlasChecksumNotFoundErr)
		})
	}
}

// TestCompatMigrateSet_RefusalPrecedesConnection is the set counterpart of the
// status ordering row: the refusal is emitted even when --url cannot be
// reached. Output-only discriminator — pre-change this exited 1 with a
// connection error.
func TestCompatMigrateSet_RefusalPrecedesConnection(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "m")
	writeStatusIntegrityUnhashed(c, dir)

	stdout, stderr, err := runCompat(
		"migrate", "set", setIntegrityVersion,
		"--url", "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
		"--dir", "file://"+dir,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, atlasChecksumGuidance)
	c.Assert(stderr, qt.Equals, atlasChecksumNotFoundErr)
}

// TestCompatMigrateSet_ConfigResolvedDirRefuses covers the directory reached
// through atlas.hcl rather than through --dir.
func TestCompatMigrateSet_ConfigResolvedDirRefuses(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeStatusIntegrityUnhashed(c, "migrations")
	writeAtlasApplyProjectConfig(c, filepath.Join(root, "set.db"), "atlas", "LINEAR")

	stdout, stderr, err := runCompat("migrate", "set", setIntegrityVersion, "--env", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum file not found")
	c.Assert(stdout, qt.Equals, atlasChecksumGuidance)
	c.Assert(stderr, qt.Equals, atlasChecksumNotFoundErr)
}

// TestCompatMigrateSet_AntiRegressionHashedDirSets is an ANTI-REGRESSION row,
// not a discriminator: it passes before and after. It guards against a gate
// that over-refuses and breaks the ordinary hashed-directory workflow.
func TestCompatMigrateSet_AntiRegressionHashedDirSets(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m")
	writeStatusIntegrityHashed(c, dir)

	stdout, stderr, err := runCompat(
		"migrate", "set", setIntegrityVersion,
		"--url", "sqlite://"+filepath.Join(tempDir, "ok.db"),
		"--dir", "file://"+dir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "Current version is "+setIntegrityVersion+" (1 set):\n\n  + "+setIntegrityVersion+" (init)\n\n")
	c.Assert(stderr, qt.Equals, "")
}

// TestCompatMigrateSet_MatchesValidateOutput holds set's refusal byte-identical
// to `migrate validate` on the same directory, the property that made the apply
// gate reusable here in the first place.
func TestCompatMigrateSet_MatchesValidateOutput(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m")
	writeStatusIntegrityEdited(c, dir)

	setOut, setErrOut, setErr := runCompat(
		"migrate", "set", setIntegrityVersion,
		"--url", "sqlite://"+filepath.Join(tempDir, "parity.db"),
		"--dir", "file://"+dir,
	)
	validateOut, validateErrOut, validateErr := runCompat("migrate", "validate", "--dir", "file://"+dir)

	c.Assert(setErr, qt.IsNotNil)
	c.Assert(validateErr, qt.IsNotNil)
	c.Assert(setOut, qt.Equals, validateOut)
	c.Assert(setErrOut, qt.Equals, validateErrOut)
	c.Assert(setErr.Error(), qt.Equals, validateErr.Error())
}
