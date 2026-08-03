package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests pin stokaro/ptah#974 on `ptah-compat migrate status`: the
// atlas.sum integrity gate that already guarded `migrate apply` and
// `migrate validate` now guards status too.
//
// Measured against the pinned community binary v1.3.0. Before this change,
// status reported normally and exited 0 on every drift state below — most
// misleadingly on a directory whose only migration had been DELETED after
// hashing, where it printed "Database is up to date".
//
// The set half of the same gate lives in migrate_set_integrity_test.go.

const (
	// statusIntegrityMigration is the one migration every fixture below starts
	// from. Its name is load-bearing: it appears verbatim in the drift pointer
	// lines the community binary prints.
	statusIntegrityMigration = "20260101000000_init.sql"
	statusIntegritySecond    = "20260102000000_two.sql"

	// atlasChecksumGuidance is the guidance block the community binary writes to
	// STDOUT for a directory carrying no atlas.sum. Its counterpart with a drift
	// pointer is built by atlasChecksumGuidanceWith.
	atlasChecksumGuidance = "You have a checksum error in your migration directory.\n" +
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n"

	atlasChecksumNotFoundErr = "Error: checksum file not found\n"
	atlasChecksumMismatchErr = "Error: checksum mismatch\n"
)

// atlasChecksumGuidanceWith builds the guidance block for a hashed directory
// that drifted, with the measured `L<line>: <file> was <verb>` pointer.
func atlasChecksumGuidanceWith(pointer string) string {
	return "You have a checksum error in your migration directory.\n" +
		"\n\t" + pointer + "\n\n" +
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n"
}

// writeStatusIntegrityUnhashed writes one Atlas migration and no atlas.sum.
func writeStatusIntegrityUnhashed(c *qt.C, dir string) {
	c.Helper()
	writeAtlasApplyProjectMigration(c, dir, statusIntegrityMigration,
		"CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n")
}

// writeStatusIntegrityHashed writes the same directory with a valid atlas.sum.
func writeStatusIntegrityHashed(c *qt.C, dir string) {
	c.Helper()
	writeStatusIntegrityUnhashed(c, dir)
	writeAtlasApplyProjectSum(c, dir)
}

// writeStatusIntegrityEdited hashes the directory and then edits the migration,
// leaving atlas.sum stale.
func writeStatusIntegrityEdited(c *qt.C, dir string) {
	c.Helper()
	writeStatusIntegrityHashed(c, dir)
	writeAtlasApplyProjectMigration(c, dir, statusIntegrityMigration,
		"CREATE TABLE t1 (id INTEGER PRIMARY KEY, extra TEXT);\n")
}

// writeStatusIntegrityAdded hashes the directory and then adds a migration the
// sum does not cover.
func writeStatusIntegrityAdded(c *qt.C, dir string) {
	c.Helper()
	writeStatusIntegrityHashed(c, dir)
	writeAtlasApplyProjectMigration(c, dir, statusIntegritySecond,
		"CREATE TABLE t2 (id INTEGER PRIMARY KEY);\n")
}

// writeStatusIntegrityRemoved hashes the directory and then deletes the only
// migration. This is the state pre-change status reported as "Database is up to
// date", which is why it is in the table rather than represented by the edited
// row alone.
func writeStatusIntegrityRemoved(c *qt.C, dir string) {
	c.Helper()
	writeStatusIntegrityHashed(c, dir)
	c.Assert(os.Remove(filepath.Join(dir, statusIntegrityMigration)), qt.IsNil)
}

// TestCompatMigrateStatus_DriftedDirRefuses is the discriminator for #974.
//
// Pre-change every row here exits 0 and prints `=== MIGRATION STATUS ===`; the
// removed row additionally reports "Database is up to date" for a directory
// whose migration is gone. Post-change every row exits 1 with output
// byte-identical to the pinned community binary v1.3.0.
func TestCompatMigrateStatus_DriftedDirRefuses(t *testing.T) {
	tests := []struct {
		name       string
		writeDir   func(*qt.C, string)
		wantStdout string
		wantStderr string
		wantErr    string
	}{
		{
			name:       "never hashed",
			writeDir:   writeStatusIntegrityUnhashed,
			wantStdout: atlasChecksumGuidance,
			wantStderr: atlasChecksumNotFoundErr,
			wantErr:    "checksum file not found",
		},
		{
			name:       "hashed then edited",
			writeDir:   writeStatusIntegrityEdited,
			wantStdout: atlasChecksumGuidanceWith("L2: " + statusIntegrityMigration + " was edited"),
			wantStderr: atlasChecksumMismatchErr,
			wantErr:    "checksum mismatch",
		},
		{
			name:       "hashed then added",
			writeDir:   writeStatusIntegrityAdded,
			wantStdout: atlasChecksumGuidanceWith("L3: " + statusIntegritySecond + " was added"),
			wantStderr: atlasChecksumMismatchErr,
			wantErr:    "checksum mismatch",
		},
		{
			name:       "hashed then removed",
			writeDir:   writeStatusIntegrityRemoved,
			wantStdout: atlasChecksumGuidanceWith("L2: " + statusIntegrityMigration + " was removed"),
			wantStderr: atlasChecksumMismatchErr,
			wantErr:    "checksum mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := filepath.Join(tempDir, "m")
			tt.writeDir(c, dir)
			dbPath := filepath.Join(tempDir, "status.db")

			stdout, stderr, err := runCompat(
				"migrate", "status",
				"--url", "sqlite://"+dbPath,
				"--dir", "file://"+dir,
			)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, tt.wantErr)
			c.Assert(stdout, qt.Equals, tt.wantStdout)
			c.Assert(stderr, qt.Equals, tt.wantStderr)
			// The gate precedes the connection, so the target was never opened.
			_, statErr := os.Stat(dbPath)
			c.Assert(os.IsNotExist(statErr), qt.IsTrue)
		})
	}
}

// TestCompatMigrateStatus_RefusalPrecedesConnection pins the gate's position
// relative to the database connection.
//
// The discriminator here is the OUTPUT, not the exit code: pre-change this
// invocation already exited 1, with a connection error. An assertion on the
// exit code alone would pass with and without the fix and would prove nothing.
func TestCompatMigrateStatus_RefusalPrecedesConnection(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "m")
	writeStatusIntegrityUnhashed(c, dir)

	stdout, stderr, err := runCompat(
		"migrate", "status",
		"--url", "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
		"--dir", "file://"+dir,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, atlasChecksumGuidance)
	c.Assert(stderr, qt.Equals, atlasChecksumNotFoundErr)
}

// TestCompatMigrateStatus_UnhashedDirWithNonVersionedSQLRefuses pins that the
// gate keys on the presence of a *.sql file rather than on migrations the
// planner can parse.
//
// Output-only discriminator again: pre-change this exited 1 with
// `no migration files matched format "atlas"`.
func TestCompatMigrateStatus_UnhashedDirWithNonVersionedSQLRefuses(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m_foo")
	writeAtlasApplyProjectMigration(c, dir, "foo.sql", "CREATE TABLE foo (id INTEGER PRIMARY KEY);\n")

	stdout, stderr, err := runCompat(
		"migrate", "status",
		"--url", "sqlite://"+filepath.Join(tempDir, "foo.db"),
		"--dir", "file://"+dir,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum file not found")
	c.Assert(stdout, qt.Equals, atlasChecksumGuidance)
	c.Assert(stderr, qt.Equals, atlasChecksumNotFoundErr)
}

// TestCompatMigrateStatus_ConfigResolvedDirRefuses covers the directory reached
// through atlas.hcl rather than through --dir. It is the row that catches a fix
// applied to the --dir branch alone.
func TestCompatMigrateStatus_ConfigResolvedDirRefuses(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeStatusIntegrityUnhashed(c, "migrations")
	writeAtlasApplyProjectConfig(c, filepath.Join(root, "status.db"), "atlas", "LINEAR")

	stdout, stderr, err := runCompat("migrate", "status", "--env", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum file not found")
	c.Assert(stdout, qt.Equals, atlasChecksumGuidance)
	c.Assert(stderr, qt.Equals, atlasChecksumNotFoundErr)
}

// TestCompatMigrateStatus_FormatTemplateRefuses pins that the Go-template
// output path is gated too. It renders after the connection, so it sits
// downstream of the gate — this asserts that rather than assuming it.
func TestCompatMigrateStatus_FormatTemplateRefuses(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m")
	writeStatusIntegrityUnhashed(c, dir)

	stdout, stderr, err := runCompat(
		"migrate", "status",
		"--url", "sqlite://"+filepath.Join(tempDir, "format.db"),
		"--dir", "file://"+dir,
		"--format", "{{ .Current }}",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum file not found")
	c.Assert(stdout, qt.Equals, atlasChecksumGuidance)
	c.Assert(stderr, qt.Equals, atlasChecksumNotFoundErr)
}

// TestCompatMigrateStatus_AntiRegressionCleanDirsReportNormally holds the three
// states that already agreed with the community binary before this change.
//
// They are ANTI-REGRESSION rows, not discriminators: each exits 0 both before
// and after, and none of them would fail if the gate were never wired in. They
// exist to catch a gate that over-refuses — in particular the empty-directory
// bootstrap that #970 established must keep working.
func TestCompatMigrateStatus_AntiRegressionCleanDirsReportNormally(t *testing.T) {
	tests := []struct {
		name     string
		writeDir func(*qt.C, string)
		wantLine string
	}{
		{
			name:     "anti-regression: empty directory",
			writeDir: func(c *qt.C, dir string) { c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil) },
			wantLine: "Total Migrations: 0",
		},
		{
			name: "anti-regression: no SQL files",
			writeDir: func(c *qt.C, dir string) {
				c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "README.md"), []byte("migrations live here\n"), 0o600), qt.IsNil)
			},
			wantLine: "Total Migrations: 0",
		},
		{
			name:     "anti-regression: hashed clean directory",
			writeDir: writeStatusIntegrityHashed,
			wantLine: "Total Migrations: 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := filepath.Join(tempDir, "m")
			tt.writeDir(c, dir)

			stdout, stderr, err := runCompat(
				"migrate", "status",
				"--url", "sqlite://"+filepath.Join(tempDir, "clean.db"),
				"--dir", "file://"+dir,
			)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "=== MIGRATION STATUS ===")
			c.Assert(stdout, qt.Contains, tt.wantLine)
			c.Assert(stderr, qt.Equals, "")
		})
	}
}

// TestCompatMigrateStatus_UnhashedNestedSQLReportsNothingPending replaces the
// _KnownDivergence pin this used to be.
//
// The divergence it recorded — exit 1 here against the community binary's 0 —
// was the visible end of stokaro/ptah#976: the registrar recursed, so a nested
// file was pending here and nothing-to-execute there, and the gate refused the
// whole directory to keep an unhashed migration from running unverified. With
// the selection narrowed to the set atlas.sum covers there is no such
// migration, so there is nothing left to diverge about and the pin would have
// been preserving the bug rather than the boundary.
//
// Status is asserted rather than assumed to follow apply: the two verbs share
// one gate precisely so a read-only verb cannot report on a directory apply
// would refuse, and the reverse — reporting a refusal where apply now proceeds
// — is the same drift from the other side.
func TestCompatMigrateStatus_UnhashedNestedSQLReportsNothingPending(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m_nested")
	writeAtlasApplyProjectMigration(c, filepath.Join(dir, "sub"), statusIntegrityMigration,
		"CREATE TABLE nested (id INTEGER PRIMARY KEY);\n")

	stdout, stderr, err := runCompat(
		"migrate", "status",
		"--url", "sqlite://"+filepath.Join(tempDir, "nested.db"),
		"--dir", "file://"+dir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "Total Migrations: 0")
	c.Assert(stderr, qt.Equals,
		"warning: sub/"+statusIntegrityMigration+" is not covered by atlas.sum and will not run; "+
			"Atlas migrations are top-level files named *.sql\n")
}

// TestCompatMigrateStatus_MatchesValidateOutput proves status shares one code
// path with `migrate validate`, so the two cannot drift in wording, stream, or
// exit value on the same directory.
func TestCompatMigrateStatus_MatchesValidateOutput(t *testing.T) {
	tests := []struct {
		name     string
		writeDir func(*qt.C, string)
	}{
		{name: "never hashed", writeDir: writeStatusIntegrityUnhashed},
		{name: "hashed then edited", writeDir: writeStatusIntegrityEdited},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := filepath.Join(tempDir, "m")
			tt.writeDir(c, dir)

			statusOut, statusErrOut, statusErr := runCompat(
				"migrate", "status",
				"--url", "sqlite://"+filepath.Join(tempDir, "parity.db"),
				"--dir", "file://"+dir,
			)
			validateOut, validateErrOut, validateErr := runCompat("migrate", "validate", "--dir", "file://"+dir)

			c.Assert(statusErr, qt.IsNotNil)
			c.Assert(validateErr, qt.IsNotNil)
			c.Assert(statusOut, qt.Equals, validateOut)
			c.Assert(statusErrOut, qt.Equals, validateErrOut)
			c.Assert(statusErr.Error(), qt.Equals, validateErr.Error())
		})
	}
}
