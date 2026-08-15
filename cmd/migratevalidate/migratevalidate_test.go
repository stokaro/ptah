package migratevalidate_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/migratevalidate"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

func execute(args ...string) (stdout, stderr string, err error) {
	cmd := migratevalidate.NewMigrateValidateCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func executeAtlas(args ...string) (stdout, stderr string, err error) {
	cmd := migratevalidate.NewAtlasMigrateValidateCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// migrationsDir writes a clean pair plus a matching ptah.sum and returns the dir.
func migrationsDir(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()
	dir := c.TempDir()
	write := func(name, content string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	write("0000000001_init.up.sql", "CREATE TABLE t (id INT);\n")
	write("0000000001_init.down.sql", "DROP TABLE t;\n")
	_, err := migratesum.Write(dir)
	c.Assert(err, qt.IsNil)
	return dir
}

func TestValidate_CleanDirectoryExitsZero(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := execute("--dir", migrationsDir(c.TB))
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, "OK: migrations directory matches ptah.sum\n")
	c.Assert(stderr, qt.Equals, "")
}

func TestValidate_AutoReadsAtlasSum(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_initial.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	stdout, _, err := execute("--dir", dir)
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "OK: migrations directory matches atlas.sum")
}

func TestValidate_EditedMigrationExitsOneWithDiff(t *testing.T) {
	c := qt.New(t)

	dir := migrationsDir(c.TB)
	// Tamper with an already-hashed migration.
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE t (id BIGINT);\n"), 0o600), qt.IsNil)

	_, stderr, err := execute("--dir", dir)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr, qt.Contains, "changed: 0000000001_init.up.sql")
}

func TestValidate_MissingSumFileExitsTwo(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)

	_, stderr, err := execute("--dir", dir)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(err, qt.ErrorMatches, ".*ptah.sum not found.*")
	// The actionable guidance must reach the user, not be swallowed.
	c.Assert(stderr, qt.Contains, "run `ptah migrations hash`")
}

func TestValidate_MissingDirectoryExitsTwo(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := execute("--dir", filepath.Join(t.TempDir(), "does-not-exist"))
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	// The message surfaces the directory and the underlying stat error.
	c.Assert(stderr, qt.Contains, "migrations directory")
	c.Assert(err, qt.ErrorMatches, ".*does-not-exist.*")
}

func TestAtlasDirectoryError_MissingStatPreservesPathCause(t *testing.T) {
	c := qt.New(t)
	pathErr := &os.PathError{Op: "stat", Path: "nope", Err: fs.ErrNotExist}
	original := fmt.Errorf("migrations directory nope: %w", pathErr)

	got := migratevalidate.AtlasDirectoryError("nope", original)

	c.Assert(got.Error(), qt.Equals, "sql/migrate: stat nope: file does not exist")
	c.Assert(got, qt.ErrorIs, original)
	c.Assert(got, qt.ErrorIs, fs.ErrNotExist)
	var gotPathErr *os.PathError
	c.Assert(got, qt.ErrorAs, &gotPathErr)
	c.Assert(gotPathErr, qt.Equals, pathErr)
}

func TestAtlasDirectoryError_NonMatchingErrorsUnchanged(t *testing.T) {
	tests := []struct {
		name string
		dir  string
		err  error
	}{
		{
			name: "regular file",
			dir:  "migrations",
			err:  errors.New("migrations directory migrations: not a directory"),
		},
		{
			name: "stat permission denied",
			dir:  "migrations",
			err: fmt.Errorf("migrations directory migrations: %w", &os.PathError{
				Op: "stat", Path: "migrations", Err: fs.ErrPermission,
			}),
		},
		{
			name: "missing open",
			dir:  "migrations",
			err: fmt.Errorf("open migrations directory: %w", &os.PathError{
				Op: "open", Path: "migrations", Err: fs.ErrNotExist,
			}),
		},
		{
			name: "missing stat with unrelated context",
			dir:  "migrations",
			err: fmt.Errorf("read config: %w", &os.PathError{
				Op: "stat", Path: "migrations", Err: fs.ErrNotExist,
			}),
		},
		{
			name: "missing stat for another directory",
			dir:  "migrations",
			err: fmt.Errorf("migrations directory other: %w", &os.PathError{
				Op: "stat", Path: "other", Err: fs.ErrNotExist,
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := migratevalidate.AtlasDirectoryError(test.dir, test.err)

			c.Assert(got, qt.Equals, test.err)
		})
	}
}

func TestValidate_PositionalArgExitsTwoWithMessage(t *testing.T) {
	c := qt.New(t)

	// A stray positional (e.g. the path typed without --dir) is a usage
	// error (exit 2 with a message), not a silent exit 1 that would look
	// like drift.
	_, stderr, err := execute(migrationsDir(c.TB), "stray")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "unexpected positional arguments")
}

func TestValidate_CorruptSumFileExitsTwoNotOne(t *testing.T) {
	c := qt.New(t)

	dir := migrationsDir(c.TB)
	// A structurally broken ptah.sum (an h1: hash that is not valid base64)
	// is a usage failure (exit 2), not content drift (exit 1).
	c.Assert(os.WriteFile(filepath.Join(dir, "ptah.sum"),
		[]byte("h1:not-valid-base64!!\n"), 0o600), qt.IsNil)

	stdout, stderr, err := execute("--dir", dir)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, "malformed directory hash line")
}

func TestValidate_AtlasMalformedSumMatchesAtlasStreams(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_initial.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.sum"),
		[]byte("h1:tampered\n"), 0o600), qt.IsNil)

	stdout, stderr, err := executeAtlas("--dir", dir, "--dir-format", "atlas")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorMatches, "checksum mismatch")
	c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n"+
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
	c.Assert(stderr, qt.Equals, "Error: checksum mismatch\n")
}

func TestValidate_AtlasCleanDirectoryIsSilent(t *testing.T) {
	c := qt.New(t)
	dir := cleanAtlasDirectory(c.TB)

	stdout, stderr, err := executeAtlas("--dir", dir, "--dir-format", "atlas")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
}

func TestValidate_AtlasDevReplayIsSilent(t *testing.T) {
	c := qt.New(t)
	dir := cleanAtlasDirectory(c.TB)
	devDBPath := filepath.Join(t.TempDir(), "dev.db")

	stdout, stderr, err := executeAtlas(
		"--dir", dir,
		"--dir-format", "atlas",
		"--dev-url", "sqlite://"+devDBPath,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
}

func TestValidate_AtlasMissingSumMatchesAtlasStreams(t *testing.T) {
	c := qt.New(t)
	dir := atlasDirectoryWithoutSum(c.TB)

	stdout, stderr, err := executeAtlas("--dir", dir, "--dir-format", "atlas")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorMatches, "checksum file not found")
	c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n"+
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
	c.Assert(stderr, qt.Equals, "Error: checksum file not found\n")
}

func TestValidate_AtlasIntegrityDriftMatchesAtlasStreams(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	migrationPath := filepath.Join(dir, "1_initial.sql")
	c.Assert(os.WriteFile(migrationPath, []byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(migrationPath, []byte("CREATE TABLE t (id BIGINT);\n"), 0o600), qt.IsNil)

	stdout, stderr, err := executeAtlas("--dir", dir, "--dir-format", "atlas")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorMatches, "checksum mismatch")
	c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n\n"+
		"\tL2: 1_initial.sql was edited\n\n"+
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
	c.Assert(stderr, qt.Equals, "Error: checksum mismatch\n")
}

func TestValidate_AtlasAddedMigrationMatchesAtlasStreams(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_initial.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "2_second.sql"),
		[]byte("CREATE TABLE u (id INT);\n"), 0o600), qt.IsNil)

	stdout, stderr, err := executeAtlas("--dir", dir, "--dir-format", "atlas")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorMatches, "checksum mismatch")
	c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n\n"+
		"\tL3: 2_second.sql was added\n\n"+
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
	c.Assert(stderr, qt.Equals, "Error: checksum mismatch\n")
}

func TestValidate_AtlasRemovedMigrationMatchesAtlasStreams(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	firstMigration := filepath.Join(dir, "1_initial.sql")
	c.Assert(os.WriteFile(firstMigration, []byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "2_second.sql"),
		[]byte("CREATE TABLE u (id INT);\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Remove(firstMigration), qt.IsNil)

	stdout, stderr, err := executeAtlas("--dir", dir, "--dir-format", "atlas")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorMatches, "checksum mismatch")
	c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n\n"+
		"\tL2: 1_initial.sql was removed\n\n"+
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
	c.Assert(stderr, qt.Equals, "Error: checksum mismatch\n")
}

func TestValidate_AtlasDuplicateSumEntryMatchesAtlasStreams(t *testing.T) {
	c := qt.New(t)
	dir := duplicateAtlasSumDirectory(c.TB)

	stdout, stderr, err := executeAtlas("--dir", dir, "--dir-format", "atlas")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorMatches, "checksum mismatch")
	c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n"+
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
	c.Assert(stderr, qt.Equals, "Error: checksum mismatch\n")
}

func TestValidate_NativeDuplicateSumEntryExitsTwo(t *testing.T) {
	c := qt.New(t)
	dir := duplicateAtlasSumDirectory(c.TB)

	stdout, stderr, err := execute("--dir", dir, "--dir-format", "atlas")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(err, qt.ErrorMatches, `failed to parse atlas\.sum: duplicate entry for "1_initial\.sql"`)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, `duplicate entry for "1_initial.sql"`)
}

func TestValidate_NativeSuccessStdoutWriteFailure(t *testing.T) {
	c := qt.New(t)
	dir := migrationsDir(c.TB)
	cmd := migratevalidate.NewMigrateValidateCommand()
	cmd.SetOut(failingWriter{})
	cmd.SetArgs([]string{"--dir", dir})

	err := cmd.Execute()

	c.Assert(exitcode.Code(err, 2), qt.Equals, 2)
	c.Assert(err, qt.ErrorIs, errWriteFailed)
	c.Assert(err, qt.ErrorMatches, "write validation success: write failed")
}

func TestValidate_AtlasChecksumStdoutWriteFailure(t *testing.T) {
	c := qt.New(t)
	dir := malformedAtlasDirectory(c.TB)
	cmd := migratevalidate.NewAtlasMigrateValidateCommand()
	var stderr bytes.Buffer
	cmd.SetOut(failingWriter{})
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--dir", dir, "--dir-format", "atlas"})

	err := cmd.Execute()
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorIs, errWriteFailed)
	c.Assert(err, qt.ErrorMatches, "checksum mismatch: failed to write checksum output: write checksum guidance: write failed")
	c.Assert(stderr.String(), qt.Equals, "Error: checksum mismatch\n")
}

func TestValidate_AtlasChecksumStderrWriteFailure(t *testing.T) {
	c := qt.New(t)
	dir := malformedAtlasDirectory(c.TB)
	cmd := migratevalidate.NewAtlasMigrateValidateCommand()
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(failingWriter{})
	cmd.SetArgs([]string{"--dir", dir, "--dir-format", "atlas"})

	err := cmd.Execute()
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorIs, errWriteFailed)
	c.Assert(err, qt.ErrorMatches, "checksum mismatch: failed to write checksum output: write checksum error: write failed")
	c.Assert(stdout.String(), qt.Equals, "You have a checksum error in your migration directory.\n"+
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
}

func TestValidate_AtlasMissingSumStdoutWriteFailure(t *testing.T) {
	c := qt.New(t)
	dir := atlasDirectoryWithoutSum(c.TB)
	cmd := migratevalidate.NewAtlasMigrateValidateCommand()
	var stderr bytes.Buffer
	cmd.SetOut(failingWriter{})
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--dir", dir, "--dir-format", "atlas"})

	err := cmd.Execute()

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorIs, errWriteFailed)
	c.Assert(err, qt.ErrorMatches, "checksum file not found: failed to write checksum output: write checksum guidance: write failed")
	c.Assert(stderr.String(), qt.Equals, "Error: checksum file not found\n")
}

func TestValidate_AtlasMissingSumStderrWriteFailure(t *testing.T) {
	c := qt.New(t)
	dir := atlasDirectoryWithoutSum(c.TB)
	cmd := migratevalidate.NewAtlasMigrateValidateCommand()
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(failingWriter{})
	cmd.SetArgs([]string{"--dir", dir, "--dir-format", "atlas"})

	err := cmd.Execute()

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorIs, errWriteFailed)
	c.Assert(err, qt.ErrorMatches, "checksum file not found: failed to write checksum output: write checksum error: write failed")
	c.Assert(stdout.String(), qt.Equals, "You have a checksum error in your migration directory.\n"+
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
}

func malformedAtlasDirectory(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()
	dir := atlasDirectoryWithoutSum(c.TB)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.sum"),
		[]byte("h1:tampered\n"), 0o600), qt.IsNil)
	return dir
}

func cleanAtlasDirectory(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()
	dir := atlasDirectoryWithoutSum(c.TB)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

func atlasDirectoryWithoutSum(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_initial.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	return dir
}

func duplicateAtlasSumDirectory(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_initial.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	sum, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	duplicate := []byte(sum.Entries[0].Name + " " + sum.Entries[0].Hash + "\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.sum"),
		append(sum.Bytes(), duplicate...), 0o600), qt.IsNil)
	return dir
}

type failingWriter struct{}

var errWriteFailed = errors.New("write failed")

func (failingWriter) Write(_ []byte) (int, error) {
	return 0, errWriteFailed
}
