package migrationintegrity_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationintegrity"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

type editedCheckpointFixture struct {
	format   migrator.MigrationDirFormat
	editName string
	seed     func(*qt.C, string) string
	write    func(*qt.C, string, fs.FS) []string
}

func TestRefreshEditedCheckpointIntegrity(t *testing.T) {
	tests := map[string]editedCheckpointFixture{
		"ptah": {
			format:   migrator.MigrationDirFormatPtah,
			editName: "0000000002_snapshot.checkpoint.up.sql",
			seed: func(c *qt.C, dir string) string {
				path := filepath.Join(dir, "0000000001_init.up.sql")
				c.Assert(os.WriteFile(path, []byte("CREATE TABLE users (id INTEGER);\n"), 0o600), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE users;\n"), 0o600), qt.IsNil)
				return path
			},
			write: func(c *qt.C, dir string, authorized fs.FS) []string {
				up, down, err := generator.WriteCheckpointFilesWithOptions(
					dir,
					2,
					"snapshot",
					"CREATE TABLE users (id INTEGER);\n",
					"DROP TABLE users;\n",
					generator.CheckpointWriteOptions{AuthorizedMigrationsFS: authorized},
				)
				c.Assert(err, qt.IsNil)
				return []string{up, down}
			},
		},
		"atlas": {
			format:   migrator.MigrationDirFormatAtlas,
			editName: "2_snapshot.sql",
			seed: func(c *qt.C, dir string) string {
				path := filepath.Join(dir, "1_init.sql")
				c.Assert(os.WriteFile(path, []byte("CREATE TABLE users (id INTEGER);\n"), 0o600), qt.IsNil)
				return path
			},
			write: func(c *qt.C, dir string, authorized fs.FS) []string {
				path, err := generator.WriteAtlasCheckpointFileWithOptions(
					dir,
					2,
					"snapshot",
					"CREATE TABLE users (id INTEGER);\n",
					generator.CheckpointWriteOptions{AuthorizedMigrationsFS: authorized},
				)
				c.Assert(err, qt.IsNil)
				return []string{path}
			},
		},
	}

	for name, fixture := range tests {
		t.Run(name, func(t *testing.T) {
			t.Run("edited checkpoint only", func(t *testing.T) {
				c := qt.New(t)
				dir, authorized, _, paths := prepareCheckpoint(c, fixture)
				writer := openEditedCheckpointWriter(c, dir)
				authorization := authorizeCheckpointEdit(c, writer, fixture, authorized, paths)
				editCheckpoint(c, dir, fixture.editName)

				err := migrationintegrity.RefreshEditedCheckpointIntegrity(
					t.Context(), writer, authorization,
				)

				c.Assert(err, qt.IsNil)
				assertMigrationIntegrity(c, dir, fixture.format, true)
			})

			t.Run("concurrent prior history change", func(t *testing.T) {
				c := qt.New(t)
				dir, authorized, priorPath, paths := prepareCheckpoint(c, fixture)
				writer := openEditedCheckpointWriter(c, dir)
				authorization := authorizeCheckpointEdit(c, writer, fixture, authorized, paths)
				editCheckpoint(c, dir, fixture.editName)
				c.Assert(os.WriteFile(priorPath, []byte("CREATE TABLE attacker (id INTEGER);\n"), 0o600), qt.IsNil)

				err := migrationintegrity.RefreshEditedCheckpointIntegrity(
					t.Context(), writer, authorization,
				)

				c.Assert(err, qt.ErrorIs, migrationintegrity.ErrAuthorizedHistoryChanged)
				assertMigrationIntegrity(c, dir, fixture.format, false)
			})

			t.Run("concurrent integrity metadata change", func(t *testing.T) {
				c := qt.New(t)
				dir, authorized, _, paths := prepareCheckpoint(c, fixture)
				writer := openEditedCheckpointWriter(c, dir)
				authorization := authorizeCheckpointEdit(c, writer, fixture, authorized, paths)
				editCheckpoint(c, dir, fixture.editName)
				sumName, err := migratesum.FileNameForFormat(fixture.format)
				c.Assert(err, qt.IsNil)
				sum, err := os.ReadFile(filepath.Join(dir, sumName))
				c.Assert(err, qt.IsNil)
				c.Assert(len(sum) > 3, qt.IsTrue)
				const base64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
				index := strings.IndexByte(base64Alphabet, sum[3])
				c.Assert(index >= 0, qt.IsTrue)
				sum[3] = base64Alphabet[(index+1)%len(base64Alphabet)]
				writeRootedFile(c, dir, sumName, sum)

				err = migrationintegrity.RefreshEditedCheckpointIntegrity(
					t.Context(), writer, authorization,
				)

				c.Assert(err, qt.ErrorIs, migrationintegrity.ErrAuthorizedHistoryChanged)
				assertMigrationIntegrity(c, dir, fixture.format, false)
			})

			t.Run("different writer", func(t *testing.T) {
				c := qt.New(t)
				dir, authorized, _, paths := prepareCheckpoint(c, fixture)
				writer := openEditedCheckpointWriter(c, dir)
				authorization := authorizeCheckpointEdit(c, writer, fixture, authorized, paths)
				editCheckpoint(c, dir, fixture.editName)
				otherWriter := openEditedCheckpointWriter(c, dir)

				err := migrationintegrity.RefreshEditedCheckpointIntegrity(
					t.Context(), otherWriter, authorization,
				)

				c.Assert(err, qt.ErrorMatches, "checkpoint edit authorization belongs to a different migration writer")
				assertMigrationIntegrity(c, dir, fixture.format, false)
			})
		})
	}
}

func prepareCheckpoint(
	c *qt.C,
	fixture editedCheckpointFixture,
) (string, fs.FS, string, []string) {
	c.Helper()
	dir := c.TempDir()
	priorPath := fixture.seed(c, dir)
	_, err := migratesum.WriteWithFormat(dir, fixture.format)
	c.Assert(err, qt.IsNil)
	authorized, err := migrationsnapshot.CaptureDirectory(dir)
	c.Assert(err, qt.IsNil)
	paths := fixture.write(c, dir, authorized)
	editPath := filepath.Join(dir, fixture.editName)
	c.Assert(paths[0], qt.Equals, editPath)
	return dir, authorized, priorPath, paths
}

func authorizeCheckpointEdit(
	c *qt.C,
	writer *atlasmigrate.MigrationWriter,
	fixture editedCheckpointFixture,
	authorized fs.FS,
	paths []string,
) migrationintegrity.CheckpointEditAuthorization {
	c.Helper()
	authorization, err := migrationintegrity.AuthorizeCheckpointEdit(
		c.Context(), writer, fixture.format, authorized, paths...,
	)
	c.Assert(err, qt.IsNil)
	return authorization
}

func editCheckpoint(c *qt.C, dir, name string) {
	c.Helper()
	editPath := filepath.Join(dir, name)
	contents, err := os.ReadFile(editPath)
	c.Assert(err, qt.IsNil)
	contents = append(contents, []byte("-- edited checkpoint\n")...)
	writeRootedFile(c, dir, name, contents)
}

func writeRootedFile(c *qt.C, dir, name string, contents []byte) {
	c.Helper()
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_TRUNC, 0)
	c.Assert(err, qt.IsNil)
	written, err := file.Write(contents)
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.Equals, len(contents))
	c.Assert(file.Close(), qt.IsNil)
	c.Assert(root.Close(), qt.IsNil)
}

func openEditedCheckpointWriter(c *qt.C, dir string) *atlasmigrate.MigrationWriter {
	c.Helper()
	writer, err := atlasmigrate.OpenMigrationWriter(nil, dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(writer.Close(), qt.IsNil) })
	return writer
}

func assertMigrationIntegrity(
	c *qt.C,
	dir string,
	format migrator.MigrationDirFormat,
	wantOK bool,
) {
	c.Helper()
	result, hashed, err := migratesum.VerifyHashed(os.DirFS(dir), format)
	c.Assert(err, qt.IsNil)
	c.Assert(hashed, qt.IsTrue)
	c.Check(result.OK(), qt.Equals, wantOK, qt.Commentf("%s", result.Describe()))
}
