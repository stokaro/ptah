package migratesum_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

func writeUnhashedPtahDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_users.up.sql"), []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_users.down.sql"), []byte("DROP TABLE users;\n"), 0o600), qt.IsNil)
	return dir
}

func writeUnhashedAtlasDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_users.sql"), []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	return dir
}

func hashDir(c *qt.C, dir string, format migrator.MigrationDirFormat) string {
	c.Helper()
	_, err := migratesum.WriteWithFormat(dir, format)
	c.Assert(err, qt.IsNil)
	return dir
}

func writeHashedPtahDir(c *qt.C) string {
	c.Helper()
	return hashDir(c, writeUnhashedPtahDir(c), migrator.MigrationDirFormatPtah)
}

func writeHashedAtlasDir(c *qt.C) string {
	c.Helper()
	return hashDir(c, writeUnhashedAtlasDir(c), migrator.MigrationDirFormatAtlas)
}

func TestVerifyHashed_UnhashedDirectorySkipsVerification(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		dir    string
		format migrator.MigrationDirFormat
	}{
		{name: "ptah dir auto format", dir: writeUnhashedPtahDir(c), format: migrator.MigrationDirFormatAuto},
		{name: "ptah dir explicit format", dir: writeUnhashedPtahDir(c), format: migrator.MigrationDirFormatPtah},
		{name: "atlas dir auto format", dir: writeUnhashedAtlasDir(c), format: migrator.MigrationDirFormatAuto},
		{name: "atlas dir explicit format", dir: writeUnhashedAtlasDir(c), format: migrator.MigrationDirFormatAtlas},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			result, hashed, err := migratesum.VerifyHashed(os.DirFS(test.dir), test.format)
			c.Assert(err, qt.IsNil)
			c.Assert(hashed, qt.IsFalse)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestVerifyHashed_HashedDirectoryVerifies(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name        string
		dir         string
		format      migrator.MigrationDirFormat
		sumFileName string
	}{
		{name: "ptah.sum auto format", dir: writeHashedPtahDir(c), format: migrator.MigrationDirFormatAuto, sumFileName: migratesum.FileName},
		{name: "ptah.sum explicit format", dir: writeHashedPtahDir(c), format: migrator.MigrationDirFormatPtah, sumFileName: migratesum.FileName},
		{name: "atlas.sum auto format", dir: writeHashedAtlasDir(c), format: migrator.MigrationDirFormatAuto, sumFileName: migratesum.AtlasFileName},
		{name: "atlas.sum explicit format", dir: writeHashedAtlasDir(c), format: migrator.MigrationDirFormatAtlas, sumFileName: migratesum.AtlasFileName},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			result, hashed, err := migratesum.VerifyHashed(os.DirFS(test.dir), test.format)
			c.Assert(err, qt.IsNil)
			c.Assert(hashed, qt.IsTrue)
			c.Assert(result.OK(), qt.IsTrue)
			c.Assert(result.SumFileName, qt.Equals, test.sumFileName)
		})
	}
}

func TestVerifyHashed_TamperedDirectoryReportsDrift(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedPtahDir(c)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_users.up.sql"), []byte("CREATE TABLE tampered (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	result, hashed, err := migratesum.VerifyHashed(os.DirFS(dir), migrator.MigrationDirFormatAuto)

	c.Assert(err, qt.IsNil)
	c.Assert(hashed, qt.IsTrue)
	c.Assert(result.OK(), qt.IsFalse)
	c.Assert(result.Changed, qt.DeepEquals, []string{"0000000001_users.up.sql"})
}

func TestVerifyHashed_ExplicitFormatIgnoresOtherSumFile(t *testing.T) {
	c := qt.New(t)
	// The directory carries only ptah.sum; asked explicitly about the Atlas
	// integrity file, it counts as unhashed.
	dir := writeHashedPtahDir(c)

	result, hashed, err := migratesum.VerifyHashed(os.DirFS(dir), migrator.MigrationDirFormatAtlas)

	c.Assert(err, qt.IsNil)
	c.Assert(hashed, qt.IsFalse)
	c.Assert(result, qt.IsNil)
}

func TestVerifyHashed_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("both sum files in auto mode are ambiguous", func(c *qt.C) {
		dir := writeHashedPtahDir(c)
		c.Assert(os.WriteFile(filepath.Join(dir, migratesum.AtlasFileName), []byte("h1:bogus=\n"), 0o600), qt.IsNil)

		_, hashed, err := migratesum.VerifyHashed(os.DirFS(dir), migrator.MigrationDirFormatAuto)

		c.Assert(hashed, qt.IsTrue)
		c.Assert(err, qt.ErrorMatches, "both ptah.sum and atlas.sum exist.*")
	})

	c.Run("malformed sum file is an error", func(c *qt.C) {
		dir := writeUnhashedAtlasDir(c)
		c.Assert(os.WriteFile(filepath.Join(dir, migratesum.AtlasFileName), []byte("not a sum file"), 0o600), qt.IsNil)

		_, hashed, err := migratesum.VerifyHashed(os.DirFS(dir), migrator.MigrationDirFormatAtlas)

		c.Assert(hashed, qt.IsTrue)
		c.Assert(err, qt.ErrorIs, migratesum.ErrSumFileMalformed)
	})

	c.Run("unknown format is rejected", func(c *qt.C) {
		dir := writeHashedPtahDir(c)

		_, hashed, err := migratesum.VerifyHashed(os.DirFS(dir), migrator.MigrationDirFormat("bogus"))

		c.Assert(hashed, qt.IsFalse)
		c.Assert(err, qt.ErrorMatches, `unknown migration directory format "bogus".*`)
	})
}
