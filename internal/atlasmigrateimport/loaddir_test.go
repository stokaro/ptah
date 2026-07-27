package atlasmigrateimport_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasmigrateimport"
)

func TestLoadDir_ConvertsEachFormatToUpOnlyEntries(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		format    atlasmigrateimport.Format
		files     map[string]string
		wantNames []string
		wantData  map[string]string
	}{
		{
			name:   "atlas passthrough",
			format: atlasmigrateimport.FormatAtlas,
			files: map[string]string{
				"1_init.sql": "CREATE TABLE atlas_t (id int);\n",
				"2_next.sql": "CREATE TABLE atlas_t2 (id int);\n",
			},
			wantNames: []string{"1_init.sql", "2_next.sql"},
			wantData: map[string]string{
				"1_init.sql": "CREATE TABLE atlas_t (id int);\n",
				"2_next.sql": "CREATE TABLE atlas_t2 (id int);\n",
			},
		},
		{
			name:   "golang-migrate keeps up files",
			format: atlasmigrateimport.FormatGolangMigrate,
			files: map[string]string{
				"1_init.up.sql":   "CREATE TABLE gm_t (id int);\n",
				"1_init.down.sql": "DROP TABLE gm_t;\n",
				"2_next.up.sql":   "ALTER TABLE gm_t ADD name text;\n",
			},
			wantNames: []string{"1_init.sql", "2_next.sql"},
			wantData: map[string]string{
				"1_init.sql": "CREATE TABLE gm_t (id int);\n",
				"2_next.sql": "ALTER TABLE gm_t ADD name text;\n",
			},
		},
		{
			name:   "goose keeps up section",
			format: atlasmigrateimport.FormatGoose,
			files: map[string]string{
				"1_init.sql": "-- +goose Up\nCREATE TABLE goose_t (id int);\n-- +goose Down\nDROP TABLE goose_t;\n",
			},
			wantNames: []string{"1_init.sql"},
			wantData:  map[string]string{"1_init.sql": "CREATE TABLE goose_t (id int);\n"},
		},
		{
			name:   "dbmate keeps up section",
			format: atlasmigrateimport.FormatDBMate,
			files: map[string]string{
				"1_init.sql": "-- migrate:up\nCREATE TABLE dbmate_t (id int);\n-- migrate:down\nDROP TABLE dbmate_t;\n",
			},
			wantNames: []string{"1_init.sql"},
			wantData:  map[string]string{"1_init.sql": "CREATE TABLE dbmate_t (id int);\n"},
		},
		{
			name:   "liquibase drops rollback",
			format: atlasmigrateimport.FormatLiquibase,
			files: map[string]string{
				"1_init.sql": "--liquibase formatted sql\n--changeset a:1\nCREATE TABLE lb_t (id int);\n--rollback DROP TABLE lb_t;\n",
			},
			wantNames: []string{"1_init.sql"},
			wantData:  map[string]string{"1_init.sql": "--changeset a:1\nCREATE TABLE lb_t (id int);\n"},
		},
		{
			name:   "flyway versioned only",
			format: atlasmigrateimport.FormatFlyway,
			files: map[string]string{
				"V1__init.sql":      "CREATE TABLE fw_t (id int);\n",
				"U1__init.sql":      "DROP TABLE fw_t;\n",
				"V2__add_users.sql": "CREATE TABLE fw_users (id int);\n",
			},
			wantNames: []string{"1_init.sql", "2_add_users.sql"},
			wantData: map[string]string{
				"1_init.sql":      "CREATE TABLE fw_t (id int);\n",
				"2_add_users.sql": "CREATE TABLE fw_users (id int);\n",
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dir := c.TempDir()
			for name, content := range tt.files {
				writeLoadFile(c, dir, name, content)
			}

			loaded, err := atlasmigrateimport.LoadDir(dir, tt.format)

			c.Assert(err, qt.IsNil)
			c.Assert(loaded.Format, qt.Equals, tt.format)
			c.Assert(entryNames(loaded), qt.DeepEquals, tt.wantNames)
			for _, entry := range loaded.Entries {
				c.Assert(string(entry.Data), qt.Equals, tt.wantData[entry.Name])
			}
		})
	}
}

func TestLoadDir_FSConformsToStdlibAndOmitsAtlasSum(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeLoadFile(c, dir, "1_init.sql", "-- +goose Up\nCREATE TABLE fs_t (id int);\n-- +goose Down\nDROP TABLE fs_t;\n")
	writeLoadFile(c, dir, "2_next.sql", "-- +goose Up\nALTER TABLE fs_t ADD name text;\n-- +goose Down\nALTER TABLE fs_t DROP COLUMN name;\n")

	loaded, err := atlasmigrateimport.LoadDir(dir, atlasmigrateimport.FormatGoose)
	c.Assert(err, qt.IsNil)

	fsys := loaded.FS()
	c.Assert(fstest.TestFS(fsys, "1_init.sql", "2_next.sql"), qt.IsNil)

	// External formats carry no Atlas integrity file, so the converted
	// filesystem must not fabricate one.
	_, statErr := fs.Stat(fsys, "atlas.sum")
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)

	up, err := fs.ReadFile(fsys, "1_init.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Equals, "CREATE TABLE fs_t (id int);\n")
}

func TestLoadDir_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("flyway repeatable migration", func(c *qt.C) {
		dir := c.TempDir()
		writeLoadFile(c, dir, "V1__init.sql", "CREATE TABLE fw (id int);\n")
		writeLoadFile(c, dir, "R__views.sql", "CREATE VIEW v AS SELECT 1;\n")

		loaded, err := atlasmigrateimport.LoadDir(dir, atlasmigrateimport.FormatFlyway)

		c.Assert(err, qt.ErrorMatches, `Flyway repeatable migration R__views\.sql cannot be imported yet because Ptah does not execute Atlas R-suffixed migrations`)
		c.Assert(loaded, qt.IsNil)
	})

	c.Run("empty directory", func(c *qt.C) {
		loaded, err := atlasmigrateimport.LoadDir(c.TempDir(), atlasmigrateimport.FormatGoose)

		c.Assert(err, qt.ErrorMatches, `no importable migration files found in .* for format "goose"`)
		c.Assert(loaded, qt.IsNil)
	})

	c.Run("missing directory", func(c *qt.C) {
		loaded, err := atlasmigrateimport.LoadDir(filepath.Join(c.TempDir(), "does-not-exist"), atlasmigrateimport.FormatGoose)

		c.Assert(err, qt.ErrorMatches, `read source migration directory .*: .*`)
		c.Assert(loaded, qt.IsNil)
	})
}

func writeLoadFile(c *qt.C, dir, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

func entryNames(loaded *atlasmigrateimport.Loaded) []string {
	names := make([]string, 0, len(loaded.Entries))
	for _, entry := range loaded.Entries {
		names = append(names, entry.Name)
	}
	return names
}
