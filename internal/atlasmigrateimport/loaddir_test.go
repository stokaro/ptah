package atlasmigrateimport_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
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
			// V1 and V2 land in the versioned band; see
			// TestLoadFSFlywayAtlasVersions for where the numbers come from.
			wantNames: []string{"4611686018427469511_init.sql", "4611686018427510315_add_users.sql"},
			wantData: map[string]string{
				"4611686018427469511_init.sql":      "CREATE TABLE fw_t (id int);\n",
				"4611686018427510315_add_users.sql": "CREATE TABLE fw_users (id int);\n",
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

func TestLoadFSLiquibaseDirectApplyKeepsNumberedFileIdentity(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"7_changelog.sql": &fstest.MapFile{Data: []byte(`--liquibase formatted sql
--changeset alice:first
CREATE TABLE first_table (id int);
--rollback DROP TABLE first_table;
--changeset bob:second
CREATE TABLE second_table (id int);
--rollback DROP TABLE second_table;
`)},
	}

	loaded, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

	c.Assert(err, qt.IsNil)
	c.Assert(entryNames(loaded), qt.DeepEquals, []string{"7_changelog.sql"})
	c.Assert(string(loaded.Entries[0].Data), qt.Contains, "--changeset alice:first")
	c.Assert(string(loaded.Entries[0].Data), qt.Contains, "--changeset bob:second")
	c.Assert(string(loaded.Entries[0].Data), qt.Not(qt.Contains), "--rollback")
}

func TestLoadFSLiquibaseDirectApplyStillRefusesConventionalName(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{
		"changelog.sql": &fstest.MapFile{Data: []byte(`--liquibase formatted sql
--changeset alice:first
CREATE TABLE first_table (id int);
`)},
	}

	_, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

	c.Assert(err, qt.ErrorMatches, `no importable migration files found in migrations for format "liquibase"`)
}

func TestLoadDir_FailurePath(t *testing.T) {
	c := qt.New(t)

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

func TestLoadDir_FlywayOrdersVersionsNumerically(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		files     map[string]string
		wantNames []string
	}{
		{
			name: "major.minor orders before next major",
			files: map[string]string{
				"V2__major.sql":   "CREATE TABLE major_step (id int);\n",
				"V1.5__minor.sql": "CREATE TABLE minor_step (id int);\n",
				"V10__later.sql":  "CREATE TABLE later_step (id int);\n",
			},
			// Flyway order 1.5 < 2 < 10, not the digit-concatenated 15, 2, 10
			// that would invert 1.5 and 2.
			wantNames: []string{
				"4611686018427471935_minor.sql",
				"4611686018427510315_major.sql",
				"4611686018427836747_later.sql",
			},
		},
		{
			name: "dotted differs from concatenated",
			files: map[string]string{
				"V2.0__a.sql": "CREATE TABLE a (id int);\n",
				"V20__b.sql":  "CREATE TABLE b (id int);\n",
			},
			// V2.0 and V20 are distinct versions; 2.0 < 20, so both apply in
			// that order rather than colliding.
			wantNames: []string{"4611686018427510719_a.sql", "4611686018428244787_b.sql"},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dir := c.TempDir()
			for name, content := range tt.files {
				writeLoadFile(c, dir, name, content)
			}

			loaded, err := atlasmigrateimport.LoadDir(dir, atlasmigrateimport.FormatFlyway)

			c.Assert(err, qt.IsNil)
			c.Assert(entryNames(loaded), qt.DeepEquals, tt.wantNames)
		})
	}
}

func TestLoadDir_DBMateStripsDirectiveOptions(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeLoadFile(c, dir, "1_index.sql", "-- migrate:up transaction:false\nCREATE INDEX idx ON t (id);\n-- migrate:down\nDROP INDEX idx;\n")

	loaded, err := atlasmigrateimport.LoadDir(dir, atlasmigrateimport.FormatDBMate)

	c.Assert(err, qt.IsNil)
	c.Assert(loaded.Entries, qt.HasLen, 1)
	// "transaction:false" is part of the directive line and must not leak into
	// the executable SQL.
	c.Assert(string(loaded.Entries[0].Data), qt.Equals, "CREATE INDEX idx ON t (id);\n")
}

func TestLoadDir_GoosePreservesStatementBlockAndDropsDown(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeLoadFile(c, dir, "1_fn.sql", "-- +goose Up\n-- +goose StatementBegin\nCREATE FUNCTION f() RETURNS int AS $$\nBEGIN\n  RETURN 1;\nEND;\n$$ LANGUAGE plpgsql;\n-- +goose StatementEnd\n-- +goose Down\nDROP FUNCTION f();\n")

	loaded, err := atlasmigrateimport.LoadDir(dir, atlasmigrateimport.FormatGoose)

	c.Assert(err, qt.IsNil)
	c.Assert(loaded.Entries, qt.HasLen, 1)
	got := string(loaded.Entries[0].Data)
	// The whole function body (including its internal semicolons) survives; the
	// StatementBegin/End markers are stripped and the down section is excluded.
	c.Assert(got, qt.Equals, "CREATE FUNCTION f() RETURNS int AS $$\nBEGIN\n  RETURN 1;\nEND;\n$$ LANGUAGE plpgsql;\n")
	c.Assert(got, qt.Not(qt.Contains), "DROP FUNCTION")
}

// TestLoadDir_FlywayConvertsThroughTheRealFilesystem exercises the shapes the
// convergence changed over an on-disk directory rather than a MapFS, so the
// capture in CaptureFS is in the loop: nested migrations are covered by a
// Flyway atlas.sum, and a snapshot that stopped at the top level would convert
// a smaller set than the sum it is verified against.
func TestLoadDir_FlywayConvertsThroughTheRealFilesystem(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(dir, "sub"), 0o750), qt.IsNil)
	writeLoadFile(c, dir, "V1.5__a.sql", "CREATE TABLE a (id int);\n")
	writeLoadFile(c, dir, "V1_5__b.sql", "CREATE TABLE b (id int);\n")
	writeLoadFile(c, dir, filepath.Join("sub", "V2__nested.sql"), "CREATE TABLE n (id int);\n")
	writeLoadFile(c, dir, "R__views.sql", "CREATE VIEW v AS SELECT 1;\n")

	loaded, err := atlasmigrateimport.LoadDir(dir, atlasmigrateimport.FormatFlyway)

	c.Assert(err, qt.IsNil)
	// V1.5 and V1_5 are distinct versions to Atlas CE and it runs both; the
	// previous encoding merged them and refused the pair. The repeatable runs
	// last, and the nested migration runs at all.
	c.Assert(entryNames(loaded), qt.DeepEquals, []string{
		"4611686018427471935_a.sql",
		"4611686018427471936_b.sql",
		"4611686018427510315_nested.sql",
		"9223372036854775807_views.sql",
	})
}

// TestLoadDir_RejectsMissingUpDirective covers dbmate only.
//
// It used to carry two goose rows — a file with no directives, and one whose
// only marker was the malformed "--+goose Up". stokaro/ptah#981 is those
// refusals being wrong: Atlas recognizes neither line as a directive, so both
// files simply have none, and it executes each one's bytes verbatim and records
// the revision. Measured on both fixtures. A file with no directives has no
// rollback section that could leak onto the apply path, so the caution that
// justifies refusing a BROKEN directive set does not reach them.
//
// Goose parsing — the directive-free path and the out-of-order refusals that
// replaced this over-refusal — is pinned in goosedirectives_test.go.
func TestLoadDir_RejectsMissingUpDirective(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		format  atlasmigrateimport.Format
		content string
	}{
		{
			name:    "dbmate without up directive",
			format:  atlasmigrateimport.FormatDBMate,
			content: "CREATE TABLE t (id int);\n",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dir := c.TempDir()
			writeLoadFile(c, dir, "1_x.sql", tt.content)

			loaded, err := atlasmigrateimport.LoadDir(dir, tt.format)

			c.Assert(err, qt.ErrorMatches, `migration file 1_x\.sql carries no "-- migrate:up" directive.*`)
			c.Assert(loaded, qt.IsNil)
		})
	}
}

func TestLoadDir_RejectsDuplicateConvertedVersion(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeLoadFile(c, dir, "1_a.sql", "-- +goose Up\nCREATE TABLE a (id int);\n")
	writeLoadFile(c, dir, "01_a.sql", "-- +goose Up\nCREATE TABLE b (id int);\n")

	loaded, err := atlasmigrateimport.LoadDir(dir, atlasmigrateimport.FormatGoose)

	c.Assert(err, qt.ErrorMatches, `migration files .* and .* map to the same version 1`)
	c.Assert(loaded, qt.IsNil)
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
