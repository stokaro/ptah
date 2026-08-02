package atlasmigrate_test

import (
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/fsnapshot"
)

func TestResolveApplyDir_AtlasFormatReadsDirectoryUnchanged(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name       string
		configured string
		query      url.Values
	}{
		{
			name: "default",
		},
		{
			name:       "configured Atlas",
			configured: "atlas",
		},
		{
			name:       "Atlas URL query overrides configured",
			configured: "goose",
			query:      url.Values{"format": []string{"atlas"}},
		},
		{
			name:       "empty URL format selects Atlas",
			configured: "goose",
			query:      url.Values{"format": []string{""}},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dir := c.TempDir()
			writeFormatFile(c, dir, "1_init.sql", "CREATE TABLE atlas_unchanged (id INTEGER PRIMARY KEY);\n")
			writeFormatFile(c, dir, "1_init.down.sql", "DROP TABLE atlas_unchanged;\n")

			gotFS, err := resolveApplySource(
				os.DirFS(dir),
				dir,
				tt.configured,
				tt.query,
			)

			c.Assert(err, qt.IsNil)
			writeFormatFile(c, dir, "1_init.sql", "CREATE TABLE changed_after_capture (id INTEGER PRIMARY KEY);\n")
			// The Atlas snapshot preserves both the byte-for-byte up file and
			// the accompanying down file after the source changes.
			c.Assert(readFSFile(c, gotFS, "1_init.sql"), qt.Equals, "CREATE TABLE atlas_unchanged (id INTEGER PRIMARY KEY);\n")
			c.Assert(readFSFile(c, gotFS, "1_init.down.sql"), qt.Equals, "DROP TABLE atlas_unchanged;\n")
		})
	}
}

func TestResolveApplyDir_ConvertsExternalFormatsToUpOnly(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name       string
		configured string
		query      url.Values
		file       string
		source     string
		wantFile   string
		wantSQL    string
	}{
		{
			name:       "goose keeps only up section",
			configured: "goose",
			file:       "1_init.sql",
			source:     "-- +goose Up\nCREATE TABLE goose_up (id int);\n-- +goose Down\nDROP TABLE goose_up;\n",
			wantFile:   "1_init.sql",
			wantSQL:    "CREATE TABLE goose_up (id int);\n",
		},
		{
			name:       "dbmate keeps only up section",
			configured: "dbmate",
			file:       "1_init.sql",
			source:     "-- migrate:up\nCREATE TABLE dbmate_up (id int);\n-- migrate:down\nDROP TABLE dbmate_up;\n",
			wantFile:   "1_init.sql",
			wantSQL:    "CREATE TABLE dbmate_up (id int);\n",
		},
		{
			name:       "liquibase drops rollback directives",
			configured: "liquibase",
			file:       "1_init.sql",
			source:     "--liquibase formatted sql\n\n--changeset atlas:1-1\nCREATE TABLE liquibase_up (id int);\n--rollback DROP TABLE liquibase_up;\n",
			wantFile:   "1_init.sql",
			wantSQL:    "--changeset atlas:1-1\nCREATE TABLE liquibase_up (id int);\n",
		},
		{
			name:       "golang-migrate uses up file",
			configured: "golang-migrate",
			file:       "1_init.up.sql",
			source:     "CREATE TABLE golang_migrate_up (id int);\n",
			wantFile:   "1_init.sql",
			wantSQL:    "CREATE TABLE golang_migrate_up (id int);\n",
		},
		{
			name:       "flyway versioned migration",
			configured: "flyway",
			file:       "V1__init.sql",
			source:     "CREATE TABLE flyway_up (id int);\n",
			// V1 lands in the versioned band; see
			// atlasmigrateimport.TestLoadFSFlywayAtlasVersions.
			wantFile: "4611686018427469511_init.sql",
			wantSQL:  "CREATE TABLE flyway_up (id int);\n",
		},
		{
			name:       "URL format overrides configured atlas default",
			configured: "atlas",
			query:      url.Values{"format": []string{"goose"}},
			file:       "1_init.sql",
			source:     "-- +goose Up\nCREATE TABLE goose_url (id int);\n-- +goose Down\nDROP TABLE goose_url;\n",
			wantFile:   "1_init.sql",
			wantSQL:    "CREATE TABLE goose_url (id int);\n",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dir := c.TempDir()
			writeFormatFile(c, dir, tt.file, tt.source)

			gotFS, err := resolveApplySource(
				os.DirFS(dir),
				dir,
				tt.configured,
				tt.query,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(readFSFile(c, gotFS, tt.wantFile), qt.Equals, tt.wantSQL)
			// The original source file name is not carried into the converted
			// filesystem when it differs from the Atlas single-file name.
			c.Assert(fsFileNames(c, gotFS), qt.DeepEquals, []string{tt.wantFile})
		})
	}
}

func TestResolveApplyDir_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name       string
		configured string
		query      url.Values
		want       string
	}{
		{
			name:       "unknown configured format",
			configured: "custom",
			want:       `unknown Atlas migration directory format "custom": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`,
		},
		{
			name:       "case-sensitive format",
			configured: "ATLAS",
			want:       `unknown Atlas migration directory format "ATLAS": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`,
		},
		{
			name:       "configured format whitespace is significant",
			configured: " atlas ",
			want:       `unknown Atlas migration directory format " atlas ": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`,
		},
		{
			name:       "URL format overrides configured format",
			configured: "atlas",
			query:      url.Values{"format": []string{"custom"}},
			want:       `unknown Atlas migration directory format "custom": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`,
		},
		{
			name:       "URL format whitespace is significant",
			configured: "atlas",
			query:      url.Values{"format": []string{" atlas "}},
			want:       `unknown Atlas migration directory format " atlas ": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`,
		},
		{
			name:       "unknown query parameter",
			configured: "atlas",
			query:      url.Values{"version": []string{"1"}},
			want:       `unsupported migration directory URL query parameter "version"`,
		},
		{
			name:       "unknown query parameters are reported deterministically",
			configured: "atlas",
			query:      url.Values{"version": []string{"1"}, "checksum": []string{"required"}},
			want:       `unsupported migration directory URL query parameter "checksum"`,
		},
		{
			name:       "multiple format query parameters",
			configured: "atlas",
			query:      url.Values{"format": []string{"atlas", "goose"}},
			want:       "migration directory URL contains multiple format parameters",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dir := c.TempDir()
			gotFS, err := resolveApplySource(
				os.DirFS(dir),
				dir,
				tt.configured,
				tt.query,
			)

			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(gotFS, qt.DeepEquals, fsnapshot.Snapshot{})
		})
	}
}

func TestResolveApplyDir_RejectsUnexecutableAndEmptyDirectories(t *testing.T) {
	c := qt.New(t)

	c.Run("flyway migrations sharing one Atlas version", func(c *qt.C) {
		dir := c.TempDir()
		writeFormatFile(c, dir, "V1__a.sql", "CREATE TABLE a (id int);\n")
		writeFormatFile(c, dir, "V1__b.sql", "CREATE TABLE b (id int);\n")

		gotFS, err := resolveApplySource(os.DirFS(dir), dir, "flyway", nil)

		c.Assert(err, qt.ErrorMatches, `Flyway migrations V1__a\.sql and V1__b\.sql both carry the Atlas version "1"`)
		c.Assert(gotFS, qt.DeepEquals, fsnapshot.Snapshot{})
	})

	c.Run("empty external directory", func(c *qt.C) {
		dir := c.TempDir()
		gotFS, err := resolveApplySource(os.DirFS(dir), dir, "goose", nil)

		c.Assert(err, qt.ErrorMatches, `no importable migration files found in .* for format "goose"`)
		c.Assert(gotFS, qt.DeepEquals, fsnapshot.Snapshot{})
	})

	c.Run("Go-based Goose migration", func(c *qt.C) {
		dir := c.TempDir()
		writeFormatFile(c, dir, "1_init.sql", "-- +goose Up\nCREATE TABLE users (id int);\n")
		writeFormatFile(c, dir, "2_seed.go", "package migrations\n")

		gotFS, err := resolveApplySource(os.DirFS(dir), dir, "goose", nil)

		c.Assert(err, qt.ErrorMatches, `Go-based Goose migration "2_seed\.go" is not supported \(SQL migrations only\)`)
		c.Assert(gotFS, qt.DeepEquals, fsnapshot.Snapshot{})
	})

	c.Run("Liquibase XML changelog", func(c *qt.C) {
		dir := c.TempDir()
		writeFormatFile(c, dir, "1_init.sql", "--liquibase formatted sql\n--changeset ptah:1\nCREATE TABLE users (id int);\n")
		writeFormatFile(c, dir, "changelog.xml", "<databaseChangeLog></databaseChangeLog>\n")

		gotFS, err := resolveApplySource(os.DirFS(dir), dir, "liquibase", nil)

		c.Assert(err, qt.ErrorMatches, `liquibase XML/YAML/JSON changelogs are not yet supported .* found changelog\.xml`)
		c.Assert(gotFS, qt.DeepEquals, fsnapshot.Snapshot{})
	})
}

// applyDirFormatCases are the (configured, query) combinations the apply path
// resolves, with the format each selects.
func applyDirFormatCases() []struct {
	name       string
	configured string
	query      url.Values
	format     string
} {
	return []struct {
		name       string
		configured string
		query      url.Values
		format     string
	}{
		{name: "default", format: "atlas"},
		{name: "configured Atlas", configured: "atlas", format: "atlas"},
		{name: "empty URL format selects Atlas", configured: "goose", query: url.Values{"format": []string{""}}, format: "atlas"},
		{name: "URL query overrides configured to Atlas", configured: "goose", query: url.Values{"format": []string{"atlas"}}, format: "atlas"},
		{name: "configured goose", configured: "goose", format: "goose"},
		{name: "configured flyway", configured: "flyway", format: "flyway"},
		{name: "configured liquibase", configured: "liquibase", format: "liquibase"},
		{name: "configured dbmate", configured: "dbmate", format: "dbmate"},
		{name: "configured golang-migrate", configured: "golang-migrate", format: "golang-migrate"},
		{name: "URL query overrides Atlas to goose", query: url.Values{"format": []string{"goose"}}, format: "goose"},
	}
}

// TestResolveApplyDirFormat covers the single format resolution the apply path
// makes (stokaro/ptah#970): the executed filesystem and the integrity gate both
// consume this one value.
func TestResolveApplyDirFormat(t *testing.T) {
	c := qt.New(t)

	for _, tt := range applyDirFormatCases() {
		c.Run(tt.name, func(c *qt.C) {
			got, err := atlasmigrate.ResolveApplyDirFormat(tt.configured, tt.query)

			c.Assert(err, qt.IsNil)
			c.Assert(string(got), qt.Equals, tt.format)
			// The gate keys on this same value: only the native Atlas format
			// reads a directory that can carry atlas.sum.
			c.Assert(atlasmigrate.ReadsNativeAtlasDir(got), qt.Equals, tt.format == "atlas")
		})
	}

	c.Run("unknown format reports the resolve error", func(c *qt.C) {
		got, err := atlasmigrate.ResolveApplyDirFormat("sqitch", nil)

		c.Assert(err, qt.ErrorMatches, `unknown Atlas migration directory format "sqitch".*`)
		c.Assert(string(got), qt.Equals, "")
	})
}

// TestResolveApplySourceForFormatReadsEachFormat pins what each resolved format
// makes the apply migrator read.
//
// It does NOT prove the apply command wires one resolution into both the loader
// and the integrity gate — a duplicated-but-agreeing computation is invisible
// to any test, because the resolver is pure and both calls would take the same
// inputs. Two other things carry that: verifyAtlasApplyChecksum takes an
// already-resolved format rather than the raw --dir string and query, so it
// cannot re-resolve without a visible signature change; and
// TestCompatMigrateApply_ConvertedDirStaysUngated_KnownDivergence goes red if
// the gate ever stops seeing the ?format= override (it would then verify
// atlas.sum against a converted filesystem that has none and refuse).
func TestResolveApplySourceForFormatReadsEachFormat(t *testing.T) {
	c := qt.New(t)

	for _, tt := range applyDirFormatCases() {
		c.Run(tt.name, func(c *qt.C) {
			dir := c.TempDir()
			writeSeamFixture(c, dir, tt.format)

			format, err := atlasmigrate.ResolveApplyDirFormat(tt.configured, tt.query)
			c.Assert(err, qt.IsNil)

			got, err := atlasmigrate.ResolveApplySourceForFormat(os.DirFS(dir), dir, format)

			c.Assert(err, qt.IsNil)
			names := fsFileNames(c, got)
			c.Assert(len(names) > 0, qt.IsTrue)
			// Every format is rebuilt as up-only Atlas migrations, so a
			// golang-migrate down file never survives into what gets executed.
			// No other fixture writes that name, so the assertion is trivially
			// true elsewhere and load-bearing for golang-migrate.
			c.Assert(names, qt.Not(qt.Contains), "1_init.down.sql")
			for _, name := range names {
				c.Assert(readFSFile(c, got, name), qt.Not(qt.Equals), "")
			}
		})
	}
}

// resolveApplySource performs the two-step resolution the apply command
// performs: resolve the directory format once, then load the source for that
// format. Tests go through it so they exercise the production wiring rather
// than a convenience wrapper that production does not use.
func resolveApplySource(
	source fs.FS,
	display string,
	configured string,
	query url.Values,
) (fsnapshot.Snapshot, error) {
	format, err := atlasmigrate.ResolveApplyDirFormat(configured, query)
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	return atlasmigrate.ResolveApplySourceForFormat(source, display, format)
}

// writeSeamFixture writes the minimal directory the named format can read.
func writeSeamFixture(c *qt.C, dir, format string) {
	c.Helper()
	switch format {
	case "goose":
		writeFormatFile(c, dir, "1_init.sql", "-- +goose Up\nCREATE TABLE seam (id int);\n")
	case "dbmate":
		writeFormatFile(c, dir, "1_init.sql", "-- migrate:up\nCREATE TABLE seam (id int);\n")
	case "liquibase":
		writeFormatFile(c, dir, "1_init.sql", "--liquibase formatted sql\n--changeset app:1\nCREATE TABLE seam (id int);\n")
	case "flyway":
		writeFormatFile(c, dir, "V1__init.sql", "CREATE TABLE seam (id int);\n")
	case "golang-migrate":
		writeFormatFile(c, dir, "1_init.up.sql", "CREATE TABLE seam (id int);\n")
		writeFormatFile(c, dir, "1_init.down.sql", "DROP TABLE seam;\n")
	default:
		writeFormatFile(c, dir, "1_init.sql", "CREATE TABLE seam (id int);\n")
	}
}

func writeFormatFile(c *qt.C, dir, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

func readFSFile(c *qt.C, fsys fs.FS, name string) string {
	c.Helper()
	data, err := fs.ReadFile(fsys, name)
	c.Assert(err, qt.IsNil)
	return string(data)
}

func fsFileNames(c *qt.C, fsys fs.FS) []string {
	c.Helper()
	entries, err := fs.ReadDir(fsys, ".")
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}
