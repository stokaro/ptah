package atlasmigrateimport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// skeletonSeedFile is the pre-existing migration each layout's fixture starts
// from, so the round-trip test below can tell "the new file is loadable" from
// "the directory happened to load".
var skeletonSeedFile = map[atlasmigrateimport.Format]struct {
	name    string
	content string
}{
	atlasmigrateimport.FormatGolangMigrate: {"1_init.up.sql", "CREATE TABLE s1 (id INTEGER);\n"},
	atlasmigrateimport.FormatFlyway:        {"V1__init.sql", "CREATE TABLE s2 (id INTEGER);\n"},
	atlasmigrateimport.FormatGoose:         {"1_init.sql", "-- +goose Up\nCREATE TABLE s3 (id INTEGER);\n"},
	atlasmigrateimport.FormatDBMate:        {"1_init.sql", "-- migrate:up\nCREATE TABLE s4 (id INTEGER);\n"},
	atlasmigrateimport.FormatLiquibase: {
		"1_init.sql",
		"--liquibase formatted sql\n--changeset a:1\nCREATE TABLE s5 (id INTEGER);\n",
	},
}

func skeletonFormats() []atlasmigrateimport.Format {
	return []atlasmigrateimport.Format{
		atlasmigrateimport.FormatGolangMigrate,
		atlasmigrateimport.FormatFlyway,
		atlasmigrateimport.FormatGoose,
		atlasmigrateimport.FormatDBMate,
		atlasmigrateimport.FormatLiquibase,
	}
}

// TestSkeletonFilesMatchTheOracleLayout pins the names and bytes measured from
// the pinned community binary v1.3.0 on 2026-08-06, so a change to either shows
// up here rather than in a directory an operator cannot read back.
//
// The rows separate the layout axis: goose, dbmate and liquibase share one file
// NAME and differ only in bytes, while golang-migrate and flyway differ in name
// and in file count. A table that carried only the first three could not tell
// the name rule from the Atlas one.
//
// Reverted — that is, with the emitter removed — this file does not compile.
// With a single row's bytes changed, that row prints the wanted and got
// []SkeletonFile side by side.
func TestSkeletonFilesMatchTheOracleLayout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		format atlasmigrateimport.Format
		want   []atlasmigrateimport.SkeletonFile
	}{
		{
			name:   "golang_migrate_writes_a_pair_up_first",
			format: atlasmigrateimport.FormatGolangMigrate,
			want: []atlasmigrateimport.SkeletonFile{
				{Name: "20260806071434_addcol.up.sql"},
				{Name: "20260806071434_addcol.down.sql"},
			},
		},
		{
			name:   "flyway_writes_a_versioned_and_an_undo_file",
			format: atlasmigrateimport.FormatFlyway,
			want: []atlasmigrateimport.SkeletonFile{
				{Name: "V20260806071434__addcol.sql"},
				{Name: "U20260806071434__addcol.sql"},
			},
		},
		{
			name:   "goose_writes_its_directive_pair",
			format: atlasmigrateimport.FormatGoose,
			want: []atlasmigrateimport.SkeletonFile{
				{Name: "20260806071434_addcol.sql", Content: "-- +goose Up\n\n-- +goose Down\n"},
			},
		},
		{
			name:   "dbmate_writes_its_directive_pair",
			format: atlasmigrateimport.FormatDBMate,
			want: []atlasmigrateimport.SkeletonFile{
				{Name: "20260806071434_addcol.sql", Content: "-- migrate:up\n\n-- migrate:down\n"},
			},
		},
		{
			name:   "liquibase_writes_its_header_without_a_trailing_newline",
			format: atlasmigrateimport.FormatLiquibase,
			want: []atlasmigrateimport.SkeletonFile{
				{Name: "20260806071434_addcol.sql", Content: "--liquibase formatted sql"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			got, err := atlasmigrateimport.SkeletonFiles(tt.format, 20260806071434, "addcol")

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestSkeletonFilesAreCoveredAndLoadable is the reason the emitter lives beside
// the reading rules: every name it writes has to be a name [SumFileNames]
// covers and [LoadFS] can load, for the same layout.
//
// It is the property no single-sided test states. A name that the integrity file
// does not cover is a migration `migrate validate` cannot see; a name the loader
// rejects is one `migrate apply` refuses on a directory this binary wrote. The
// covered-count assertion is what separates them for golang-migrate and flyway,
// where two files are written and exactly one is covered.
//
// Reverted — with a name rule changed so the emitted file falls outside the
// covered glob, measured by emitting `.upp.sql` for golang-migrate — the
// golang-migrate row alone fails, on the membership assertion rather than on
// either count, because that one runs first:
//
//	error:     no matching element found
//	container: []string{"1_init.up.sql"}
//	want:      "20260806071434_addcol.upp.sql"
func TestSkeletonFilesAreCoveredAndLoadable(t *testing.T) {
	t.Parallel()
	for _, format := range skeletonFormats() {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			seed := skeletonSeedFile[format]
			files, err := atlasmigrateimport.SkeletonFiles(format, 20260806071434, "addcol")
			c.Assert(err, qt.IsNil)
			fsys := fstest.MapFS{seed.name: &fstest.MapFile{Data: []byte(seed.content)}}
			for _, file := range files {
				fsys[file.Name] = &fstest.MapFile{Data: []byte(file.Content)}
			}

			covered, err := atlasmigrateimport.SumFileNames(fsys, format)
			c.Assert(err, qt.IsNil)
			loaded, err := atlasmigrateimport.LoadFS(fsys, "skeleton", format)
			c.Assert(err, qt.IsNil)

			c.Assert(covered, qt.Contains, files[0].Name)
			c.Assert(covered, qt.HasLen, 2)
			c.Assert(loaded.Entries, qt.HasLen, 2)
		})
	}
}

// TestSkeletonFilesRejectsWhatItCannotName pins the inputs the emitter refuses,
// each with the sentinel a caller matches on rather than the message text.
//
// Reverted — with either guard removed — the empty-name row returns a file named
// `20260806071434_.up.sql` and the separator row returns
// `20260806071434_add/col.up.sql`, a name the caller would resolve against the
// migration directory.
func TestSkeletonFilesRejectsWhatItCannotName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		format  atlasmigrateimport.Format
		migName string
		wantErr error
	}{
		{
			name:    "empty_name",
			format:  atlasmigrateimport.FormatGolangMigrate,
			migName: "",
			wantErr: atlasmigrateimport.ErrSkeletonNameRequired,
		},
		{
			name:    "blank_name",
			format:  atlasmigrateimport.FormatGoose,
			migName: "   ",
			wantErr: atlasmigrateimport.ErrSkeletonNameRequired,
		},
		{
			name:    "forward_slash",
			format:  atlasmigrateimport.FormatGolangMigrate,
			migName: "add/col",
			wantErr: atlasmigrateimport.ErrSkeletonNameNotAnElement,
		},
		{
			name:    "backslash",
			format:  atlasmigrateimport.FormatFlyway,
			migName: `add\col`,
			wantErr: atlasmigrateimport.ErrSkeletonNameNotAnElement,
		},
		{
			name:    "parent_directory",
			format:  atlasmigrateimport.FormatDBMate,
			migName: "..",
			wantErr: atlasmigrateimport.ErrSkeletonNameNotAnElement,
		},
		{
			name:    "newline",
			format:  atlasmigrateimport.FormatLiquibase,
			migName: "add\ncol",
			wantErr: atlasmigrateimport.ErrSkeletonNameNotAnElement,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			got, err := atlasmigrateimport.SkeletonFiles(tt.format, 20260806071434, tt.migName)

			c.Assert(err, qt.ErrorIs, tt.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestSkeletonFilesRejectsTheNativeAtlasLayout is the control that keeps the
// emitter from becoming a second definition of the Atlas layout's file name.
//
// The compat surface forwards `--dir-format atlas` to `ptah migrations create`,
// which already owns that name; an emitter that answered for it too would be a
// rule with two homes, and the second one drifts.
//
// Reverted — with the atlas guard removed — this row returns
// `20260806071434_addcol.sql` and passes silently, which is exactly the drift it
// exists to catch.
func TestSkeletonFilesRejectsTheNativeAtlasLayout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		format  atlasmigrateimport.Format
		wantErr string
	}{
		{
			name:    "atlas",
			format:  atlasmigrateimport.FormatAtlas,
			wantErr: "the atlas migration directory format writes no skeleton files",
		},
		{
			name:    "unknown",
			format:  atlasmigrateimport.Format("nosuchformat"),
			wantErr: `unknown migration import format "nosuchformat"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			got, err := atlasmigrateimport.SkeletonFiles(tt.format, 20260806071434, "addcol")

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}
