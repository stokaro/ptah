package migrationfile_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

func TestDiscoverMigrationFilesAtlasAuto(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"20220318104615_add_users.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
		"20220318104614_team_A.sql":    &fstest.MapFile{Data: []byte("CREATE TABLE teams (id INT);\n")},
		"atlas.sum":                    &fstest.MapFile{Data: []byte("ignored\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 2)
	c.Assert(files[0].Path, qt.Equals, "20220318104614_team_A.sql")
	c.Assert(files[0].Version, qt.Equals, int64(20220318104614))
	c.Assert(files[0].Format, qt.Equals, migrationfile.DirFormatAtlas)
	c.Assert(files[1].Path, qt.Equals, "20220318104615_add_users.sql")
}

func TestDiscoverMigrationFilesAutoDetectsTimestampAtlasVersionsWithoutSum(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"20240112070806.sql":        &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
		"20240116003831_second.sql": &fstest.MapFile{Data: []byte("ALTER TABLE users ADD COLUMN name TEXT;\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 2)
	c.Assert(files[0].Path, qt.Equals, "20240112070806.sql")
	c.Assert(files[0].Name, qt.Equals, "20240112070806")
	c.Assert(files[0].Format, qt.Equals, migrationfile.DirFormatAtlas)
	c.Assert(files[1].Path, qt.Equals, "20240116003831_second.sql")
	c.Assert(files[1].Name, qt.Equals, "Second")
}

func TestDiscoverMigrationFilesAtlasExplicitAllowsShortVersions(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"1_initial.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].Path, qt.Equals, "1_initial.sql")
	c.Assert(files[0].Version, qt.Equals, int64(1))
	c.Assert(files[0].Format, qt.Equals, migrationfile.DirFormatAtlas)

	files, err = migrationfile.Discover(fsys, migrationfile.DirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].Path, qt.Equals, "1_initial.sql")
	c.Assert(files[0].Version, qt.Equals, int64(1))
}

func TestDiscoverMigrationFilesAutoDetectsShortBareVersionWithoutSum(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"1.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].Path, qt.Equals, "1.sql")
	c.Assert(files[0].Version, qt.Equals, int64(1))
	c.Assert(files[0].Format, qt.Equals, migrationfile.DirFormatAtlas)
}

func TestDiscoverMigrationFilesAutoDetectsShortAtlasVersionsWithSum(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"1_initial.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
		"2.sql":         &fstest.MapFile{Data: []byte("ALTER TABLE users ADD COLUMN name TEXT;\n")},
		"atlas.sum":     &fstest.MapFile{Data: []byte("h1:fake\n1_initial.sql h1:fake\n2.sql h1:fake\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 2)
	c.Assert(files[0].Path, qt.Equals, "1_initial.sql")
	c.Assert(files[0].Version, qt.Equals, int64(1))
	c.Assert(files[0].Format, qt.Equals, migrationfile.DirFormatAtlas)
	c.Assert(files[1].Path, qt.Equals, "2.sql")
	c.Assert(files[1].Version, qt.Equals, int64(2))
	c.Assert(files[1].Name, qt.Equals, "2")
	c.Assert(files[1].Format, qt.Equals, migrationfile.DirFormatAtlas)
}

func TestDiscoverMigrationFilesRecognizesAtlasImportedFlywayRepeatables(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"2_baseline.sql":        &fstest.MapFile{Data: []byte("CREATE TABLE post (id INT);\n")},
		"3R_views.sql":          &fstest.MapFile{Data: []byte("CREATE VIEW my_view AS SELECT * FROM post;\n")},
		"3_third_migration.sql": &fstest.MapFile{Data: []byte("ALTER TABLE post ADD COLUMN title TEXT;\n")},
		"not_a_repeatable.sql":  &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		"also_not_R_views.sql":  &fstest.MapFile{Data: []byte("SELECT 2;\n")},
		"atlas.sum":             &fstest.MapFile{Data: []byte("ignored\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 3)
	c.Assert(files[0].Path, qt.Equals, "2_baseline.sql")
	c.Assert(files[0].Version, qt.Equals, int64(2))
	c.Assert(files[1].Path, qt.Equals, "3R_views.sql")
	c.Assert(files[1].Repeatable, qt.IsTrue)
	c.Assert(files[1].Version, qt.Equals, int64(3))
	c.Assert(files[1].RevisionVersion(), qt.Equals, "3R")
	c.Assert(files[2].Path, qt.Equals, "3_third_migration.sql")
	c.Assert(files[2].Version, qt.Equals, int64(3))
}

func TestDiscoverMigrationFilesRecognizesCheckpoints(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"0000000001_init.up.sql":                  &fstest.MapFile{Data: []byte("CREATE TABLE post (id INT);\n")},
		"0000000001_init.down.sql":                &fstest.MapFile{Data: []byte("DROP TABLE post;\n")},
		"0000000002_snapshot.checkpoint.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE post (id INT, title TEXT);\n")},
		"0000000002_snapshot.checkpoint.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE post;\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatPtah)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 4)

	initUp := migrationFileByPath(files, "0000000001_init.up.sql")
	c.Assert(initUp.IsCheckpoint, qt.IsFalse)

	checkpointUp := migrationFileByPath(files, "0000000002_snapshot.checkpoint.up.sql")
	c.Assert(checkpointUp.IsCheckpoint, qt.IsTrue)
	c.Assert(checkpointUp.Version, qt.Equals, int64(2))
	c.Assert(checkpointUp.Direction, qt.Equals, "up")
	c.Assert(checkpointUp.Name, qt.Equals, "Snapshot")

	checkpointDown := migrationFileByPath(files, "0000000002_snapshot.checkpoint.down.sql")
	c.Assert(checkpointDown.IsCheckpoint, qt.IsTrue)
	c.Assert(checkpointDown.Direction, qt.Equals, "down")
}

func migrationFileByPath(files []migrationfile.File, wantPath string) migrationfile.File {
	for _, f := range files {
		if f.Path == wantPath {
			return f
		}
	}
	return migrationfile.File{}
}

func TestDiscoverMigrationFilesAutoDetectsAtlasImportedNames(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"1_initial.down.sql":               &fstest.MapFile{Data: []byte("DROP TABLE users;\n")},
		"1_initial.up.sql":                 &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
		"2.10.x-20_description.sql":        &fstest.MapFile{Data: []byte("CREATE TABLE audit (id INT);\n")},
		"sub/3_partly.sql":                 &fstest.MapFile{Data: []byte("CREATE TABLE logs (id INT);\n")},
		"sub/4.a_sub.up.sql":               &fstest.MapFile{Data: []byte("CREATE TABLE nested (id INT);\n")},
		"sub/4.a_sub.down.sql":             &fstest.MapFile{Data: []byte("DROP TABLE nested;\n")},
		"0000000001_ptah_width_legacy.sql": &fstest.MapFile{Data: []byte("DROP TABLE legacy;\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 6)
	c.Assert(files[0].Path, qt.Equals, "1_initial.down.sql")
	c.Assert(files[0].Direction, qt.Equals, "down")
	c.Assert(files[1].Path, qt.Equals, "1_initial.up.sql")
	c.Assert(files[1].Direction, qt.Equals, "up")
	c.Assert(files[2].Path, qt.Equals, "2.10.x-20_description.sql")
	c.Assert(files[2].Name, qt.Equals, "10 X-20 Description")
	c.Assert(files[5].Path, qt.Equals, "sub/4.a_sub.up.sql")
}

func TestDiscoverMigrationFilesAutoPrefersPtahWhenPresent(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"0000000001_init.up.sql":       &fstest.MapFile{Data: []byte("CREATE TABLE t (id INT);\n")},
		"0000000001_init.down.sql":     &fstest.MapFile{Data: []byte("DROP TABLE t;\n")},
		"20220318104614_atlas_way.sql": &fstest.MapFile{Data: []byte("CREATE TABLE atlas_t (id INT);\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 2)
	for _, file := range files {
		c.Assert(file.Format, qt.Equals, migrationfile.DirFormatPtah)
	}
}

// discoveryAtlasSpreadFS holds one file in each of the three positions the
// Atlas covered set distinguishes: top level spelled `.sql` (covered), one
// level down (not covered), and top level spelled `.SQL` (not covered).
func discoveryAtlasSpreadFS(extra map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{
		"1_a.sql":     &fstest.MapFile{Data: []byte("CREATE TABLE a (id INT);\n")},
		"sub/2_b.sql": &fstest.MapFile{Data: []byte("CREATE TABLE b (id INT);\n")},
		"3_c.SQL":     &fstest.MapFile{Data: []byte("CREATE TABLE c (id INT);\n")},
	}
	for name, body := range extra {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

// discoveryPtahSpreadFS is the same spread in ptah's naming convention, whose
// integrity file is computed FROM discovery and therefore never constrains it.
func discoveryPtahSpreadFS(extra map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{
		"0000000001_init.up.sql":       &fstest.MapFile{Data: []byte("CREATE TABLE a (id INT);\n")},
		"sub/0000000002_more.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE b (id INT);\n")},
		"0000000003_third.up.sql":      &fstest.MapFile{Data: []byte("CREATE TABLE c (id INT);\n")},
		"0000000001_init.down.sql":     &fstest.MapFile{Data: []byte("DROP TABLE a;\n")},
		"sub/0000000002_more.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE b;\n")},
		"0000000003_third.down.sql":    &fstest.MapFile{Data: []byte("DROP TABLE c;\n")},
	}
	for name, body := range extra {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

// TestDiscoverMigrationFilesAtlasSumGovernsSelection enumerates every branch of
// the format x integrity-file matrix, not only the one stokaro/ptah#976 was
// reported against.
//
// The narrowing is keyed on which integrity file governs the directory, which
// is not the same question as which format was requested: `--dir-format auto`
// over a directory that already carries an atlas.sum resolves to the Atlas
// hasher, so it must resolve to the Atlas file set too. A first attempt at this
// fix gated on the explicit format alone and left that branch recursing, with
// the hole fully open on the native surface — so the auto-with-sum row is the
// one this table exists for, and the auto-without-sum row is what proves the
// narrowing did not leak into the branch whose sum does cover the whole tree.
func TestDiscoverMigrationFilesAtlasSumGovernsSelection(t *testing.T) {
	const sumBody = "h1:fake\n1_a.sql h1:fake\n"

	tests := []struct {
		name      string
		fsys      func() fstest.MapFS
		format    migrationfile.DirFormat
		wantPaths []string
	}{
		{
			name:      "explicit atlas narrows to the covered set",
			fsys:      func() fstest.MapFS { return discoveryAtlasSpreadFS(nil) },
			format:    migrationfile.DirFormatAtlas,
			wantPaths: []string{"1_a.sql"},
		},
		{
			name:      "auto with atlas.sum narrows to the covered set",
			fsys:      func() fstest.MapFS { return discoveryAtlasSpreadFS(map[string]string{"atlas.sum": sumBody}) },
			format:    migrationfile.DirFormatAuto,
			wantPaths: []string{"1_a.sql"},
		},
		{
			name:      "auto without atlas.sum keeps the whole tree",
			fsys:      func() fstest.MapFS { return discoveryAtlasSpreadFS(nil) },
			format:    migrationfile.DirFormatAuto,
			wantPaths: []string{"1_a.sql", "sub/2_b.sql"},
		},
		{
			name:   "ptah keeps the whole tree",
			fsys:   func() fstest.MapFS { return discoveryPtahSpreadFS(nil) },
			format: migrationfile.DirFormatPtah,
			wantPaths: []string{
				"0000000001_init.up.sql", "0000000001_init.down.sql",
				"sub/0000000002_more.up.sql", "sub/0000000002_more.down.sql",
				"0000000003_third.up.sql", "0000000003_third.down.sql",
			},
		},
		{
			name:   "ptah beside an atlas.sum keeps the whole tree",
			fsys:   func() fstest.MapFS { return discoveryPtahSpreadFS(map[string]string{"atlas.sum": sumBody}) },
			format: migrationfile.DirFormatPtah,
			wantPaths: []string{
				"0000000001_init.up.sql", "0000000001_init.down.sql",
				"sub/0000000002_more.up.sql", "sub/0000000002_more.down.sql",
				"0000000003_third.up.sql", "0000000003_third.down.sql",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := migrationfile.Discover(tt.fsys(), tt.format)
			c.Assert(err, qt.IsNil)

			paths := make([]string, 0, len(files))
			for _, file := range files {
				paths = append(paths, file.Path)
			}
			c.Assert(paths, qt.ContentEquals, tt.wantPaths)
		})
	}
}

func TestDiscoverMigrationFilesUnknownOnlySQLErrors(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"cleanup.sql":           &fstest.MapFile{Data: []byte("DROP TABLE users;\n")},
		"0000000001_legacy.sql": &fstest.MapFile{Data: []byte("DROP TABLE audit;\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAuto)
	c.Assert(files, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, `no migration files matched format "auto"; unrecognized SQL files: .*`)
	c.Assert(err.Error(), qt.Contains, "cleanup.sql")
	c.Assert(err.Error(), qt.Contains, "0000000001_legacy.sql")
}

// TestDiscoverMigrationFilesNestedAtlasSumDoesNotGovern covers the predicate
// that decides which integrity file governs a directory.
//
// The scan walks recursively, so a nested `sub/atlas.sum` would set the flag,
// while the function that actually picks the hasher looks at the root only.
// When the two disagreed, a directory holding a top-level pair, a nested pair
// and `sub/atlas.sum` applied version 1 and stopped — exit 0, success banner,
// no warning, and the author's second migration silently never ran.
//
// The assertion is on the discovered set rather than on an error, because that
// failure had no error to assert: it succeeded, quietly, with less work done.
func TestDiscoverMigrationFilesNestedAtlasSumDoesNotGovern(t *testing.T) {
	tests := []struct {
		name string
		fsys fstest.MapFS
		want int
	}{
		{
			name: "nested atlas.sum leaves the ptah pair discoverable",
			fsys: fstest.MapFS{
				"0000000001_init.up.sql":       &fstest.MapFile{Data: []byte("CREATE TABLE a (id int);")},
				"0000000001_init.down.sql":     &fstest.MapFile{Data: []byte("DROP TABLE a;")},
				"sub/0000000002_more.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE b (id int);")},
				"sub/0000000002_more.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE b;")},
				"sub/atlas.sum":                &fstest.MapFile{Data: []byte("h1:xxx=\n")},
			},
			// Four files, not two migrations: the discovery returns each
			// direction separately.
			want: 4,
		},
		{
			name: "a top-level atlas.sum still governs",
			fsys: fstest.MapFS{
				"20240101000000_init.sql": &fstest.MapFile{Data: []byte("CREATE TABLE a (id int);")},
				"sub/20240102000000_more.sql": &fstest.MapFile{
					Data: []byte("CREATE TABLE b (id int);"),
				},
				"atlas.sum": &fstest.MapFile{Data: []byte("h1:xxx=\n20240101000000_init.sql h1:yyy=\n")},
			},
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := migrationfile.Discover(tt.fsys, migrationfile.DirFormatAuto)

			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, tt.want)
		})
	}
}
