package migrator

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
)

func TestParseMigrationFileName(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		expected    *MigrationFile
		expectError bool
	}{
		{
			name:     "valid up migration",
			filename: "0000000001_create_users_table.up.sql",
			expected: &MigrationFile{
				Version:   1,
				Name:      "Create Users Table",
				Direction: "up",
				Extension: ".sql",
			},
			expectError: false,
		},
		{
			name:     "valid down migration",
			filename: "0000000002_add_email_index.down.sql",
			expected: &MigrationFile{
				Version:   2,
				Name:      "Add Email Index",
				Direction: "down",
				Extension: ".sql",
			},
			expectError: false,
		},
		{
			name:        "invalid format - no direction",
			filename:    "0000000001_create_users_table.sql",
			expected:    nil,
			expectError: true,
		},
		{
			// Regression for issue #245: the unescaped dot in fileNameRe used
			// to make any description ending in "up"/"down" parse as a
			// migration (cleanup.sql ran as UP with description "Clea").
			name:        "description ending in up is not a direction",
			filename:    "0000000001_cleanup.sql",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "description ending in down is not a direction",
			filename:    "0000000001_teardown.sql",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "setup without direction suffix",
			filename:    "0000000001_setup.sql",
			expected:    nil,
			expectError: true,
		},
		{
			name:     "description ending in up with a proper direction suffix",
			filename: "0000000003_cleanup.up.sql",
			expected: &MigrationFile{
				Version:   3,
				Name:      "Cleanup",
				Direction: "up",
				Extension: ".sql",
			},
			expectError: false,
		},
		{
			name:     "description ending in down with a proper direction suffix",
			filename: "0000000004_teardown.down.sql",
			expected: &MigrationFile{
				Version:   4,
				Name:      "Teardown",
				Direction: "down",
				Extension: ".sql",
			},
			expectError: false,
		},
		{
			// Pins the other half of the naming language: descriptions may
			// contain dots, so an over-tightened pattern ((.*) -> ([^.]*))
			// must fail here instead of silently skipping such migrations.
			name:     "description containing dots",
			filename: "0000000001_v2.0_schema.up.sql",
			expected: &MigrationFile{
				Version:   1,
				Name:      "V2.0 Schema",
				Direction: "up",
				Extension: ".sql",
			},
			expectError: false,
		},
		{
			// The LAST direction token wins; everything before it is
			// description (greedy match).
			name:     "multiple direction tokens",
			filename: "0000000001_foo.up.down.sql",
			expected: &MigrationFile{
				Version:   1,
				Name:      "Foo.up",
				Direction: "down",
				Extension: ".sql",
			},
			expectError: false,
		},
		{
			name:     "checkpoint up migration",
			filename: "0000000005_baseline.checkpoint.up.sql",
			expected: &MigrationFile{
				Version:      5,
				Name:         "Baseline",
				Direction:    "up",
				Extension:    ".sql",
				IsCheckpoint: true,
			},
			expectError: false,
		},
		{
			name:     "checkpoint down migration",
			filename: "0000000005_baseline.checkpoint.down.sql",
			expected: &MigrationFile{
				Version:      5,
				Name:         "Baseline",
				Direction:    "down",
				Extension:    ".sql",
				IsCheckpoint: true,
			},
			expectError: false,
		},
		{
			// The checkpoint marker only counts immediately before the
			// direction; a description that merely contains "checkpoint" is an
			// ordinary migration.
			name:     "description containing checkpoint is not a checkpoint",
			filename: "0000000006_add_checkpoint_column.up.sql",
			expected: &MigrationFile{
				Version:      6,
				Name:         "Add Checkpoint Column",
				Direction:    "up",
				Extension:    ".sql",
				IsCheckpoint: false,
			},
			expectError: false,
		},
		{
			// Only a literal dot separates description from direction: a
			// lenient-separator pattern ([._]) must not sneak back in.
			name:        "underscore before direction is not a separator",
			filename:    "0000000001_add_users_up.sql",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "dash before direction is not a separator",
			filename:    "0000000001_migrate-up.sql",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "invalid format - wrong extension",
			filename:    "0000000001_create_users_table.up.txt",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "invalid format - no description",
			filename:    "0000000001_.up.sql",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "invalid format - wrong version format",
			filename:    "1_create_users_table.up.sql",
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := ParseMigrationFileName(tt.filename)

			if tt.expectError {
				c.Assert(err, qt.IsNotNil)
				c.Assert(result, qt.IsNil)
			} else {
				c.Assert(err, qt.IsNil)
				c.Assert(result, qt.IsNotNil)
				c.Assert(result.Version, qt.Equals, tt.expected.Version)
				c.Assert(result.Name, qt.Equals, tt.expected.Name)
				c.Assert(result.Direction, qt.Equals, tt.expected.Direction)
				c.Assert(result.Extension, qt.Equals, tt.expected.Extension)
				c.Assert(result.IsCheckpoint, qt.Equals, tt.expected.IsCheckpoint)
			}
		})
	}
}

func TestGenerateCheckpointMigrationFileName(t *testing.T) {
	c := qt.New(t)

	up := GenerateCheckpointMigrationFileName(5, "Cumulative Snapshot", "up")
	c.Assert(up, qt.Equals, "0000000005_cumulative_snapshot.checkpoint.up.sql")
	down := GenerateCheckpointMigrationFileName(5, "Cumulative Snapshot", "down")
	c.Assert(down, qt.Equals, "0000000005_cumulative_snapshot.checkpoint.down.sql")

	// A generated checkpoint name round-trips through the parser as a checkpoint.
	parsed, err := ParseMigrationFileName(up)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Version, qt.Equals, int64(5))
	c.Assert(parsed.Direction, qt.Equals, "up")
	c.Assert(parsed.Name, qt.Equals, "Cumulative Snapshot")
	c.Assert(parsed.IsCheckpoint, qt.IsTrue)
}

func TestParseAtlasMigrationFileName(t *testing.T) {
	c := qt.New(t)

	migrationFile, err := ParseAtlasMigrationFileName("20220318104614_team_A.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(20220318104614))
	c.Assert(migrationFile.Name, qt.Equals, "Team A")
	c.Assert(migrationFile.Direction, qt.Equals, "up")
	c.Assert(migrationFile.Extension, qt.Equals, ".sql")
	c.Assert(migrationFile.Format, qt.Equals, MigrationDirFormatAtlas)

	migrationFile, err = ParseAtlasMigrationFileName("1_initial.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(1))
	c.Assert(migrationFile.Name, qt.Equals, "Initial")

	migrationFile, err = ParseAtlasMigrationFileName("20240112070806.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(20240112070806))
	c.Assert(migrationFile.Name, qt.Equals, "20240112070806")

	migrationFile, err = ParseAtlasMigrationFileName("1_initial.up.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(1))
	c.Assert(migrationFile.Name, qt.Equals, "Initial")
	c.Assert(migrationFile.Direction, qt.Equals, "up")

	migrationFile, err = ParseAtlasMigrationFileName("1_initial.down.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(1))
	c.Assert(migrationFile.Name, qt.Equals, "Initial")
	c.Assert(migrationFile.Direction, qt.Equals, "down")

	migrationFile, err = ParseAtlasMigrationFileName("2.10.x-20_description.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(2))
	c.Assert(migrationFile.Name, qt.Equals, "10 X-20 Description")

	migrationFile, err = ParseAtlasMigrationFileName("3R_views.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(3))
	c.Assert(migrationFile.RevisionVersion(), qt.Equals, "3R")
	c.Assert(migrationFile.Name, qt.Equals, "Views")
	c.Assert(migrationFile.Direction, qt.Equals, "up")
	c.Assert(migrationFile.Format, qt.Equals, MigrationDirFormatAtlas)
	c.Assert(migrationFile.Repeatable, qt.IsTrue)

	migrationFile, err = ParseAtlasMigrationFileName("R__refresh_views.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(0))
	c.Assert(migrationFile.RevisionVersion(), qt.Equals, "R")
	c.Assert(migrationFile.Name, qt.Equals, "Refresh Views")
	c.Assert(migrationFile.Repeatable, qt.IsTrue)

	_, err = ParseAtlasMigrationFileName("R__.sql")
	c.Assert(err, qt.ErrorMatches, "invalid Atlas migration file name format")

	_, err = ParseAtlasMigrationFileName("3R_views.down.sql")
	c.Assert(err, qt.ErrorMatches, "invalid Atlas migration file name format")
}

func TestDiscoverMigrationFilesAtlasAuto(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"20220318104615_add_users.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
		"20220318104614_team_A.sql":    &fstest.MapFile{Data: []byte("CREATE TABLE teams (id INT);\n")},
		"atlas.sum":                    &fstest.MapFile{Data: []byte("ignored\n")},
	}

	files, err := DiscoverMigrationFiles(fsys, MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 2)
	c.Assert(files[0].Path, qt.Equals, "20220318104614_team_A.sql")
	c.Assert(files[0].Version, qt.Equals, int64(20220318104614))
	c.Assert(files[0].Format, qt.Equals, MigrationDirFormatAtlas)
	c.Assert(files[1].Path, qt.Equals, "20220318104615_add_users.sql")
}

func TestDiscoverMigrationFilesAutoDetectsTimestampAtlasVersionsWithoutSum(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"20240112070806.sql":        &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
		"20240116003831_second.sql": &fstest.MapFile{Data: []byte("ALTER TABLE users ADD COLUMN name TEXT;\n")},
	}

	files, err := DiscoverMigrationFiles(fsys, MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 2)
	c.Assert(files[0].Path, qt.Equals, "20240112070806.sql")
	c.Assert(files[0].Name, qt.Equals, "20240112070806")
	c.Assert(files[0].Format, qt.Equals, MigrationDirFormatAtlas)
	c.Assert(files[1].Path, qt.Equals, "20240116003831_second.sql")
	c.Assert(files[1].Name, qt.Equals, "Second")
}

func TestDiscoverMigrationFilesAtlasExplicitAllowsShortVersions(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"1_initial.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
	}

	files, err := DiscoverMigrationFiles(fsys, MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].Path, qt.Equals, "1_initial.sql")
	c.Assert(files[0].Version, qt.Equals, int64(1))
	c.Assert(files[0].Format, qt.Equals, MigrationDirFormatAtlas)

	files, err = DiscoverMigrationFiles(fsys, MigrationDirFormatAuto)
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

	files, err := DiscoverMigrationFiles(fsys, MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].Path, qt.Equals, "1.sql")
	c.Assert(files[0].Version, qt.Equals, int64(1))
	c.Assert(files[0].Format, qt.Equals, MigrationDirFormatAtlas)
}

func TestDiscoverMigrationFilesAutoDetectsShortAtlasVersionsWithSum(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"1_initial.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
		"2.sql":         &fstest.MapFile{Data: []byte("ALTER TABLE users ADD COLUMN name TEXT;\n")},
		"atlas.sum":     &fstest.MapFile{Data: []byte("h1:fake\n1_initial.sql h1:fake\n2.sql h1:fake\n")},
	}

	files, err := DiscoverMigrationFiles(fsys, MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 2)
	c.Assert(files[0].Path, qt.Equals, "1_initial.sql")
	c.Assert(files[0].Version, qt.Equals, int64(1))
	c.Assert(files[0].Format, qt.Equals, MigrationDirFormatAtlas)
	c.Assert(files[1].Path, qt.Equals, "2.sql")
	c.Assert(files[1].Version, qt.Equals, int64(2))
	c.Assert(files[1].Name, qt.Equals, "2")
	c.Assert(files[1].Format, qt.Equals, MigrationDirFormatAtlas)
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

	files, err := DiscoverMigrationFiles(fsys, MigrationDirFormatAuto)
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

	files, err := DiscoverMigrationFiles(fsys, MigrationDirFormatPtah)
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

func migrationFileByPath(files []MigrationFile, wantPath string) MigrationFile {
	for _, f := range files {
		if f.Path == wantPath {
			return f
		}
	}
	return MigrationFile{}
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

	files, err := DiscoverMigrationFiles(fsys, MigrationDirFormatAuto)
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

	files, err := DiscoverMigrationFiles(fsys, MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 2)
	for _, file := range files {
		c.Assert(file.Format, qt.Equals, MigrationDirFormatPtah)
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
		format    MigrationDirFormat
		wantPaths []string
	}{
		{
			name:      "explicit atlas narrows to the covered set",
			fsys:      func() fstest.MapFS { return discoveryAtlasSpreadFS(nil) },
			format:    MigrationDirFormatAtlas,
			wantPaths: []string{"1_a.sql"},
		},
		{
			name:      "auto with atlas.sum narrows to the covered set",
			fsys:      func() fstest.MapFS { return discoveryAtlasSpreadFS(map[string]string{"atlas.sum": sumBody}) },
			format:    MigrationDirFormatAuto,
			wantPaths: []string{"1_a.sql"},
		},
		{
			name:      "auto without atlas.sum keeps the whole tree",
			fsys:      func() fstest.MapFS { return discoveryAtlasSpreadFS(nil) },
			format:    MigrationDirFormatAuto,
			wantPaths: []string{"1_a.sql", "sub/2_b.sql"},
		},
		{
			name:   "ptah keeps the whole tree",
			fsys:   func() fstest.MapFS { return discoveryPtahSpreadFS(nil) },
			format: MigrationDirFormatPtah,
			wantPaths: []string{
				"0000000001_init.up.sql", "0000000001_init.down.sql",
				"sub/0000000002_more.up.sql", "sub/0000000002_more.down.sql",
				"0000000003_third.up.sql", "0000000003_third.down.sql",
			},
		},
		{
			name:   "ptah beside an atlas.sum keeps the whole tree",
			fsys:   func() fstest.MapFS { return discoveryPtahSpreadFS(map[string]string{"atlas.sum": sumBody}) },
			format: MigrationDirFormatPtah,
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

			files, err := DiscoverMigrationFiles(tt.fsys(), tt.format)
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

	files, err := DiscoverMigrationFiles(fsys, MigrationDirFormatAuto)
	c.Assert(files, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, `no migration files matched format "auto"; unrecognized SQL files: .*`)
	c.Assert(err.Error(), qt.Contains, "cleanup.sql")
	c.Assert(err.Error(), qt.Contains, "0000000001_legacy.sql")
}

func TestValidateMigrationFileName(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected bool
	}{
		{
			name:     "valid up migration",
			filename: "0000000001_create_users_table.up.sql",
			expected: true,
		},
		{
			name:     "valid down migration",
			filename: "0000000002_add_email_index.down.sql",
			expected: true,
		},
		{
			name:     "invalid format",
			filename: "invalid_filename.sql",
			expected: false,
		},
		{
			name:     "description ending in up without direction suffix",
			filename: "0000000001_cleanup.sql",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := ValidateMigrationFileName(tt.filename)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestGenerateMigrationFileName(t *testing.T) {
	tests := []struct {
		name        string
		version     int64
		description string
		direction   string
		expected    string
	}{
		{
			name:        "basic generation",
			version:     1,
			description: "Create Users Table",
			direction:   "up",
			expected:    "0000000001_create_users_table.up.sql",
		},
		{
			name:        "with special characters",
			version:     123,
			description: "Add Email Index & Constraints",
			direction:   "down",
			expected:    "0000000123_add_email_index__constraints.down.sql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := GenerateMigrationFileName(tt.version, tt.description, tt.direction)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestMigrationPair(t *testing.T) {
	c := qt.New(t)

	upFile := &MigrationFile{
		Version:   1,
		Name:      "Create Users Table",
		Direction: "up",
		Extension: ".sql",
	}

	downFile := &MigrationFile{
		Version:   1,
		Name:      "Create Users Table",
		Direction: "down",
		Extension: ".sql",
	}

	// Test complete pair
	completePair := MigrationPair{
		Up:   upFile,
		Down: downFile,
	}

	c.Assert(completePair.IsComplete(), qt.IsTrue)
	c.Assert(completePair.HasUp(), qt.IsTrue)
	c.Assert(completePair.HasDown(), qt.IsTrue)
	c.Assert(completePair.GetVersion(), qt.Equals, int64(1))
	c.Assert(completePair.GetDescription(), qt.Equals, "Create Users Table")

	// Test incomplete pair (only up)
	upOnlyPair := MigrationPair{
		Up:   upFile,
		Down: nil,
	}

	c.Assert(upOnlyPair.IsComplete(), qt.IsFalse)
	c.Assert(upOnlyPair.HasUp(), qt.IsTrue)
	c.Assert(upOnlyPair.HasDown(), qt.IsFalse)
	c.Assert(upOnlyPair.GetVersion(), qt.Equals, int64(1))
	c.Assert(upOnlyPair.GetDescription(), qt.Equals, "Create Users Table")

	// Test incomplete pair (only down)
	downOnlyPair := MigrationPair{
		Up:   nil,
		Down: downFile,
	}

	c.Assert(downOnlyPair.IsComplete(), qt.IsFalse)
	c.Assert(downOnlyPair.HasUp(), qt.IsFalse)
	c.Assert(downOnlyPair.HasDown(), qt.IsTrue)
	c.Assert(downOnlyPair.GetVersion(), qt.Equals, int64(1))
	c.Assert(downOnlyPair.GetDescription(), qt.Equals, "Create Users Table")

	// Test empty pair
	emptyPair := MigrationPair{}

	c.Assert(emptyPair.IsComplete(), qt.IsFalse)
	c.Assert(emptyPair.HasUp(), qt.IsFalse)
	c.Assert(emptyPair.HasDown(), qt.IsFalse)
	c.Assert(emptyPair.GetVersion(), qt.Equals, int64(0))
	c.Assert(emptyPair.GetDescription(), qt.Equals, "")
}

func TestGroupMigrationFiles(t *testing.T) {
	c := qt.New(t)

	files := []MigrationFile{
		{Version: 1, Name: "Create Users", Direction: "up", Extension: ".sql"},
		{Version: 1, Name: "Create Users", Direction: "down", Extension: ".sql"},
		{Version: 2, Name: "Add Index", Direction: "up", Extension: ".sql"},
		{Version: 3, Name: "Add Column", Direction: "down", Extension: ".sql"},
	}

	groups := GroupMigrationFiles(files)

	c.Assert(groups, qt.HasLen, 3)

	// Check version 1 (complete pair)
	pair1 := groups[1]
	c.Assert(pair1.IsComplete(), qt.IsTrue)
	c.Assert(pair1.GetVersion(), qt.Equals, int64(1))

	// Check version 2 (only up)
	pair2 := groups[2]
	c.Assert(pair2.IsComplete(), qt.IsFalse)
	c.Assert(pair2.HasUp(), qt.IsTrue)
	c.Assert(pair2.HasDown(), qt.IsFalse)

	// Check version 3 (only down)
	pair3 := groups[3]
	c.Assert(pair3.IsComplete(), qt.IsFalse)
	c.Assert(pair3.HasUp(), qt.IsFalse)
	c.Assert(pair3.HasDown(), qt.IsTrue)
}

func TestValidateMigrationPairs(t *testing.T) {
	c := qt.New(t)

	pairs := map[int64]MigrationPair{
		1: {
			Up:   &MigrationFile{Version: 1, Direction: "up"},
			Down: &MigrationFile{Version: 1, Direction: "down"},
		},
		2: {
			Up:   &MigrationFile{Version: 2, Direction: "up"},
			Down: nil, // Missing down migration
		},
		3: {
			Up:   nil, // Missing up migration
			Down: &MigrationFile{Version: 3, Direction: "down"},
		},
	}

	incomplete := ValidateMigrationPairs(pairs)

	c.Assert(incomplete, qt.HasLen, 2)
	c.Assert(incomplete, qt.Contains, int64(2))
	c.Assert(incomplete, qt.Contains, int64(3))
}

func TestFindMigrationGaps(t *testing.T) {
	c := qt.New(t)

	// Test with no gaps
	versions1 := []int64{1, 2, 3, 4, 5}
	gaps1 := FindMigrationGaps(versions1)
	c.Assert(gaps1, qt.HasLen, 0)

	// Test with gaps
	versions2 := []int64{1, 3, 6, 8}
	gaps2 := FindMigrationGaps(versions2)
	c.Assert(gaps2, qt.HasLen, 4) // Should be 4: gaps at 2, 4, 5, 7
	c.Assert(gaps2, qt.Contains, int64(2))
	c.Assert(gaps2, qt.Contains, int64(4))
	c.Assert(gaps2, qt.Contains, int64(5))
	c.Assert(gaps2, qt.Contains, int64(7))

	// Test with empty slice
	versions3 := make([]int64, 0)
	gaps3 := FindMigrationGaps(versions3)
	c.Assert(gaps3, qt.IsNil)

	// Test with single version
	versions4 := []int64{1}
	gaps4 := FindMigrationGaps(versions4)
	c.Assert(gaps4, qt.HasLen, 0)
}

func TestGetNextMigrationVersion(t *testing.T) {
	c := qt.New(t)

	version1 := GetNextMigrationVersion()
	c.Assert(version1, qt.Not(qt.Equals), 0)

	// Get another version and ensure it's different (or at least not less)
	version2 := GetNextMigrationVersion()
	c.Assert(version2, qt.Not(qt.Equals), 0)
	// Version2 should be >= version1 (timestamps should be monotonic or equal)
	c.Assert(version2 >= version1, qt.IsTrue)
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

			files, err := DiscoverMigrationFiles(tt.fsys, MigrationDirFormatAuto)

			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, tt.want)
		})
	}
}
