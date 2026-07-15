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
			}
		})
	}
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
	_, err = ParseAtlasMigrationFileName("20220318104614_team_A.up.sql")
	c.Assert(err, qt.ErrorMatches, "Atlas migration file name must not use Ptah direction suffixes")
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
	c.Assert(files, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, `no migration files matched format "auto"; unrecognized SQL files: 1_initial.sql`)
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
	versions3 := []int64{}
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
