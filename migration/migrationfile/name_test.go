package migrationfile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

func TestParseFileName(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		expected    *migrationfile.File
		expectError bool
	}{
		{
			name:     "valid up migration",
			filename: "0000000001_create_users_table.up.sql",
			expected: &migrationfile.File{
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
			expected: &migrationfile.File{
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
			expected: &migrationfile.File{
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
			expected: &migrationfile.File{
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
			expected: &migrationfile.File{
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
			expected: &migrationfile.File{
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
			expected: &migrationfile.File{
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
			expected: &migrationfile.File{
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
			expected: &migrationfile.File{
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

			result, err := migrationfile.ParseFileName(tt.filename)

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

func TestCheckpointFileName(t *testing.T) {
	c := qt.New(t)

	up := migrationfile.CheckpointFileName(5, "Cumulative Snapshot", "up")
	c.Assert(up, qt.Equals, "0000000005_cumulative_snapshot.checkpoint.up.sql")
	down := migrationfile.CheckpointFileName(5, "Cumulative Snapshot", "down")
	c.Assert(down, qt.Equals, "0000000005_cumulative_snapshot.checkpoint.down.sql")

	// A generated checkpoint name round-trips through the parser as a checkpoint.
	parsed, err := migrationfile.ParseFileName(up)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Version, qt.Equals, int64(5))
	c.Assert(parsed.Direction, qt.Equals, "up")
	c.Assert(parsed.Name, qt.Equals, "Cumulative Snapshot")
	c.Assert(parsed.IsCheckpoint, qt.IsTrue)
}

func TestParseAtlasFileName(t *testing.T) {
	c := qt.New(t)

	migrationFile, err := migrationfile.ParseAtlasFileName("20220318104614_team_A.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(20220318104614))
	c.Assert(migrationFile.Name, qt.Equals, "Team A")
	c.Assert(migrationFile.Direction, qt.Equals, "up")
	c.Assert(migrationFile.Extension, qt.Equals, ".sql")
	c.Assert(migrationFile.Format, qt.Equals, migrationfile.DirFormatAtlas)

	migrationFile, err = migrationfile.ParseAtlasFileName("1_initial.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(1))
	c.Assert(migrationFile.Name, qt.Equals, "Initial")

	migrationFile, err = migrationfile.ParseAtlasFileName("20240112070806.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(20240112070806))
	c.Assert(migrationFile.Name, qt.Equals, "20240112070806")

	migrationFile, err = migrationfile.ParseAtlasFileName("1_initial.up.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(1))
	c.Assert(migrationFile.Name, qt.Equals, "Initial")
	c.Assert(migrationFile.Direction, qt.Equals, "up")

	migrationFile, err = migrationfile.ParseAtlasFileName("1_initial.down.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(1))
	c.Assert(migrationFile.Name, qt.Equals, "Initial")
	c.Assert(migrationFile.Direction, qt.Equals, "down")

	migrationFile, err = migrationfile.ParseAtlasFileName("2.10.x-20_description.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(2))
	c.Assert(migrationFile.Name, qt.Equals, "10 X-20 Description")

	migrationFile, err = migrationfile.ParseAtlasFileName("3R_views.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(3))
	c.Assert(migrationFile.RevisionVersion(), qt.Equals, "3R")
	c.Assert(migrationFile.Name, qt.Equals, "Views")
	c.Assert(migrationFile.Direction, qt.Equals, "up")
	c.Assert(migrationFile.Format, qt.Equals, migrationfile.DirFormatAtlas)
	c.Assert(migrationFile.Repeatable, qt.IsTrue)

	migrationFile, err = migrationfile.ParseAtlasFileName("R__refresh_views.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFile.Version, qt.Equals, int64(0))
	c.Assert(migrationFile.RevisionVersion(), qt.Equals, "R")
	c.Assert(migrationFile.Name, qt.Equals, "Refresh Views")
	c.Assert(migrationFile.Repeatable, qt.IsTrue)

	_, err = migrationfile.ParseAtlasFileName("R__.sql")
	c.Assert(err, qt.ErrorMatches, "invalid Atlas migration file name format")

	_, err = migrationfile.ParseAtlasFileName("3R_views.down.sql")
	c.Assert(err, qt.ErrorMatches, "invalid Atlas migration file name format")
}

func TestFileName(t *testing.T) {
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

			result := migrationfile.FileName(tt.version, tt.description, tt.direction)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestNextVersion(t *testing.T) {
	c := qt.New(t)

	version1 := migrationfile.NextVersion()
	c.Assert(version1, qt.Not(qt.Equals), 0)

	// Get another version and ensure it's different (or at least not less)
	version2 := migrationfile.NextVersion()
	c.Assert(version2, qt.Not(qt.Equals), 0)
	// Version2 should be >= version1 (timestamps should be monotonic or equal)
	c.Assert(version2 >= version1, qt.IsTrue)
}
