package atlasreport_test

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/migrationreplay"
	migrationlint "ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// replayFixture is the directory the replay-failure tests analyze: a base that
// creates two tables, then one file dropping each. Linting the last file alone
// reports DS102 for it, so a report that still carried the analysis would
// carry that finding.
func replayFixture() fstest.MapFS {
	return fstest.MapFS{
		"1.sql": {Data: []byte("CREATE TABLE users (id int);\n\nCREATE TABLE pets (id int);\n\nALTER TABLE users RENAME COLUMN nope TO oid2;\n")},
		"2.sql": {Data: []byte("DROP TABLE users;\n")},
		"3.sql": {Data: []byte("DROP TABLE pets;\n")},
	}
}

func analyzeReplayFixture(c *qt.C, fsys fstest.MapFS, selection migrationlint.VersionSelection) migrationlint.Analysis {
	c.Helper()
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatAtlas,
		Dialect:       "sqlite",
		Selection:     selection,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// lintReplayError wraps a migration error the way the lint report does before
// it reaches the compatibility surface.
func lintReplayError(failed *migrationreplay.MigrationError) error {
	return fmt.Errorf("error validating migration SQL on dev database: %w", failed)
}

const replayAnswer = `SQL logic error: no such column: "nope" (1)`

func TestNewMigrateLintReplayFailure_HappyPath(t *testing.T) {
	tests := []struct {
		name             string
		files            fstest.MapFS
		selected         []int64
		revisionVersions map[int64]string
		failed           migrationreplay.MigrationError
		wantFile         string
		wantError        string
	}{
		{
			name:     "a statement of the base the selection starts from names no statement",
			files:    replayFixture(),
			selected: []int64{3},
			failed: migrationreplay.MigrationError{
				Version:    1,
				VersionKey: "1",
				Statement: &migrationreplay.FailedStatement{
					SQL:     "ALTER TABLE users RENAME COLUMN nope TO oid2",
					Written: "ALTER TABLE users RENAME COLUMN nope TO oid2;",
					Index:   3,
					Answer:  errors.New(replayAnswer),
				},
			},
			wantFile:  "1.sql",
			wantError: "executing statement: " + replayAnswer,
		},
		{
			name:     "a statement of a selected file is quoted as the file spells it",
			files:    replayFixture(),
			selected: []int64{1, 2, 3},
			failed: migrationreplay.MigrationError{
				Version:    1,
				VersionKey: "1",
				Statement: &migrationreplay.FailedStatement{
					SQL:     "ALTER TABLE users RENAME COLUMN nope TO oid2",
					Written: "ALTER TABLE users RENAME COLUMN nope TO oid2;",
					Index:   3,
					Answer:  errors.New(replayAnswer),
				},
			},
			wantFile:  "1.sql",
			wantError: "executing statement: ALTER TABLE users RENAME COLUMN nope TO oid2;: " + replayAnswer,
		},
		{
			name:     "a statement refused before it ran keeps the reason it was refused",
			files:    replayFixture(),
			selected: []int64{3},
			failed: migrationreplay.MigrationError{
				Version:    2,
				VersionKey: "2",
				Err:        errors.New("failed to validate migration SQL in 2.sql: refused"),
			},
			wantFile:  "2.sql",
			wantError: "failed to validate migration SQL in 2.sql: refused",
		},
		{
			name: "a converted file is found by its source token",
			files: fstest.MapFS{
				"10_b.sql": {Data: []byte("INSERT INTO missing VALUES (1);\n")},
			},
			selected:         []int64{10},
			revisionVersions: map[int64]string{10: "1.5"},
			failed: migrationreplay.MigrationError{
				Version:    10,
				VersionKey: "1.5",
				Statement: &migrationreplay.FailedStatement{
					SQL:     "INSERT INTO missing VALUES (1)",
					Written: "INSERT INTO missing VALUES (1);",
					Index:   1,
					Answer:  errors.New("SQL logic error: no such table: missing (1)"),
				},
			},
			wantFile:  "10_b.sql",
			wantError: "executing statement: INSERT INTO missing VALUES (1);: SQL logic error: no such table: missing (1)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeReplayFixture(c, test.files, migrationlint.VersionSelection{
				Versions:   test.selected,
				Restricted: true,
			})

			got := atlasreport.NewMigrateLintReplayFailure(analysis, test.revisionVersions, lintReplayError(&test.failed))

			c.Assert(got, qt.IsNotNil)
			c.Assert(got.File, qt.Equals, test.wantFile)
			c.Assert(got.Error, qt.Equals, test.wantError)
		})
	}
}

// TestNewMigrateLintReplayFailure_BlamesTheUpFile pins that a down file, which
// shares its version and revision key with the up file, is never the file a
// replay failure blames: the replay runs only the up file.
func TestNewMigrateLintReplayFailure_BlamesTheUpFile(t *testing.T) {
	c := qt.New(t)
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fstest.MapFS{
		"0000000001_init.down.sql": {Data: []byte("DROP TABLE users;\n")},
		"0000000001_init.up.sql":   {Data: []byte("CREATE TABLE users (id int);\n\nALTER TABLE users RENAME COLUMN nope TO x;\n")},
	}, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatPtah,
		Dialect:       "sqlite",
		Selection:     migrationlint.VersionSelection{Versions: []int64{1}, Restricted: true},
	})
	c.Assert(err, qt.IsNil)

	got := atlasreport.NewMigrateLintReplayFailure(analysis, nil, lintReplayError(&migrationreplay.MigrationError{
		Version:    1,
		VersionKey: "1",
		Statement: &migrationreplay.FailedStatement{
			SQL:     "ALTER TABLE users RENAME COLUMN nope TO x",
			Written: "ALTER TABLE users RENAME COLUMN nope TO x;",
			Index:   2,
			Answer:  errors.New(replayAnswer),
		},
	}))

	c.Assert(got, qt.IsNotNil)
	c.Assert(got.File, qt.Equals, "0000000001_init.up.sql")
	c.Assert(got.Error, qt.Equals, "executing statement: ALTER TABLE users RENAME COLUMN nope TO x;: "+replayAnswer)
}

// TestNewMigrateLintReplayFailure_FailurePath pins the replays that name no
// file of the analysis: those keep the error a caller already reports, rather
// than a report that blames a file.
func TestNewMigrateLintReplayFailure_FailurePath(t *testing.T) {
	statement := &migrationreplay.FailedStatement{SQL: "INSERT INTO missing VALUES (1)", Index: 1, Answer: errors.New("no such table")}
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "a failure before the first migration",
			err:  errors.New("error validating migration SQL on dev database: error connecting to dev database: refused"),
		},
		{
			name: "a version the directory does not hold",
			err:  lintReplayError(&migrationreplay.MigrationError{Version: 9, VersionKey: "9", Statement: statement}),
		},
		{
			name: "a revision identity the file does not carry",
			err:  lintReplayError(&migrationreplay.MigrationError{Version: 10, VersionKey: "10", Statement: statement}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeReplayFixture(c, fstest.MapFS{
				"10_b.sql": {Data: []byte("INSERT INTO missing VALUES (1);\n")},
			}, migrationlint.VersionSelection{})

			got := atlasreport.NewMigrateLintReplayFailure(analysis, map[int64]string{10: "1.5"}, test.err)

			c.Assert(got, qt.IsNil)
		})
	}
}

var replayTextDurationRe = regexp.MustCompile(`(?m)^(  -------------------------\n  -- ).+$`)

// TestWriteMigrateLintText_ReplayFailure pins the community binary's text
// report of a replay that stopped in a file: the selection's header, the error
// in place of any analyzed version, and one version with errors. No schema
// change or diagnostic line follows, because nothing was analyzed.
func TestWriteMigrateLintText_ReplayFailure(t *testing.T) {
	tests := []struct {
		name       string
		selection  migrationlint.VersionSelection
		wantHeader string
	}{
		{
			name:       "a selection after a base",
			selection:  migrationlint.VersionSelection{Versions: []int64{3}, Restricted: true},
			wantHeader: "Analyzing changes from version 2 to 3 (1 migration in total):",
		},
		{
			name:       "a selection from the first version",
			selection:  migrationlint.VersionSelection{Versions: []int64{1, 2, 3}, Restricted: true},
			wantHeader: "Analyzing changes until version 3 (3 migrations in total):",
		},
		{
			name:       "a selection of nothing still counts the failing file",
			selection:  migrationlint.VersionSelection{Restricted: true},
			wantHeader: "Analyzing changes (1 migration in total):",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeReplayFixture(c, replayFixture(), test.selection)
			var out bytes.Buffer

			err := atlasreport.WriteMigrateLintText(&out, atlasreport.MigrateLintOptions{
				Analysis: &analysis,
				ReplayFailure: &atlasreport.MigrateLintReplayFailure{
					File:  "1.sql",
					Error: "executing statement: " + replayAnswer,
				},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(replayTextDurationRe.ReplaceAllString(out.String(), "${1}DUR"), qt.Equals, test.wantHeader+"\n"+
				"\n"+
				"  Error: executing statement: "+replayAnswer+"\n"+
				"\n"+
				"  -------------------------\n"+
				"  -- DUR\n"+
				"  -- 1 version with errors\n")
		})
	}
}

// TestNewMigrateLint_ReplayFailureReportsTheFailingFileAlone pins the model a
// --format template reads: the one failing file with its error, and the
// failed replay step after the steps that ran. The fixture's last file drops a
// table, so a model that kept the analysis would list that file with DS102.
func TestNewMigrateLint_ReplayFailureReportsTheFailingFileAlone(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeReplayFixture(c, replayFixture(), migrationlint.VersionSelection{Versions: []int64{3}, Restricted: true})
	const failure = "executing statement: " + replayAnswer

	report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{
		Analysis:      &analysis,
		Integrity:     atlasreport.MigrateLintIntegrity{Checked: true},
		Error:         "error validating migration SQL on dev database: the whole chain",
		ReplayFailure: &atlasreport.MigrateLintReplayFailure{File: "1.sql", Error: failure},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Steps, qt.DeepEquals, []atlasreport.MigrateLintStep{
		{Name: "Migration Integrity Check", Text: "File atlas.sum is valid"},
		{Name: "Detect New Migration Files", Text: "Found 1 new migration files (from 3 total)"},
		{Name: "Replay Migration Files", Text: "Failed loading changes on dev database", Error: failure},
	})
	c.Assert(report.Files, qt.HasLen, 1)
	c.Assert(report.Files[0].Name, qt.Equals, "1.sql")
	c.Assert(report.Files[0].Error, qt.Equals, failure)
	c.Assert(report.Files[0].Text, qt.Equals, "")
	c.Assert(report.Files[0].Reports, qt.IsNil)
	c.Assert(report.Files[0].Findings, qt.IsNil)
}
