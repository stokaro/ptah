package atlasreport_test

import (
	"bytes"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasmigrateimport"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/migratesum"
	"ptah.run/internal/migrationsnapshot"
	migrationlint "ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// Every fixture below analyzes under CompatibilityProfileAtlas, which is what
// `ptah-compat migrate lint` passes (internal/cli/atlas/migrate_lint.go). The renderer in
// this package only ever receives an analysis produced that way, and a fixture
// built from the native profile carries a different finding set -- so an
// assertion on one is not an assertion about anything this package renders.

func TestWriteMigrateLintFormat_CustomTemplate(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id integer);")},
		"2_drop_users.sql":   {Data: []byte("DROP TABLE users;")},
	}
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatAtlas,
		Selection: migrationlint.VersionSelection{
			Versions:   []int64{2},
			Restricted: true,
		},
	})
	c.Assert(err, qt.IsNil)
	redactionURL := "postgres://app:" + "secret" + "@db.local/app?token=" + "secret" + "&sslmode=disable"
	var out bytes.Buffer

	err = atlasreport.WriteMigrateLintFormat(&out,
		`{{ .Env.Driver }}|{{ len .Files }}|{{ (index .Files 0).Name }}|{{ len (index .Files 0).Findings }}|{{ len .Steps }}|{{ (index .Steps 0).Text }}`,
		atlasreport.MigrateLintOptions{
			Driver:   "sqlite",
			URL:      redactionURL,
			Dir:      "/migrations",
			Analysis: &analysis,
		})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "sqlite|1|2_drop_users.sql|1|3|Found 1 new migration files (from 2 total)")
}

func TestWriteMigrateLintFormat_JSONFiles(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id integer);")},
	}
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)
	var out bytes.Buffer

	err = atlasreport.WriteMigrateLintFormat(&out, `{{ json .Files }}`, atlasreport.MigrateLintOptions{
		Analysis: &analysis,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `"Name":"1_create_users.sql"`)
	c.Assert(out.String(), qt.Contains, `"Text":"CREATE TABLE users`)
}

func TestNewMigrateLint_OmitsAtlasIgnoredFilesAndCounts(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_ignored.sql": {
			Data: []byte("-- atlas:nolint\n\nDROP TABLE users;\n"),
		},
		"2_selected.sql": {
			Data: []byte("CREATE TABLE accounts (id integer);\n"),
		},
	}
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)

	report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{
		Analysis: &analysis,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Files, qt.HasLen, 1)
	c.Assert(report.Files[0].Name, qt.Equals, "2_selected.sql")
	c.Assert(report.Steps, qt.HasLen, 3)
	c.Assert(report.Steps[0].Text, qt.Equals, "Found 1 new migration files (from 1 total)")
}

func TestNewMigrateLint_DuplicateBasenamesKeepFindingsScoped(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"a/1_change.sql": {Data: []byte("DROP TABLE users;")},
		"b/1_change.sql": {Data: []byte("SELECT 1;")},
	}
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)

	report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{
		Analysis: &analysis,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Files, qt.HasLen, 2)
	c.Assert(report.Files[0].Name, qt.Equals, "a/1_change.sql")
	c.Assert(report.Files[0].Findings, qt.HasLen, 1)
	c.Assert(report.Files[1].Name, qt.Equals, "b/1_change.sql")
	c.Assert(report.Files[1].Findings, qt.HasLen, 0)
}

func TestNewMigrateLint_MapsAtlasDiagnosticCodes(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_change.sql": {
			Data: []byte(`
DROP TABLE users;
ALTER TABLE accounts DROP COLUMN legacy;
ALTER TABLE accounts ADD COLUMN tenant_id INTEGER NOT NULL;
`),
		},
	}
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatAtlas,
		Dialect:       "sqlite",
	})
	c.Assert(err, qt.IsNil)

	report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{
		Analysis: &analysis,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Files, qt.HasLen, 1)
	c.Assert(report.Files[0].Findings, qt.HasLen, 3)
	c.Assert(report.Files[0].Findings[0].Rule, qt.Equals, "DS102")
	c.Assert(report.Files[0].Findings[1].Rule, qt.Equals, "DS103")
	c.Assert(report.Files[0].Findings[2].Rule, qt.Equals, "MF103")
}

func TestNewMigrateLint_OrdersMigrationsByVersion(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"10_later.sql":  {Data: []byte("SELECT 10;")},
		"2_earlier.sql": {Data: []byte("SELECT 2;")},
	}
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)

	report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{
		Analysis: &analysis,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Files, qt.HasLen, 2)
	c.Assert(report.Files[0].Name, qt.Equals, "2_earlier.sql")
	c.Assert(report.Files[1].Name, qt.Equals, "10_later.sql")
}

func TestNewMigrateLint_PathPrefixCannotCrossAttachFindings(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_change.sql":            {Data: []byte("DROP TABLE users;")},
		"migrations/1_change.sql": {Data: []byte("SELECT 1;")},
	}
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatAtlas,
		PathPrefix:    "migrations",
	})
	c.Assert(err, qt.IsNil)

	report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{
		Analysis: &analysis,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Files, qt.HasLen, 2)
	c.Assert(report.Files[0].Name, qt.Equals, "1_change.sql")
	c.Assert(report.Files[0].Findings, qt.HasLen, 1)
	c.Assert(report.Files[0].Findings[0].Rule, qt.Equals, "DS102")
	c.Assert(report.Files[1].Name, qt.Equals, "migrations/1_change.sql")
	c.Assert(report.Files[1].Findings, qt.HasLen, 0)
}

// TestNewMigrateLint_LoadStepCountsReplayedFiles pins the replay step to the
// number of files replayed, whatever each one changes. Measured against the
// pinned community binary v1.3.0 on SQLite: one version that creates two
// tables, one that only inserts a row and one rebuild of four statements each
// print "Loaded 1 changes on dev database", and two versions print 2.
//
// Counting schema changes instead turns the first three rows red: they hold
// two, zero and three changes in one file.
func TestNewMigrateLint_LoadStepCountsReplayedFiles(t *testing.T) {
	tests := []struct {
		name  string
		files fstest.MapFS
		found string
		want  string
	}{
		{
			name: "one file creating two tables",
			files: fstest.MapFS{
				"1_create.sql": {Data: []byte("CREATE TABLE a (id INTEGER);\nCREATE TABLE b (id INTEGER);\n")},
			},
			found: "Found 1 new migration files (from 1 total)",
			want:  "Loaded 1 changes on dev database",
		},
		{
			name: "one file with no structural change",
			files: fstest.MapFS{
				"1_seed.sql": {Data: []byte("INSERT INTO users (id) VALUES (1);\n")},
			},
			found: "Found 1 new migration files (from 1 total)",
			want:  "Loaded 1 changes on dev database",
		},
		{
			name: "one file mixing a create, an insert and an alter",
			files: fstest.MapFS{
				"1_mixed.sql": {Data: []byte("CREATE TABLE t (id INTEGER);\n" +
					"INSERT INTO t (id) VALUES (1);\n" +
					"ALTER TABLE t ADD COLUMN a INTEGER, ADD COLUMN b INTEGER;\n")},
			},
			found: "Found 1 new migration files (from 1 total)",
			want:  "Loaded 1 changes on dev database",
		},
		{
			name: "two files",
			files: fstest.MapFS{
				"1_create_users.sql":    {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
				"2_create_accounts.sql": {Data: []byte("CREATE TABLE accounts (id INTEGER);\n")},
			},
			found: "Found 2 new migration files (from 2 total)",
			want:  "Loaded 2 changes on dev database",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), test.files, migrationlint.Options{
				Compatibility: migrationlint.CompatibilityProfileAtlas,
				DirFormat:     migrationfile.DirFormatAtlas,
				Dialect:       "sqlite",
			})
			c.Assert(err, qt.IsNil)

			report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{Analysis: &analysis})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Steps[0].Text, qt.Equals, test.found)
			c.Assert(report.Steps[1].Name, qt.Equals, "Replay Migration Files")
			c.Assert(report.Steps[1].Text, qt.Equals, test.want)
		})
	}
}

// TestWriteMigrateLintText_SummaryCountsAnAtlasSQLiteReplacementOnce drives the
// SQLite table replacement from the analysis to the summary line, the line
// the conformance probe compares. The pinned community binary v1.3.0 prints
// "1 schema change" for the `new_users` replacement and "4 schema changes" for
// the same steps under the name users_tmp.
func TestWriteMigrateLintText_SummaryCountsAnAtlasSQLiteReplacementOnce(t *testing.T) {
	tests := []struct {
		name    string
		scratch string
		want    string
	}{
		{name: "named new_users", scratch: "new_users", want: "  -- 1 schema change\n"},
		{name: "named users_tmp", scratch: "users_tmp", want: "  -- 4 schema changes\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql := "PRAGMA foreign_keys = off;\n" +
				"CREATE TABLE " + test.scratch + " (id integer NOT NULL, PRIMARY KEY (id));\n" +
				"INSERT INTO " + test.scratch + " (id) SELECT id FROM users;\n" +
				"DROP TABLE users;\n" +
				"ALTER TABLE " + test.scratch + " RENAME TO users;\n" +
				"PRAGMA foreign_keys = on;\n"
			analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fstest.MapFS{
				"1_replace.sql": {Data: []byte(sql)},
			}, migrationlint.Options{
				Compatibility: migrationlint.CompatibilityProfileAtlas,
				DirFormat:     migrationfile.DirFormatAtlas,
				Dialect:       "sqlite",
			})
			c.Assert(err, qt.IsNil)
			var out bytes.Buffer

			err = atlasreport.WriteMigrateLintText(&out, atlasreport.MigrateLintOptions{Analysis: &analysis})

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Contains, test.want)
		})
	}
}

func TestNewMigrateLint_IncludesAtlasRepeatableFiles(t *testing.T) {
	c := qt.New(t)
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
		"2R_drop_users.sql":  {Data: []byte("DROP TABLE users;\n")},
	}, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatAtlas,
		Dialect:       "sqlite",
	})
	c.Assert(err, qt.IsNil)

	report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{Analysis: &analysis})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Files, qt.HasLen, 2)
	c.Assert(report.Files[0].Name, qt.Equals, "1_create_users.sql")
	c.Assert(report.Files[1].Name, qt.Equals, "2R_drop_users.sql")
	c.Assert(report.Files[1].Findings, qt.HasLen, 1)
	c.Assert(report.Files[1].Findings[0].Rule, qt.Equals, "DS102")
	c.Assert(report.Steps[0].Text, qt.Equals, "Found 2 new migration files (from 2 total)")
	c.Assert(report.Steps[1].Text, qt.Equals, "Loaded 2 changes on dev database")
}

func TestWriteMigrateLintFormat_RedactsSensitiveURL(t *testing.T) {
	c := qt.New(t)
	redactionURL := "postgres://app:" + "secret" + "@db.local/app?token=" + "secret" +
		"&access_token=" + "secret" +
		"&auth-token=" + "secret" +
		"&api_key=" + "secret" +
		"&client_secret=" + "secret" +
		"&sslmode=disable"
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fstest.MapFS{
		"1_empty.sql": {Data: []byte("-- no changes\n")},
	}, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)
	var out bytes.Buffer

	err = atlasreport.WriteMigrateLintFormat(&out, `{{ .Env.URL }}`, atlasreport.MigrateLintOptions{
		URL:      redactionURL,
		Analysis: &analysis,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "postgres://app@db.local/app?access_token=xxxxx&api_key=xxxxx&auth-token=xxxxx&client_secret=xxxxx&sslmode=disable&token=xxxxx")
}

func TestWriteMigrateLintFormat_ValidAtlasSumAddsIntegrityStep(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id integer);\n")},
	}
	sum, err := migratesum.ComputeWithFormat(fsys, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	fsys[migratesum.AtlasFileName] = &fstest.MapFile{Data: sum.Bytes()}
	snapshot, err := migrationsnapshot.Capture(fsys)
	c.Assert(err, qt.IsNil)
	fsys["1_create_users.sql"].Data = []byte("DROP TABLE users;\n")

	covered, err := atlasmigrateimport.SumFileNames(snapshot, atlasmigrateimport.FormatAtlas)
	c.Assert(err, qt.IsNil)
	integrity, err := atlasreport.InspectMigrateLintIntegrity(snapshot, covered)
	c.Assert(err, qt.IsNil)
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), snapshot, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrationfile.DirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)
	var out bytes.Buffer

	err = atlasreport.WriteMigrateLintFormat(&out, `{{ len .Steps }}|{{ (index .Steps 0).Text }}`, atlasreport.MigrateLintOptions{
		Analysis:  &analysis,
		Integrity: integrity,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "4|File atlas.sum is valid")
}

func TestWriteMigrateLintFormat_InvalidAtlasSumRendersIntegrityFailure(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_create_users.sql":     {Data: []byte("CREATE TABLE users (id integer);\n")},
		migratesum.AtlasFileName: {Data: []byte("stale\n")},
	}
	covered, err := atlasmigrateimport.SumFileNames(fsys, atlasmigrateimport.FormatAtlas)
	c.Assert(err, qt.IsNil)
	integrity, err := atlasreport.InspectMigrateLintIntegrity(fsys, covered)
	c.Assert(err, qt.IsNil)
	var out bytes.Buffer

	err = atlasreport.WriteMigrateLintFormat(&out,
		`{{ len .Steps }}|{{ (index .Steps 0).Text }}|{{ (index .Files 0).Name }}|{{ (index .Files 0).Error }}`,
		atlasreport.MigrateLintOptions{
			Integrity: integrity,
		})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1|File atlas.sum is invalid|atlas.sum|checksum mismatch")
}
