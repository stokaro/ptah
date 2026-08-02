package atlasreport_test

import (
	"bytes"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	migrationlint "go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestWriteMigrateLintFormat_CustomTemplate(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id integer);")},
		"2_drop_users.sql":   {Data: []byte("DROP TABLE users;")},
	}
	analysis, err := migrationlint.AnalyzeFS(fsys, migrationlint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
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
	analysis, err := migrationlint.AnalyzeFS(fsys, migrationlint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
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
	analysis, err := migrationlint.AnalyzeFS(fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
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
	analysis, err := migrationlint.AnalyzeFS(fsys, migrationlint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
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
	analysis, err := migrationlint.AnalyzeFS(fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
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
	analysis, err := migrationlint.AnalyzeFS(fsys, migrationlint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
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
	analysis, err := migrationlint.AnalyzeFS(fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
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

func TestNewMigrateLint_LoadsSemanticChangeCountForMultiActionAlter(t *testing.T) {
	c := qt.New(t)
	analysis, err := migrationlint.AnalyzeFS(fstest.MapFS{
		"1_alter.sql": {Data: []byte("ALTER TABLE users ADD COLUMN a INTEGER, ADD COLUMN b INTEGER;\n")},
	}, migrationlint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "sqlite",
	})
	c.Assert(err, qt.IsNil)

	report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{Analysis: &analysis})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Steps, qt.HasLen, 3)
	c.Assert(report.Steps[1].Name, qt.Equals, "Replay Migration Files")
	// One ALTER statement expresses two schema changes: counting the file or
	// statement would report 1.
	c.Assert(report.Steps[1].Text, qt.Equals, "Loaded 2 changes on dev database")
}

func TestNewMigrateLint_LoadsZeroChangesForNonDDLFile(t *testing.T) {
	c := qt.New(t)
	analysis, err := migrationlint.AnalyzeFS(fstest.MapFS{
		"1_seed.sql": {Data: []byte("INSERT INTO users (id) VALUES (1);\n")},
	}, migrationlint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "sqlite",
	})
	c.Assert(err, qt.IsNil)

	report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{Analysis: &analysis})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Steps[0].Text, qt.Equals, "Found 1 new migration files (from 1 total)")
	// The file has a statement but no structural change.
	c.Assert(report.Steps[1].Text, qt.Equals, "Loaded 0 changes on dev database")
}

func TestNewMigrateLint_LoadsSemanticChangeCountForMixedFile(t *testing.T) {
	c := qt.New(t)
	analysis, err := migrationlint.AnalyzeFS(fstest.MapFS{
		"1_mixed.sql": {Data: []byte(
			"CREATE TABLE t (id INTEGER);\n" +
				"INSERT INTO t (id) VALUES (1);\n" +
				"ALTER TABLE t ADD COLUMN a INTEGER, ADD COLUMN b INTEGER;\n")},
	}, migrationlint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "sqlite",
	})
	c.Assert(err, qt.IsNil)

	report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{Analysis: &analysis})

	c.Assert(err, qt.IsNil)
	// Three statements, one operational: 1 (CREATE) + 0 (INSERT) + 2 (ALTER).
	c.Assert(report.Steps[1].Text, qt.Equals, "Loaded 3 changes on dev database")
}

func TestNewMigrateLint_LoadsOneChangePerSingleStatementFixtureFile(t *testing.T) {
	c := qt.New(t)
	analysis, err := migrationlint.AnalyzeFS(fstest.MapFS{
		"1_create_users.sql":    {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
		"2_create_accounts.sql": {Data: []byte("CREATE TABLE accounts (id INTEGER);\n")},
	}, migrationlint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "sqlite",
	})
	c.Assert(err, qt.IsNil)

	report, err := atlasreport.NewMigrateLint(atlasreport.MigrateLintOptions{Analysis: &analysis})

	c.Assert(err, qt.IsNil)
	// Imported one-statement-per-change fixtures keep parity: two files, each a
	// single CREATE, is two changes.
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
	analysis, err := migrationlint.AnalyzeFS(fstest.MapFS{
		"1_empty.sql": {Data: []byte("-- no changes\n")},
	}, migrationlint.Options{DirFormat: migrator.MigrationDirFormatAtlas})
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
	sum, err := migratesum.ComputeWithFormat(fsys, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	fsys[migratesum.AtlasFileName] = &fstest.MapFile{Data: sum.Bytes()}
	snapshot, err := migrationsnapshot.Capture(fsys)
	c.Assert(err, qt.IsNil)
	fsys["1_create_users.sql"].Data = []byte("DROP TABLE users;\n")

	integrity, err := atlasreport.InspectMigrateLintIntegrity(snapshot)
	c.Assert(err, qt.IsNil)
	analysis, err := migrationlint.AnalyzeFS(snapshot, migrationlint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
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
	integrity, err := atlasreport.InspectMigrateLintIntegrity(fsys)
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
