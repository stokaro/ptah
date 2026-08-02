package atlasreport_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/migration/migrator"
)

func migrateDownReportFS() fstest.MapFS {
	return fstest.MapFS{
		"1_init.sql":           {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
		"1_init.down.sql":      {Data: []byte("DROP TABLE users;")},
		"2_add_email.sql":      {Data: []byte("ALTER TABLE users ADD COLUMN email TEXT;")},
		"2_add_email.down.sql": {Data: []byte("ALTER TABLE users DROP COLUMN email;")},
	}
}

func migrateDownReportMigrations() []*migrator.Migration {
	return []*migrator.Migration{
		{Version: 1, UpSQL: "CREATE TABLE users (id INTEGER PRIMARY KEY);", DownSQL: "DROP TABLE users;"},
		{Version: 2, UpSQL: "ALTER TABLE users ADD COLUMN email TEXT;", DownSQL: "ALTER TABLE users DROP COLUMN email;"},
	}
}

func TestWriteMigrateDownFormat_CustomTemplate(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := atlasreport.WriteMigrateDownFormat(
		&out,
		`{{ .Driver }}|{{ .Dir }}|{{ len .Planned }}|{{ len .Reverted }}|{{ .Current }}|{{ .Target }}|{{ .Total }}|{{ index (index .Reverted 0).Applied 0 }}`,
		atlasreport.MigrateDownResultOptions{
			Driver:           "sqlite",
			URL:              "sqlite://down.db",
			Dir:              "file://migrations",
			FS:               migrateDownReportFS(),
			Migrations:       migrateDownReportMigrations(),
			PlannedVersions:  []int64{2},
			RevertedVersions: []int64{2},
			CurrentVersion:   2,
			TargetVersion:    1,
			Reverted:         true,
			StartedAt:        time.Unix(100, 0).UTC(),
			EndedAt:          time.Unix(101, 0).UTC(),
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals,
		"sqlite|file://migrations|1|1|2|1|1|ALTER TABLE users DROP COLUMN email")
}

func TestWriteMigrateDownFormat_JSONExposesAtlasFields(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := atlasreport.WriteMigrateDownFormat(&out, "{{ json . }}", atlasreport.MigrateDownResultOptions{
		Driver:           "sqlite",
		URL:              "sqlite://down.db?password=hunter2",
		Dir:              "file://migrations",
		FS:               migrateDownReportFS(),
		Migrations:       migrateDownReportMigrations(),
		PlannedVersions:  []int64{2, 1},
		RevertedVersions: []int64{2, 1},
		CurrentVersion:   2,
		TargetVersion:    0,
		Reverted:         true,
		StartedAt:        time.Unix(100, 0).UTC(),
		EndedAt:          time.Unix(101, 0).UTC(),
	})
	c.Assert(err, qt.IsNil)

	var report struct {
		Driver  string `json:"Driver"`
		Planned []struct {
			Name        string `json:"Name"`
			Version     string `json:"Version"`
			Description string `json:"Description"`
		} `json:"Planned"`
		Reverted []struct {
			Name    string   `json:"Name"`
			Version string   `json:"Version"`
			Applied []string `json:"Applied"`
		} `json:"Reverted"`
		Current string `json:"Current"`
		Target  string `json:"Target"`
		Total   int    `json:"Total"`
		Error   string `json:"Error"`
		URL     struct {
			RawQuery string `json:"RawQuery"`
		} `json:"URL"`
	}
	c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
	c.Assert(report.Driver, qt.Equals, "sqlite")
	c.Assert(report.Planned, qt.HasLen, 2)
	// Planned and Reverted stay in revert order: newest first.
	c.Assert(report.Planned[0].Name, qt.Equals, "2_add_email.sql")
	c.Assert(report.Planned[0].Version, qt.Equals, "2")
	c.Assert(report.Planned[0].Description, qt.Equals, "add_email")
	c.Assert(report.Reverted, qt.HasLen, 2)
	c.Assert(report.Reverted[0].Applied, qt.DeepEquals, []string{"ALTER TABLE users DROP COLUMN email"})
	c.Assert(report.Reverted[1].Applied, qt.DeepEquals, []string{"DROP TABLE users"})
	c.Assert(report.Current, qt.Equals, "2")
	// Rolling back everything empties the revision history, so Target is empty.
	c.Assert(report.Target, qt.Equals, "")
	c.Assert(report.Total, qt.Equals, 2)
	c.Assert(report.Error, qt.Equals, "")
	// Sensitive URL query values are redacted like the other Atlas reports.
	c.Assert(report.URL.RawQuery, qt.Equals, "password=xxxxx")
}

func TestWriteMigrateDownFormat_DryRunHasPlannedOnly(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := atlasreport.WriteMigrateDownFormat(
		&out,
		`{{ len .Planned }}|{{ len .Reverted }}|{{ .Current }}|{{ .Target }}`,
		atlasreport.MigrateDownResultOptions{
			Driver:          "sqlite",
			URL:             "sqlite://down.db",
			Dir:             "file://migrations",
			FS:              migrateDownReportFS(),
			Migrations:      migrateDownReportMigrations(),
			PlannedVersions: []int64{2},
			CurrentVersion:  2,
			TargetVersion:   1,
			Reverted:        false,
			StartedAt:       time.Unix(100, 0).UTC(),
			EndedAt:         time.Unix(101, 0).UTC(),
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1|0|2|1")
}

func TestWriteMigrateDownFormat_FailureReportsPartialRevertWithError(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	downErr := &migrator.MigrationExecutionError{
		Err:            errors.New("no such table: users"),
		Statement:      "DROP TABLE users",
		StatementIndex: 1,
	}

	err := atlasreport.WriteMigrateDownFormat(&out, "{{ json . }}", atlasreport.MigrateDownResultOptions{
		Driver:           "sqlite",
		URL:              "sqlite://down.db",
		Dir:              "file://migrations",
		FS:               migrateDownReportFS(),
		Migrations:       migrateDownReportMigrations(),
		PlannedVersions:  []int64{2, 1},
		RevertedVersions: []int64{2},
		CurrentVersion:   2,
		TargetVersion:    0,
		Reverted:         true,
		StartedAt:        time.Unix(100, 0).UTC(),
		EndedAt:          time.Unix(101, 0).UTC(),
		ErrorText:        "failed to revert migration 1: no such table: users",
		DownError:        downErr,
	})
	c.Assert(err, qt.IsNil)

	var report struct {
		Reverted []struct {
			Version string   `json:"Version"`
			Applied []string `json:"Applied"`
			Error   *struct {
				Stmt string `json:"Stmt"`
				Text string `json:"Text"`
			} `json:"Error"`
		} `json:"Reverted"`
		Error string `json:"Error"`
	}
	c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
	// The failed file appears after the cleanly reverted prefix, carrying the
	// failing statement and its error text.
	c.Assert(report.Reverted, qt.HasLen, 2)
	c.Assert(report.Reverted[0].Version, qt.Equals, "2")
	c.Assert(report.Reverted[0].Error, qt.IsNil)
	c.Assert(report.Reverted[1].Version, qt.Equals, "1")
	c.Assert(report.Reverted[1].Error, qt.IsNotNil)
	c.Assert(report.Reverted[1].Error.Stmt, qt.Equals, "DROP TABLE users")
	c.Assert(report.Reverted[1].Error.Text, qt.Equals, "no such table: users")
	c.Assert(report.Error, qt.Equals, "failed to revert migration 1: no such table: users")
}

func TestWriteMigrateDownFormat_RequiresFilesystem(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := atlasreport.WriteMigrateDownFormat(&out, "{{ json . }}", atlasreport.MigrateDownResultOptions{})

	c.Assert(err, qt.ErrorMatches, `migrate down format requires migration filesystem`)
}

func TestValidateMigrateDownTemplate_RejectsInvalidTemplate(t *testing.T) {
	c := qt.New(t)

	err := atlasreport.ValidateMigrateDownTemplate("{{ .Broken")

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
}
