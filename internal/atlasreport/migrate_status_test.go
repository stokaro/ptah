package atlasreport_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestWriteMigrateStatusFormat_CustomTemplate(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id integer);")},
		"2_add_email.sql":    {Data: []byte("ALTER TABLE users ADD COLUMN email text;")},
	}
	redactionURL := "postgres://app:" + "secret" + "@db.local/app?token=" + "secret" + "&sslmode=disable"
	var out bytes.Buffer

	err := atlasreport.WriteMigrateStatusFormat(&out,
		`{{ .Status }}|{{ .Current }}|{{ .Next }}|{{ len .Available }}|{{ len .Applied }}|{{ len .Pending }}|{{ (index .Pending 0).Name }}`,
		atlasreport.MigrateStatusOptions{
			Driver: "sqlite",
			URL:    redactionURL,
			Dir:    "file://migrations?format=atlas",
			FS:     fsys,
			Status: &migrator.MigrationStatus{
				CurrentVersion:    1,
				AppliedMigrations: []int64{1},
				PendingMigrations: []int64{2},
				TotalMigrations:   2,
				HasPendingChanges: true,
			},
			AppliedRevisions: []migrator.MigrationRevision{
				{Version: 1, Description: "Create Users", Applied: 1, Total: 1},
			},
		})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "PENDING|1|2|2|1|1|2_add_email.sql")
}

func TestWriteMigrateStatusFormat_ExposesAtlasZeroValueFields(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := atlasreport.WriteMigrateStatusFormat(&out,
		`{{ .Count }}|{{ .Total }}|{{ .Error }}|{{ .SQL }}`,
		atlasreport.MigrateStatusOptions{
			FS: fstest.MapFS{},
			Status: &migrator.MigrationStatus{
				CurrentVersion: 1,
			},
		})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "0|0||")
}

func TestWriteMigrateStatusFormat_RedactsSensitiveURL(t *testing.T) {
	c := qt.New(t)
	redactionURL := "postgres://app:" + "secret" + "@db.local/app?token=" + "secret" +
		"&access_token=" + "secret" +
		"&auth-token=" + "secret" +
		"&api_key=" + "secret" +
		"&client_secret=" + "secret" +
		"&sslmode=disable"
	var out bytes.Buffer

	err := atlasreport.WriteMigrateStatusFormat(&out, `{{ .Env.URL }}`, atlasreport.MigrateStatusOptions{
		URL: redactionURL,
		FS:  fstest.MapFS{},
		Status: &migrator.MigrationStatus{
			CurrentVersion: 1,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "postgres://app@db.local/app?access_token=xxxxx&api_key=xxxxx&auth-token=xxxxx&client_secret=xxxxx&sslmode=disable&token=xxxxx")
}

func TestWriteMigrateStatusFormat_JSONShape(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id integer);")},
		"2_add_email.sql":    {Data: []byte("ALTER TABLE users ADD COLUMN email text;")},
	}
	var out bytes.Buffer

	err := atlasreport.WriteMigrateStatusFormat(&out, `{{ json . }}`, atlasreport.MigrateStatusOptions{
		Driver: "sqlite",
		URL:    "sqlite://user:secret@db.sqlite?password=hidden&token=private",
		Dir:    "file://migrations?format=atlas",
		FS:     fsys,
		Status: &migrator.MigrationStatus{
			CurrentVersion:    1,
			AppliedMigrations: []int64{1},
			PendingMigrations: []int64{2},
			TotalMigrations:   2,
			HasPendingChanges: true,
		},
		AppliedRevisions: []migrator.MigrationRevision{
			{
				Version:         1,
				Description:     "Create Users",
				Applied:         1,
				Total:           1,
				AppliedAt:       time.Unix(100, 0).UTC(),
				ExecutionTime:   50 * time.Millisecond,
				OperatorVersion: "Ptah",
			},
		},
	})

	c.Assert(err, qt.IsNil)
	var got migrateStatusJSONReport
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(got.Env.Driver, qt.Equals, "sqlite")
	c.Assert(got.Env.URL.Scheme, qt.Equals, "sqlite")
	c.Assert(got.Env.URL.Host, qt.Equals, "db.sqlite")
	c.Assert(got.Env.URL.RawQuery, qt.Equals, "password=xxxxx&token=xxxxx")
	c.Assert(got.Env.URL.Schema, qt.Equals, "main")
	c.Assert(got.Env.Dir, qt.Equals, "file://migrations?format=atlas")
	c.Assert(got.Available, qt.DeepEquals, []migrateStatusJSONFile{
		{Name: "1_create_users.sql", Version: "1", Description: "create_users"},
		{Name: "2_add_email.sql", Version: "2", Description: "add_email"},
	})
	c.Assert(got.Applied, qt.DeepEquals, []migrateStatusJSONRevision{
		{
			Version:         "1",
			Description:     "create_users",
			Type:            "applied",
			Applied:         1,
			Total:           1,
			ExecutedAt:      time.Unix(100, 0).UTC(),
			ExecutionTime:   50 * time.Millisecond,
			OperatorVersion: "Ptah",
		},
	})
	c.Assert(got.Pending, qt.DeepEquals, []migrateStatusJSONFile{
		{Name: "2_add_email.sql", Version: "2", Description: "add_email"},
	})
	c.Assert(got.Current, qt.Equals, "1")
	c.Assert(got.Next, qt.Equals, "2")
	c.Assert(got.Status, qt.Equals, "PENDING")
}

func TestWriteMigrateStatusFormat_OmitsDescriptionForVersionOnlyFile(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1.sql": {Data: []byte("CREATE TABLE users (id integer);")},
	}
	var out bytes.Buffer

	err := atlasreport.WriteMigrateStatusFormat(&out, `{{ json . }}`, atlasreport.MigrateStatusOptions{
		FS: fsys,
		Status: &migrator.MigrationStatus{
			PendingMigrations: []int64{1},
			TotalMigrations:   1,
			HasPendingChanges: true,
		},
	})

	c.Assert(err, qt.IsNil)
	var got migrateStatusJSONReport
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(got.Available, qt.DeepEquals, []migrateStatusJSONFile{
		{Name: "1.sql", Version: "1"},
	})
	c.Assert(got.Pending, qt.DeepEquals, []migrateStatusJSONFile{
		{Name: "1.sql", Version: "1"},
	})
}

func TestWriteMigrateStatusFormat_RejectsInvalidTemplate(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := atlasreport.WriteMigrateStatusFormat(&out, `{{ if }}`, atlasreport.MigrateStatusOptions{
		FS: fstest.MapFS{},
		Status: &migrator.MigrationStatus{
			CurrentVersion: 1,
		},
	})

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
	c.Assert(out.String(), qt.Equals, "")
}

type migrateStatusJSONReport struct {
	Env struct {
		Driver string
		URL    atlasReportJSONURL
		Dir    string
	}
	Available []migrateStatusJSONFile
	Applied   []migrateStatusJSONRevision
	Pending   []migrateStatusJSONFile
	Current   string
	Next      string
	Status    string
}

type migrateStatusJSONRevision struct {
	Version         string
	Description     string
	Type            string
	Applied         int
	Total           int
	ExecutedAt      time.Time
	ExecutionTime   time.Duration
	Error           string
	ErrorStmt       string
	OperatorVersion string
}

type migrateStatusJSONFile struct {
	Name        string
	Version     string
	Description string
	Type        string
}
