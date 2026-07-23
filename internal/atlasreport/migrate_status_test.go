package atlasreport_test

import (
	"bytes"
	"testing"
	"testing/fstest"

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
		})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "PENDING|1|2|2|1|1|2_add_email.sql")
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
