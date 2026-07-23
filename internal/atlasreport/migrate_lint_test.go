package atlasreport_test

import (
	"bytes"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/migratesum"
	migrationlint "github.com/stokaro/ptah/migration/lint"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestWriteMigrateLintFormat_CustomTemplate(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id integer);")},
		"2_drop_users.sql":   {Data: []byte("DROP TABLE users;")},
	}
	redactionURL := "postgres://app:" + "secret" + "@db.local/app?token=" + "secret" + "&sslmode=disable"
	var out bytes.Buffer

	err := atlasreport.WriteMigrateLintFormat(&out,
		`{{ .Env.Driver }}|{{ len .Files }}|{{ (index .Files 0).Name }}|{{ len (index .Files 0).Findings }}|{{ len .Steps }}`,
		atlasreport.MigrateLintOptions{
			Driver:   "sqlite",
			URL:      redactionURL,
			Dir:      "/migrations",
			FS:       fsys,
			Versions: []int64{2},
			Findings: []migrationlint.Finding{
				{
					Rule:     "DS101",
					Severity: migrationlint.SeverityError,
					File:     "2_drop_users.sql",
					Message:  "DROP TABLE permanently deletes table data",
				},
			},
		})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "sqlite|1|2_drop_users.sql|1|3")
}

func TestWriteMigrateLintFormat_JSONFiles(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id integer);")},
	}
	var out bytes.Buffer

	err := atlasreport.WriteMigrateLintFormat(&out, `{{ json .Files }}`, atlasreport.MigrateLintOptions{
		FS: fsys,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `"Name":"1_create_users.sql"`)
	c.Assert(out.String(), qt.Contains, `"Text":"CREATE TABLE users`)
}

func TestWriteMigrateLintFormat_RedactsSensitiveURL(t *testing.T) {
	c := qt.New(t)
	redactionURL := "postgres://app:" + "secret" + "@db.local/app?token=" + "secret" +
		"&access_token=" + "secret" +
		"&auth-token=" + "secret" +
		"&api_key=" + "secret" +
		"&client_secret=" + "secret" +
		"&sslmode=disable"
	var out bytes.Buffer

	err := atlasreport.WriteMigrateLintFormat(&out, `{{ .Env.URL }}`, atlasreport.MigrateLintOptions{
		URL: redactionURL,
		FS:  fstest.MapFS{},
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
	integrity, err := atlasreport.InspectMigrateLintIntegrity(fsys)
	c.Assert(err, qt.IsNil)
	var out bytes.Buffer

	err = atlasreport.WriteMigrateLintFormat(&out, `{{ len .Steps }}|{{ (index .Steps 0).Text }}`, atlasreport.MigrateLintOptions{
		FS:        fsys,
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
			FS:        fsys,
			Integrity: integrity,
		})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1|File atlas.sum is invalid|atlas.sum|checksum mismatch")
}
