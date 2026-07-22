package atlasreport_test

import (
	"bytes"
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestWriteMigrateApplyFormat_CustomTemplate(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "format.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
	}
	var out bytes.Buffer
	startedAt := time.Unix(100, 0).UTC()
	endedAt := time.Unix(101, 0).UTC()

	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ .Driver }}|{{ .Dir }}|{{ len .Pending }}|{{ len .Applied }}|{{ .Target }}|{{ printf "%.12s" (index (index .Applied 0).Applied 0) }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn: conn,
			FS:   fsys,
			Dir:  "file://migrations",
			URL:  "sqlite://" + dbPath,
			Status: &migrator.MigrationStatus{
				CurrentVersion:    0,
				PendingMigrations: []int64{1},
			},
			Migrations: []*migrator.Migration{
				{
					Version: 1,
					UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY);",
				},
			},
			SelectedVersions: []int64{1},
			Applied:          true,
			StartedAt:        startedAt,
			EndedAt:          endedAt,
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "sqlite|file://migrations|1|1|1|CREATE TABLE")
}

func TestWriteMigrateApplyFormat_JSONNoopMessage(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "noop.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
	}
	var out bytes.Buffer

	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ json . }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn: conn,
			FS:   fsys,
			Dir:  "file://migrations",
			URL:  "sqlite://" + dbPath,
			Status: &migrator.MigrationStatus{
				CurrentVersion: 1,
			},
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `"Message":"No migration files to execute"`)
	c.Assert(out.String(), qt.Contains, `"Driver":"sqlite"`)
}

func TestWriteMigrateApplyFormat_RedactsSensitiveURL(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "redacted.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var out bytes.Buffer
	query := url.Values{}
	query.Set("sslmode", "disable")
	query.Set("token", strings.Repeat("t", 6))
	rawURL := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("app", strings.Repeat("s", 6)),
		Host:     "db.local",
		Path:     "/app",
		RawQuery: query.Encode(),
	}

	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ .URL }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn:           conn,
			FS:             fstest.MapFS{},
			URL:            rawURL.String(),
			CurrentVersion: 1,
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "postgres://app@db.local/app?sslmode=disable&token=xxxxx")
}

func TestWriteMigrateApplyFormat_RequiresConnection(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ json . }}`,
		atlasreport.MigrateApplyResultOptions{
			FS:             fstest.MapFS{},
			CurrentVersion: 1,
		},
	)

	c.Assert(err, qt.ErrorMatches, `migrate apply format requires database connection`)
	c.Assert(out.String(), qt.Equals, "")
}

func TestWriteMigrateApplyFormat_RequiresFilesystem(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "missing-fs.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var out bytes.Buffer

	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ json . }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn:           conn,
			CurrentVersion: 1,
		},
	)

	c.Assert(err, qt.ErrorMatches, `migrate apply format requires migration filesystem`)
	c.Assert(out.String(), qt.Equals, "")
}

func TestWriteMigrateApplyFormat_RequiresStatusOrCurrentVersion(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "missing-status.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var out bytes.Buffer

	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ json . }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn: conn,
			FS:   fstest.MapFS{},
		},
	)

	c.Assert(err, qt.ErrorMatches, `migrate apply format requires migration status or current version`)
	c.Assert(out.String(), qt.Equals, "")
}
