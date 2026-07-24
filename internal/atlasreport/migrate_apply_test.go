package atlasreport_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func TestWriteMigrateApplyFormat_JSONShape(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "shape.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
		"2_add_email.sql":    {Data: []byte("ALTER TABLE users ADD COLUMN email TEXT;")},
	}
	var out bytes.Buffer
	startedAt := time.Unix(100, 0).UTC()
	endedAt := time.Unix(101, 0).UTC()

	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ json . }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn: conn,
			FS:   fsys,
			Dir:  "file://migrations",
			URL:  "sqlite://user:secret@" + dbPath + "?password=hidden&token=private",
			Status: &migrator.MigrationStatus{
				CurrentVersion:    0,
				PendingMigrations: []int64{1, 2},
			},
			Migrations: []*migrator.Migration{
				{
					Version: 1,
					UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY);",
				},
				{
					Version: 2,
					UpSQL:   "ALTER TABLE users ADD COLUMN email TEXT;",
				},
			},
			SelectedVersions: []int64{1},
			Applied:          true,
			StartedAt:        startedAt,
			EndedAt:          endedAt,
		},
	)

	c.Assert(err, qt.IsNil)
	var got migrateApplyJSONReport
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(got.Driver, qt.Equals, "sqlite")
	c.Assert(got.URL.Scheme, qt.Equals, "sqlite")
	c.Assert(got.URL.Path, qt.Equals, dbPath)
	c.Assert(got.URL.RawQuery, qt.Equals, "password=xxxxx&token=xxxxx")
	c.Assert(got.URL.Schema, qt.Equals, "main")
	c.Assert(got.Dir, qt.Equals, "file://migrations")
	c.Assert(got.Pending, qt.DeepEquals, []migrateApplyJSONFile{
		{Name: "1_create_users.sql", Version: "1", Description: "create_users"},
	})
	c.Assert(got.Applied, qt.HasLen, 1)
	c.Assert(got.Applied[0].Name, qt.Equals, "1_create_users.sql")
	c.Assert(got.Applied[0].Version, qt.Equals, "1")
	c.Assert(got.Applied[0].Description, qt.Equals, "create_users")
	c.Assert(got.Applied[0].Skipped, qt.Equals, 0)
	c.Assert(got.Applied[0].Applied, qt.DeepEquals, []string{"CREATE TABLE users (id INTEGER PRIMARY KEY)"})
	c.Assert(got.Applied[0].Checks, qt.IsNil)
	c.Assert(got.Applied[0].Error, qt.IsNil)
	c.Assert(got.Current, qt.Equals, "")
	c.Assert(got.Target, qt.Equals, "1")
	c.Assert(got.Message, qt.Equals, "Migrated to version 1 from  (1 migrations in total)")
}

func TestWriteMigrateApplyFormat_JSONErrorShape(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "error-shape.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	fsys := fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
	}
	var out bytes.Buffer
	applyErr := &migrator.MigrationExecutionError{
		Err:            errors.New("migration failed"),
		Statement:      "CREATE TABLE users (id INTEGER PRIMARY KEY)",
		StatementIndex: 0,
		Total:          1,
	}

	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ json . }}`,
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
			ErrorText:        "failed to apply migration 1: migration failed",
			ApplyError:       applyErr,
			Applied:          true,
		},
	)

	c.Assert(err, qt.IsNil)
	var got migrateApplyJSONReport
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(got.Error, qt.Equals, "failed to apply migration 1: migration failed")
	c.Assert(got.Applied, qt.HasLen, 1)
	c.Assert(got.Applied[0].Error, qt.IsNotNil)
	c.Assert(got.Applied[0].Error.Stmt, qt.Equals, "CREATE TABLE users (id INTEGER PRIMARY KEY)")
	c.Assert(got.Applied[0].Error.Text, qt.Equals, "migration failed")
	c.Assert(got.Message, qt.Equals, "")
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

type migrateApplyJSONReport struct {
	Driver  string
	URL     atlasReportJSONURL
	Dir     string
	Pending []migrateApplyJSONFile
	Applied []migrateApplyJSONAppliedFile
	Current string
	Target  string
	Error   string
	Message string
}

type migrateApplyJSONFile struct {
	Name        string
	Version     string
	Description string
}

type migrateApplyJSONAppliedFile struct {
	Name        string
	Version     string
	Description string
	Skipped     int
	Applied     []string
	Checks      []any
	Error       *migrateApplyJSONError
}

type migrateApplyJSONError struct {
	Stmt string
	Text string
}

type atlasReportJSONURL struct {
	Scheme   string
	Host     string
	Path     string
	RawQuery string
	Schema   string
}
