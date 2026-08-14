package atlasreport_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/migration/migrator"
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

func TestWriteMigrateApplyFormat_ConvertedFilesUseExactSelectedIdentities(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "converted-format.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	fsys := fstest.MapFS{
		"10_dotted.sql": {Data: []byte("CREATE TABLE dotted_report (id INTEGER PRIMARY KEY);")},
		"20_repeat.sql": {Data: []byte("CREATE TABLE repeat_report (id INTEGER PRIMARY KEY);")},
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "1.5", 20: ""}),
	)
	c.Assert(err, qt.IsNil)
	var out bytes.Buffer

	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ range .Applied }}{{ printf "%q:%s;" .Version .Name }}{{ end }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn:             conn,
			FS:               fsys,
			Status:           &migrator.MigrationStatus{},
			Migrations:       provider.Migrations(),
			SelectedVersions: []int64{10, 20},
			SelectedKeys:     []string{"1.5", ""},
			Applied:          true,
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, `"1.5":10_dotted.sql;"":20_repeat.sql;`)
}

func TestWriteMigrateApplyFormat_ConvertedFailureAttachesToExactIdentity(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "converted-failure.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	fsys := fstest.MapFS{
		"10_dotted.sql": {Data: []byte("INVALID SQL;")},
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "1.5"}),
	)
	c.Assert(err, qt.IsNil)
	applyErr := fmt.Errorf("failed to apply migration 10: %w", &migrator.MigrationExecutionError{
		Err:            errors.New("syntax error"),
		Statement:      "INVALID SQL",
		StatementIndex: 1,
		Total:          1,
	})
	var out bytes.Buffer

	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ range .Applied }}{{ printf "%q:%s" .Version .Error.Text }}{{ end }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn:             conn,
			FS:               fsys,
			Status:           &migrator.MigrationStatus{},
			Migrations:       provider.Migrations(),
			SelectedVersions: []int64{10},
			SelectedKeys:     []string{"1.5"},
			ApplyError:       applyErr,
			Applied:          true,
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, `"1.5":syntax error`)
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

func TestWriteMigrateApplyFormat_JSONPreservesExactEmptyCurrentAndTarget(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "empty-identity.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var out bytes.Buffer
	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ json . }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn:             conn,
			FS:               fstest.MapFS{"10_only.sql": {Data: []byte("SELECT 1;")}},
			SelectedVersions: []int64{10},
			SelectedKeys:     []string{""},
			CurrentKeySet:    true,
			Applied:          true,
		},
	)

	c.Assert(err, qt.IsNil)
	var got struct {
		Current *string
		Target  *string
		Pending []struct{ Version *string }
		Applied []struct{ Version *string }
	}
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(got.Current, qt.IsNotNil)
	c.Assert(*got.Current, qt.Equals, "")
	c.Assert(got.Target, qt.IsNotNil)
	c.Assert(*got.Target, qt.Equals, "")
	c.Assert(got.Pending, qt.HasLen, 1)
	c.Assert(got.Pending[0].Version, qt.IsNotNil)
	c.Assert(*got.Pending[0].Version, qt.Equals, "")
	c.Assert(got.Applied, qt.HasLen, 1)
	c.Assert(got.Applied[0].Version, qt.IsNotNil)
	c.Assert(*got.Applied[0].Version, qt.Equals, "")
}

func TestWriteMigrateApplyFormat_JSONCurrentPresenceIsAuthoritative(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "current-presence.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	render := func(opts atlasreport.MigrateApplyResultOptions) map[string]json.RawMessage {
		var out bytes.Buffer
		opts.Conn = conn
		opts.FS = fstest.MapFS{}
		opts.Status = &migrator.MigrationStatus{}
		c.Assert(atlasreport.WriteMigrateApplyFormat(&out, `{{ json . }}`, opts), qt.IsNil)
		var got map[string]json.RawMessage
		c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
		return got
	}

	absent := render(atlasreport.MigrateApplyResultOptions{})
	c.Assert(absent["Current"], qt.IsNil)
	c.Assert(absent["Target"], qt.IsNil)

	stale := render(atlasreport.MigrateApplyResultOptions{CurrentKey: "stale"})
	c.Assert(stale["Current"], qt.IsNil)
	c.Assert(stale["Target"], qt.IsNil)

	targetOnly := render(atlasreport.MigrateApplyResultOptions{
		SelectedVersions: []int64{10},
		SelectedKeys:     []string{""},
	})
	c.Assert(targetOnly["Current"], qt.IsNil)
	c.Assert(string(targetOnly["Target"]), qt.Equals, `""`)

	nonempty := render(atlasreport.MigrateApplyResultOptions{
		CurrentKey:    "1.5",
		CurrentKeySet: true,
	})
	c.Assert(string(nonempty["Current"]), qt.Equals, `"1.5"`)
	c.Assert(string(nonempty["Target"]), qt.Equals, `"1.5"`)
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
