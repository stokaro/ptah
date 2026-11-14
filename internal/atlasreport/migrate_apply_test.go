package atlasreport_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/testutils"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
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
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
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
		"20_next.sql":   {Data: []byte("CREATE TABLE t (id int);")},
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "1.5"}),
	)
	c.Assert(err, qt.IsNil)
	// The failed migration is the first of two selected, so a run that took
	// the last selected one for the failure attaches it to the wrong file.
	applyErr := fmt.Errorf("context: %w", &migrator.FailedMigrationError{
		Version:    10,
		VersionKey: "1.5",
		Err: &migrator.MigrationExecutionError{
			Err:            errors.New("syntax error"),
			Statement:      "INVALID SQL",
			StatementIndex: 1,
			Total:          1,
		},
	})
	var out bytes.Buffer

	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ range .Applied }}{{ printf "%q" .Version }}{{ with .Error }}:{{ .Text }}{{ end }};{{ end }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn:             conn,
			FS:               fsys,
			Status:           &migrator.MigrationStatus{},
			Migrations:       provider.Migrations(),
			SelectedVersions: []int64{10, 20},
			SelectedKeys:     []string{"1.5", "20"},
			ApplyError:       applyErr,
			Applied:          true,
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, `"1.5":syntax error;`)
}

func TestWriteMigrateApplyFormat_ConvertedCheckFailureNamesExactIdentity(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "converted-check.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	const key = 4611686018427471935
	fsys := fstest.MapFS{
		"4611686018427471935_b.sql": {Data: []byte("-- +ptah check assert=\"SELECT 0\"\nCREATE TABLE b (id int);\n")},
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{key: "1.5"}),
	)
	c.Assert(err, qt.IsNil)
	applyErr := fmt.Errorf("pre-migration check failed for migration %d: %w", int64(key), &migrator.FailedMigrationError{
		Version:    key,
		VersionKey: "1.5",
		Err:        &migrator.CheckFailedError{Version: key, Assert: "SELECT 0"},
	})
	var out bytes.Buffer

	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ range .Applied }}{{ .Error.Text }}|{{ range .Checks }}{{ .Error.Text }}|{{ range .Stmts }}{{ .Error }}{{ end }}{{ end }}{{ end }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn:             conn,
			FS:               fsys,
			Status:           &migrator.MigrationStatus{},
			Migrations:       provider.Migrations(),
			SelectedVersions: []int64{key},
			SelectedKeys:     []string{"1.5"},
			ApplyError:       applyErr,
			Applied:          true,
		},
	)

	// The converted key appears in no file name; every text names the
	// migration by the version the directory holds.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "pre-migration check failed for migration 1.5: "+
		"pre-migration check (unnamed) for migration 1.5 was not satisfied (assert: SELECT 0)|"+
		"pre-migration check (unnamed) for migration 1.5 was not satisfied (assert: SELECT 0)|"+
		"pre-migration check (unnamed) for migration 1.5 was not satisfied (assert: SELECT 0)")
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

func TestWriteMigrateApplyFormat_JSONOmitsEmptyRevisionIdentity(t *testing.T) {
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

	// A Flyway repeatable carries the empty revision identity. The community
	// binary names it by Name alone, and a run that ends on one reports no
	// Current and no Target.
	c.Assert(err, qt.IsNil)
	var got struct {
		Pending []map[string]json.RawMessage
		Applied []map[string]json.RawMessage
	}
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	var top map[string]json.RawMessage
	c.Assert(json.Unmarshal(out.Bytes(), &top), qt.IsNil)
	c.Assert(top["Current"], qt.IsNil)
	c.Assert(top["Target"], qt.IsNil)
	c.Assert(got.Pending, qt.HasLen, 1)
	c.Assert(got.Pending[0]["Version"], qt.IsNil)
	c.Assert(string(got.Pending[0]["Name"]), qt.Equals, `"10_only.sql"`)
	c.Assert(got.Applied, qt.HasLen, 1)
	c.Assert(got.Applied[0]["Version"], qt.IsNil)
	c.Assert(string(got.Applied[0]["Name"]), qt.Equals, `"10_only.sql"`)
}

func TestWriteMigrateApplyFormat_JSONAppliedFileOmitsEmptyMembers(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "applied-members.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	fsys := fstest.MapFS{
		"1_a.sql":     {Data: []byte("CREATE TABLE a (id int);\n")},
		"2_empty.sql": {Data: []byte("-- nothing here\n")},
	}
	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	var out bytes.Buffer
	err = atlasreport.WriteMigrateApplyFormat(
		&out,
		`{{ json .Applied }}`,
		atlasreport.MigrateApplyResultOptions{
			Conn:             conn,
			FS:               fsys,
			Migrations:       provider.Migrations(),
			Status:           &migrator.MigrationStatus{},
			SelectedVersions: []int64{1, 2},
			Applied:          true,
		},
	)

	// Measured on the community binary v1.3.0: a file with statements carries
	// Applied and no Skipped, Checks or Error member, and a file with none
	// carries neither Applied.
	c.Assert(err, qt.IsNil)
	var got []map[string]json.RawMessage
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(got, qt.HasLen, 2)
	c.Assert(slices.Sorted(maps.Keys(got[0])), qt.DeepEquals, []string{"Applied", "Description", "End", "Name", "Start", "Version"})
	c.Assert(string(got[0]["Applied"]), qt.Equals, `["CREATE TABLE a (id int);"]`)
	c.Assert(slices.Sorted(maps.Keys(got[1])), qt.DeepEquals, []string{"Description", "End", "Name", "Start", "Version"})
}

func TestWriteMigrateApplyFormat_JSONCurrentAndTargetPresence(t *testing.T) {
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
	c.Assert(targetOnly["Target"], qt.IsNil)

	// Nothing pending: the community binary reports where the database is
	// and no Target, as `{{ .Target }}` reads "" there.
	nothingPending := render(atlasreport.MigrateApplyResultOptions{
		CurrentKey:    "1.5",
		CurrentKeySet: true,
	})
	c.Assert(string(nothingPending["Current"]), qt.Equals, `"1.5"`)
	c.Assert(nothingPending["Target"], qt.IsNil)

	selected := render(atlasreport.MigrateApplyResultOptions{
		CurrentKey:       "1.5",
		CurrentKeySet:    true,
		SelectedVersions: []int64{20},
		SelectedKeys:     []string{"2"},
	})
	c.Assert(string(selected["Current"]), qt.Equals, `"1.5"`)
	c.Assert(string(selected["Target"]), qt.Equals, `"2"`)
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
			// No userinfo: a SQLite URL carries none, nothing here asserts it,
			// and atlasurl.Parse cannot read the shape on Windows -- its
			// fallback looks for a drive letter immediately after "://", which
			// credentials displace. The redaction under test is the query's.
			URL: "sqlite://" + dbPath + "?password=hidden&token=private",
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
	c.Assert(testutils.URLDatabasePath(got.URL.Opaque, got.URL.Path), qt.Equals, dbPath)
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
	c.Assert(got.Applied[0].Applied, qt.DeepEquals, []string{"CREATE TABLE users (id INTEGER PRIMARY KEY);"})
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
	Scheme string
	// Opaque carries a Windows drive path; see testutils.URLDatabasePath.
	Opaque   string
	Host     string
	Path     string
	RawQuery string
	Schema   string
}
