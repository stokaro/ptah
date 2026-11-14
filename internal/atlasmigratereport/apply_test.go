package atlasmigratereport_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/internal/atlasmigratereport"
	"ptah.run/internal/atlasreport"
	"ptah.run/migration/migrator"
)

func TestWriteApplyFormat_CustomTemplate(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "runtime-format.db")
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(
		os.WriteFile(
			filepath.Join(migrationsDir, "1_create_users.sql"),
			[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);"),
			0o600,
		),
		qt.IsNil,
	)
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var out bytes.Buffer
	startedAt := time.Unix(200, 0).UTC()
	endedAt := time.Unix(201, 0).UTC()
	err = atlasmigratereport.WriteApplyFormat(
		&out,
		`{{ .Driver }}|{{ .Dir }}|{{ len .Pending }}|{{ len .Applied }}|{{ .Target }}|{{ printf "%.12s" (index (index .Applied 0).Applied 0) }}`,
		atlasmigratereport.ApplyFormatOptions{
			Conn: conn,
			FS:   os.DirFS(migrationsDir),
			Dir:  "file://migrations",
			URL:  "sqlite://" + dbPath,
			Result: atlasmigrate.ApplyResult{
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
				Executed:         true,
				StartedAt:        startedAt,
				EndedAt:          endedAt,
			},
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "sqlite|file://migrations|1|1|1|CREATE TABLE")
}

func TestWriteApplyFormat_FailurePathRequiresMigrationFilesystem(t *testing.T) {
	c := qt.New(t)

	err := atlasmigratereport.WriteApplyFormat(
		&bytes.Buffer{},
		"{{ .Driver }}",
		atlasmigratereport.ApplyFormatOptions{},
	)

	c.Assert(err, qt.ErrorMatches, "migrate apply format requires migration filesystem")
}

// A file whose statement the database refused lists the statements it sent,
// the refused one last and spelled as the file spells it, the way the pinned
// community binary v1.3.0 lists them for the same directory on SQLite. The run
// is a real one, because only the migrator marks a statement as sent.
func TestWriteApplyFormat_ListsTheRefusedStatementLast(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_b.sql"),
		[]byte("-- above\nCREATE TABLE b (id int);\nINSERT INTO nosuch (id)\n  -- inside\n  VALUES (1);\nCREATE TABLE b2 (id int);\n"),
		0o600,
	), qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(dir, "refused.db"))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	plan, err := atlasmigrate.PrepareApply(ctx, conn, atlasmigrate.ApplyOptions{
		Dir:       migrationsDir,
		FS:        os.DirFS(migrationsDir),
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	result, err := plan.Execute(ctx)
	c.Assert(err, qt.IsNotNil)
	var out bytes.Buffer

	err = atlasmigratereport.WriteApplyFormat(&out, `{{ json . }}`, atlasmigratereport.ApplyFormatOptions{
		Conn:   conn,
		FS:     os.DirFS(migrationsDir),
		Result: result,
	})

	c.Assert(err, qt.IsNil)
	var got struct {
		Applied []struct {
			Applied []string
			Error   struct{ Stmt string }
		}
	}
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(got.Applied, qt.HasLen, 1)
	c.Assert(got.Applied[0].Applied, qt.DeepEquals, []string{
		"CREATE TABLE b (id int);",
		"INSERT INTO nosuch (id)\n  -- inside\n  VALUES (1);",
	})
	c.Assert(got.Applied[0].Error.Stmt, qt.Equals, "INSERT INTO nosuch (id)\n  -- inside\n  VALUES (1);")
}

// SourceFiles reaches the report: a converted file is named the way the
// operator's directory names it.
func TestWriteApplyFormat_NamesConvertedFilesBySource(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+filepath.Join(dir, "source.db"))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var out bytes.Buffer

	err = atlasmigratereport.WriteApplyFormat(&out, `{{ range .Pending }}{{ .Name }}:{{ .Description }}{{ end }}`, atlasmigratereport.ApplyFormatOptions{
		Conn: conn,
		FS: fstest.MapFS{
			"4611686018427471935_b_c.sql": {Data: []byte("CREATE TABLE b (id int);\n")},
		},
		SourceFiles: map[int64]atlasreport.MigrateApplySourceFile{
			4611686018427471935: {Name: "V1.5__b c.sql", Description: "b c"},
		},
		Result: atlasmigrate.ApplyResult{
			Status:           &migrator.MigrationStatus{},
			SelectedVersions: []int64{4611686018427471935},
			SelectedKeys:     []string{"1.5"},
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "V1.5__b c.sql:b c")
}
