package atlasmigratereport_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/atlasmigratereport"
	"github.com/stokaro/ptah/migration/migrator"
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
			Conn:        conn,
			ResolvedDir: migrationsDir,
			Dir:         "file://migrations",
			URL:         "sqlite://" + dbPath,
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
				StartedAt:        startedAt,
				EndedAt:          endedAt,
			},
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "sqlite|file://migrations|1|1|1|CREATE TABLE")
}
