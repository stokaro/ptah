package atlasreport_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasreport"
	"ptah.run/migration/migrator"
)

// checksTxtarMigration is one Atlas txtar migration carrying a checks.sql
// section, which is the shape the apply report has to describe.
const checksTxtarMigration = "-- atlas:txtar\n\n" +
	"-- checks.sql --\n" +
	"-- atlas:assert\n" +
	"SELECT NOT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'users');\n\n" +
	"-- migration.sql --\n" +
	"CREATE TABLE users (id INTEGER PRIMARY KEY);\n"

// applyChecksReport renders the Atlas apply report for one migration and returns
// the checks it described for it.
func applyChecksReport(c *qt.C, migrations []*migrator.Migration, fsys fs.FS, applyErr error) []reportedCheckGroup {
	c.Helper()
	dbPath := filepath.Join(c.TempDir(), "checks.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

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
				CurrentVersion:    0,
				PendingMigrations: []int64{1},
			},
			Migrations:       migrations,
			SelectedVersions: []int64{1},
			Applied:          true,
			StartedAt:        time.Unix(100, 0).UTC(),
			EndedAt:          time.Unix(101, 0).UTC(),
			ApplyError:       applyErr,
		},
	)
	c.Assert(err, qt.IsNil)

	var got struct {
		Applied []struct {
			Checks []reportedCheckGroup
		}
	}
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(got.Applied, qt.HasLen, 1)
	return got.Applied[0].Checks
}

// reportedCheckGroup mirrors the report's per-file check shape, so the tests
// read what a consumer of the JSON reads.
type reportedCheckGroup struct {
	Name  string
	Stmts []struct {
		Stmt  string
		Error *string
	}
	Error *struct {
		Stmt string
		Text string
	}
}

// txtarCheckMigrations builds the migration the way the migrator does, because
// the txtar check sections live on an unexported field a literal cannot set.
func txtarCheckMigrations(c *qt.C) ([]*migrator.Migration, fs.FS) {
	c.Helper()
	fsys := fstest.MapFS{"1_create_users.sql": {Data: []byte(checksTxtarMigration)}}
	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	return migrations, fsys
}

// TestMigrateApplyReport_ChecksListTheAssertionsAMigrationCarries is the
// reproduction from stokaro/ptah#3118.
//
// The report modelled Atlas's per-file check result and never assigned it, so a
// pipeline reading the report to learn which assertion ran got an empty list.
func TestMigrateApplyReport_ChecksListTheAssertionsAMigrationCarries(t *testing.T) {
	c := qt.New(t)
	migrations, fsys := txtarCheckMigrations(c)

	checks := applyChecksReport(c, migrations, fsys, nil)

	c.Assert(checks, qt.HasLen, 1)
	c.Assert(checks[0].Name, qt.Equals, "checks.sql")
	c.Assert(checks[0].Stmts, qt.HasLen, 1)
	c.Assert(checks[0].Stmts[0].Stmt, qt.Contains, "SELECT NOT EXISTS")
	c.Assert(checks[0].Stmts[0].Error, qt.IsNil)
	c.Assert(checks[0].Error, qt.IsNil)
}

// TestMigrateApplyReport_ChecksMarkTheAssertionThatRefused covers the run the
// issue measured: a failing checks.sql blocked the migration and the report said
// nothing about which assertion did it.
func TestMigrateApplyReport_ChecksMarkTheAssertionThatRefused(t *testing.T) {
	c := qt.New(t)
	migrations, fsys := txtarCheckMigrations(c)
	refusal := &migrator.CheckFailedError{
		Version: 1,
		Name:    "checks.sql",
		Assert:  "SELECT NOT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'users')",
	}

	checks := applyChecksReport(c, migrations, fsys, refusal)

	c.Assert(checks, qt.HasLen, 1)
	c.Assert(checks[0].Error, qt.IsNotNil)
	c.Assert(checks[0].Error.Text, qt.Contains, "was not satisfied")
	c.Assert(checks[0].Stmts, qt.HasLen, 1)
	c.Assert(checks[0].Stmts[0].Error, qt.IsNotNil)
}

// TestMigrateApplyReport_ChecksListPtahDirectives covers the other spelling: a
// check written as a `-- +ptah check` directive in the up body.
func TestMigrateApplyReport_ChecksListPtahDirectives(t *testing.T) {
	c := qt.New(t)
	upSQL := "-- +ptah check assert=\"SELECT 1\"\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n"
	fsys := fstest.MapFS{"1_create_users.sql": {Data: []byte(upSQL)}}
	migrations := []*migrator.Migration{{Version: 1, UpSQL: upSQL}}

	checks := applyChecksReport(c, migrations, fsys, nil)

	c.Assert(checks, qt.HasLen, 1)
	c.Assert(checks[0].Name, qt.Equals, "")
	c.Assert(checks[0].Stmts, qt.HasLen, 1)
	c.Assert(checks[0].Stmts[0].Stmt, qt.Contains, "SELECT 1")
}
