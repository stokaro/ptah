package migratedata_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migratedata"
	"github.com/stokaro/ptah/dbschema"
)

// seedLiveDB creates a file-backed SQLite database with a "regions" table
// seeded with the given rows and returns its connection URL. A file (not
// :memory:) database is used so the command, which opens its own connection,
// sees the same data.
func seedLiveDB(t *testing.T, rows [][2]string) string {
	t.Helper()
	c := qt.New(t)

	url := "sqlite://" + filepath.Join(t.TempDir(), "live.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), url)
	c.Assert(err, qt.IsNil)

	_, err = conn.ExecContext(context.Background(),
		`CREATE TABLE regions (code TEXT PRIMARY KEY, name TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	for _, r := range rows {
		_, err := conn.ExecContext(context.Background(),
			`INSERT INTO regions (code, name) VALUES (?, ?)`, r[0], r[1])
		c.Assert(err, qt.IsNil)
	}
	dbschema.CloseAndWarn(conn)
	return url
}

// writeRegionsFixture writes the //ptah:schema:data annotation source and
// its YAML rows file into root.
func writeRegionsFixture(t *testing.T, root, yamlRows string) {
	t.Helper()
	c := qt.New(t)

	goSrc := `package fixture

//ptah:schema:data table="regions" key="code" file="regions.yaml"
type Region struct {
	//ptah:schema:field name="code" type="TEXT" primary="true"
	Code string

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte(yamlRows), 0o600), qt.IsNil)
}

const desiredRows = `
- code: US
  name: United States
- code: CZ
  name: Czechia
`

func runData(args ...string) (string, error) {
	cmd := migratedata.NewMigrateDataCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), err
}

func TestMigrateDataCommand_WritesPair(t *testing.T) {
	c := qt.New(t)

	// Live CZ has an old name (update); the desired CZ name and a new US row
	// drive the migration.
	dbURL := seedLiveDB(t, [][2]string{{"CZ", "Czech Republic"}})
	root := t.TempDir()
	writeRegionsFixture(t, root, desiredRows)
	migrationsDir := t.TempDir()

	out, err := runData("--root-dir", root, "--db-url", dbURL, "--migrations-dir", migrationsDir, "--allow-destructive")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	// The migrations directory was empty, so the version is 1.
	up, err := os.ReadFile(filepath.Join(migrationsDir, "0000000001_data.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Contains, `INSERT INTO "regions" ("code", "name") VALUES ('US', 'United States');`)
	c.Assert(string(up), qt.Contains, `UPDATE "regions" SET "name" = 'Czechia' WHERE "code" = 'CZ';`)

	down, err := os.ReadFile(filepath.Join(migrationsDir, "0000000001_data.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Contains, `UPDATE "regions" SET "name" = 'Czech Republic' WHERE "code" = 'CZ';`)
	c.Assert(string(down), qt.Contains, `DELETE FROM "regions" WHERE "code" = 'US';`)

	sum, err := os.ReadFile(filepath.Join(migrationsDir, "ptah.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, "0000000001_data.up.sql")

	c.Assert(out, qt.Contains, "Wrote data migration version 1")
}

func TestMigrateDataCommand_DryRunWritesNothing(t *testing.T) {
	c := qt.New(t)

	dbURL := seedLiveDB(t, [][2]string{{"CZ", "Czech Republic"}})
	root := t.TempDir()
	writeRegionsFixture(t, root, desiredRows)
	migrationsDir := t.TempDir()

	out, err := runData("--root-dir", root, "--db-url", dbURL, "--migrations-dir", migrationsDir, "--dry-run", "--allow-destructive")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "dry run")
	c.Assert(out, qt.Contains, `INSERT INTO "regions"`)
	c.Assert(out, qt.Contains, `UPDATE "regions"`)

	written, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

func TestMigrateDataCommand_NoChanges(t *testing.T) {
	c := qt.New(t)

	// Live already equals desired, so nothing is generated.
	dbURL := seedLiveDB(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czechia"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, desiredRows)
	migrationsDir := t.TempDir()

	out, err := runData("--root-dir", root, "--db-url", dbURL, "--migrations-dir", migrationsDir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "no data changes")

	written, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

func TestMigrateDataCommand_DestructiveRefusedByDefault(t *testing.T) {
	c := qt.New(t)

	// The CZ name change is an UPDATE, so without --allow-destructive the command
	// refuses and writes nothing.
	dbURL := seedLiveDB(t, [][2]string{{"CZ", "Czech Republic"}})
	root := t.TempDir()
	writeRegionsFixture(t, root, desiredRows)
	migrationsDir := t.TempDir()

	out, err := runData("--root-dir", root, "--db-url", dbURL, "--migrations-dir", migrationsDir)
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "destructive")
	c.Assert(out, qt.Contains, "--allow-destructive")

	written, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

func TestMigrateDataCommand_DryRunAlsoGatesDestructive(t *testing.T) {
	c := qt.New(t)

	// The gates run before any SQL is emitted, so --dry-run is refused too and
	// prints no SQL — the documented "combine --allow-destructive --dry-run to
	// preview" behavior depends on this.
	dbURL := seedLiveDB(t, [][2]string{{"CZ", "Czech Republic"}})
	root := t.TempDir()
	writeRegionsFixture(t, root, desiredRows)
	migrationsDir := t.TempDir()

	out, err := runData("--root-dir", root, "--db-url", dbURL, "--migrations-dir", migrationsDir, "--dry-run")
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "destructive")
	c.Assert(out, qt.Not(qt.Contains), "INSERT INTO")

	written, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

func TestMigrateDataCommand_ProtectedTableRefused(t *testing.T) {
	c := qt.New(t)

	// The US insert is additive (not destructive), so --protected-table is the
	// only gate that can refuse this run.
	dbURL := seedLiveDB(t, [][2]string{{"CZ", "Czechia"}})
	root := t.TempDir()
	writeRegionsFixture(t, root, desiredRows)
	migrationsDir := t.TempDir()

	out, err := runData("--root-dir", root, "--db-url", dbURL, "--migrations-dir", migrationsDir, "--protected-table", "regions")
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "protected table(s) regions")
	c.Assert(out, qt.Contains, "--allow-prod")

	written, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

func TestMigrateDataCommand_RequiresDBURL(t *testing.T) {
	c := qt.New(t)

	out, err := runData("--root-dir", t.TempDir())
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "a database URL is required")
}
