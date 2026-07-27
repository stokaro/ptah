package datamigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/datamigrate"
)

// newRegionsConn opens an in-memory SQLite database and creates a "regions"
// table seeded with the given code/name rows. The connection is closed via
// t.Cleanup.
func newRegionsConn(t *testing.T, rows [][2]string) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)

	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	_, err = conn.ExecContext(context.Background(),
		`CREATE TABLE regions (code TEXT PRIMARY KEY, name TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	for _, r := range rows {
		_, err := conn.ExecContext(context.Background(),
			`INSERT INTO regions (code, name) VALUES (?, ?)`, r[0], r[1])
		c.Assert(err, qt.IsNil)
	}
	return conn
}

// writeRegionsFixture writes a Go source carrying a //migrator:schema:data
// annotation for the "regions" table and the referenced YAML rows file into
// root, so goschema.ParseDir + LoadManagedRows resolve the desired rows.
func writeRegionsFixture(t *testing.T, root, yamlRows string) {
	t.Helper()
	c := qt.New(t)

	goSrc := `package fixture

//migrator:schema:data table="regions" key="code" file="regions.yaml"
type Region struct {
	//migrator:schema:field name="code" type="TEXT" primary="true"
	Code string

	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte(yamlRows), 0o600), qt.IsNil)
}

const driftDesiredRows = `
- code: US
  name: United States
- code: CZ
  name: Czechia
- code: DE
  name: Germany
`

func TestGenerate_ComputesReversibleDataMigration(t *testing.T) {
	c := qt.New(t)

	// Live state: US matches desired (unchanged), CZ has an old name (update),
	// XX is live-only (delete). Desired adds DE (insert).
	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
		{"XX", "Old Name"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, driftDesiredRows)

	up, down, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{RootDir: root})
	c.Assert(err, qt.IsNil)

	// Up reaches the desired state.
	c.Assert(up, qt.Contains, `INSERT INTO "regions" ("code", "name") VALUES ('DE', 'Germany');`)
	c.Assert(up, qt.Contains, `UPDATE "regions" SET "name" = 'Czechia' WHERE "code" = 'CZ';`)
	c.Assert(up, qt.Contains, `DELETE FROM "regions" WHERE "code" = 'XX';`)
	// The unchanged US row produces no statement in either direction.
	c.Assert(up, qt.Not(qt.Contains), "'US'")
	c.Assert(down, qt.Not(qt.Contains), "'US'")

	// Down is the exact inverse of up.
	c.Assert(down, qt.Contains, `INSERT INTO "regions" ("code", "name") VALUES ('XX', 'Old Name');`)
	c.Assert(down, qt.Contains, `UPDATE "regions" SET "name" = 'Czech Republic' WHERE "code" = 'CZ';`)
	c.Assert(down, qt.Contains, `DELETE FROM "regions" WHERE "code" = 'DE';`)
}

func TestGenerate_NoDriftYieldsEmpty(t *testing.T) {
	c := qt.New(t)

	// Live state already equals the desired state, so there is nothing to do.
	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czechia"},
		{"DE", "Germany"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, driftDesiredRows)

	up, down, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{RootDir: root})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}

func TestGenerate_NilConnection(t *testing.T) {
	c := qt.New(t)

	_, _, err := datamigrate.Generate(context.Background(), nil, datamigrate.Options{RootDir: t.TempDir()})
	c.Assert(err, qt.ErrorMatches, `datamigrate: a database connection is required`)
}
