package datamigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/datamigrate"
)

// foldedRegionsConn opens an in-memory SQLite database whose table and columns
// are spelled the way an engine that folds a bare name stores them.
//
// SQLite compares table and column names without regard to ASCII case, which is
// what lets the folding defect be driven offline. Oracle is where it was
// measured: it folds the bare name the renderer wrote, so a declared `regions`
// is REGIONS in its catalog and a declared `code` is CODE
// (stokaro/ptah#3321).
func foldedRegionsConn(t *testing.T, rows [][2]string) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	_, err = conn.ExecContext(ctx, `CREATE TABLE REGIONS (CODE TEXT PRIMARY KEY, NAME TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	for _, row := range rows {
		_, err := conn.ExecContext(ctx, `INSERT INTO REGIONS (CODE, NAME) VALUES (?, ?)`, row[0], row[1])
		c.Assert(err, qt.IsNil)
	}
	return conn
}

// writeFoldedFixture declares the folded table in the lower case an author
// writes, which is the spelling the renderer would have created it with.
func writeFoldedFixture(t *testing.T, root, yamlRows string) {
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

// TestGenerate_EmptiesATableTheEngineFolded drives the reproduction
// stokaro/ptah#3321 carries: a declaration written in lower case, a table the
// engine created under a folded name, and a desired set emptied to delete every
// row.
//
// Compared exactly, the declaration names no live table, so the full delete
// cannot decide which columns its rollback would re-insert and the whole
// migration is refused with "it was not found in the live schema" for a table
// that is there.
func TestGenerate_EmptiesATableTheEngineFolded(t *testing.T) {
	c := qt.New(t)

	conn := foldedRegionsConn(t, [][2]string{{"US", "United States"}, {"CZ", "Czechia"}})
	root := t.TempDir()
	writeFoldedFixture(t, root, "[]\n")

	up, down, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:          root,
		AllowDestructive: true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, `DELETE FROM "regions" WHERE "code" = 'US'`)
	c.Assert(up, qt.Contains, `DELETE FROM "regions" WHERE "code" = 'CZ'`)
	// The rollback re-inserts whole rows: every column the catalog reports,
	// under the catalog's spelling except the key the declaration named, which
	// is the name each live row is indexed by.
	c.Assert(down, qt.Contains, `INSERT INTO "regions" ("NAME", "code") VALUES ('United States', 'US')`)
	c.Assert(down, qt.Contains, `INSERT INTO "regions" ("NAME", "code") VALUES ('Czechia', 'CZ')`)
}

// TestInspect_CountsAConvergedFoldedTableAsClean is the report-side half of the
// same recognition. The declaration and the table hold the same two rows, so
// nothing has drifted; a lookup that misses the table compares against no live
// rows at all and reports every declared row as an insert into a table that
// already holds it.
func TestInspect_CountsAConvergedFoldedTableAsClean(t *testing.T) {
	c := qt.New(t)

	conn := foldedRegionsConn(t, [][2]string{{"US", "United States"}, {"CZ", "Czechia"}})
	root := t.TempDir()
	writeFoldedFixture(t, root, "- code: US\n  name: United States\n- code: CZ\n  name: Czechia\n")

	summary, err := datamigrate.Inspect(context.Background(), conn, datamigrate.Options{
		RootDir: root,
		Live:    liveSchema(c, conn),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(summary.HasChanges(), qt.IsFalse)
	c.Assert(summary.Tables, qt.HasLen, 0)
}

// TestGenerate_RefusesAnUndeclaredColumnOnAFoldedTable is the failure path the
// lookup was hiding. A migration body cannot write a column the table does not
// have, and the refusal runs only once the table has been found: with the
// lookup missing, the read went out naming the column and the engine answered
// for it -- ORA-00904 on Oracle, and on SQLite the quoted name itself, which
// reaches the rollback as the value every row is restored to.
func TestGenerate_RefusesAnUndeclaredColumnOnAFoldedTable(t *testing.T) {
	c := qt.New(t)

	conn := foldedRegionsConn(t, [][2]string{{"US", "United States"}})
	root := t.TempDir()
	writeFoldedFixture(t, root, "- code: US\n  name: United States\n  iso3: USA\n")

	up, down, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:          root,
		AllowDestructive: true,
	})

	c.Assert(err, qt.ErrorMatches,
		`managed table "regions" does not have declared column\(s\) "iso3"; migrate the schema first or remove the column\(s\) from the row data`)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}
