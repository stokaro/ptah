package datamigrate_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"oras.land/oras-go/v2/content/memory"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/datamigrate"
	"ptah.run/internal/schemaartifact"
	"ptah.run/internal/schemaload"
	"ptah.run/migration/safety"
)

// liveSchema introspects the connection the way a command that already read the
// structure does, so the row comparison is handed the same catalog the
// structural comparison worked from.
func liveSchema(c *qt.C, conn *dbschema.DatabaseConnection) *catalog.Database {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(context.Background(), conn, nil)
	c.Assert(err, qt.IsNil)
	return live
}

// parseFixture resolves a fixture root the way a command resolves its desired
// schema, so a test can hand Inspect a model instead of a directory.
func parseFixture(c *qt.C, root string) *schemamodel.Database {
	c.Helper()
	desired, err := goschema.ParseDir(root)
	c.Assert(err, qt.IsNil)
	return desired
}

// TestInspect_CountsAHandEditedRow drives the reproduction the drift report is
// for: the structure matches and one live row carries a value nobody declared.
func TestInspect_CountsAHandEditedRow(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, `
- code: US
  name: United States
- code: CZ
  name: Czechia
`)

	summary, err := datamigrate.Inspect(context.Background(), conn, datamigrate.Options{
		RootDir: root,
		Live:    liveSchema(c, conn),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(summary.HasChanges(), qt.IsTrue)
	c.Assert(summary.Tables, qt.DeepEquals, []datamigrate.TableDrift{
		{Table: "regions", Inserts: 0, Updates: 1, Deletes: 0},
	})
	c.Assert(summary.Findings, qt.DeepEquals, []safety.Finding{
		{Category: "data_rows_updated", Count: 1, Severity: safety.Destructive},
	})
}

// TestInspect_TheSummaryCarriesNoRowValue is the guarantee the operator asked
// for: a caller that must not read row data can report this drift.
//
// The assertion is on the marshaled document rather than on the fields, because
// a field added later is what would leak a value, and only the document sees
// one arrive.
func TestInspect_TheSummaryCarriesNoRowValue(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
		{"XX", "Old Name"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, driftDesiredRows)

	summary, err := datamigrate.Inspect(context.Background(), conn, datamigrate.Options{
		RootDir: root,
		Live:    liveSchema(c, conn),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(summary.Tables, qt.DeepEquals, []datamigrate.TableDrift{
		{Table: "regions", Inserts: 1, Updates: 1, Deletes: 1},
	})

	document, err := json.Marshal(summary)
	c.Assert(err, qt.IsNil)
	// Every value on either side of the comparison: the declared ones, the live
	// ones, and the key that identifies the row that moved.
	for _, value := range []string{
		"Czechia", "Czech Republic", "United States", "Germany", "Old Name",
		"US", "CZ", "DE", "XX",
	} {
		c.Assert(string(document), qt.Not(qt.Contains), value)
	}
}

// TestInspect_CleanDatabaseReportsNoTable is the control. Without it the two
// tests above pass against an implementation that reports every managed table
// on every run, which would make the drift check fail forever.
func TestInspect_CleanDatabaseReportsNoTable(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czechia"},
		{"DE", "Germany"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, driftDesiredRows)

	summary, err := datamigrate.Inspect(context.Background(), conn, datamigrate.Options{
		RootDir: root,
		Live:    liveSchema(c, conn),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(summary, qt.IsNotNil)
	c.Assert(summary.HasChanges(), qt.IsFalse)
	c.Assert(summary.Tables, qt.HasLen, 0)
	c.Assert(summary.Findings, qt.HasLen, 0)
}

// TestInspect_AnUncreatedTableCountsEveryDeclaredRowAsAnInsert pins what the
// live schema is for. A table the database does not carry answers no SELECT, so
// reading it would turn a drift check into an error about the table the same
// check is reporting as missing.
func TestInspect_AnUncreatedTableCountsEveryDeclaredRowAsAnInsert(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	root := t.TempDir()
	writeRegionsFixture(t, root, driftDesiredRows)

	summary, err := datamigrate.Inspect(ctx, conn, datamigrate.Options{
		RootDir: root,
		Live:    liveSchema(c, conn),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(summary.Tables, qt.DeepEquals, []datamigrate.TableDrift{
		{Table: "regions", Inserts: 3, Updates: 0, Deletes: 0},
	})
	// An insert writes a row the database does not hold and takes nothing away,
	// so a threshold set to destructive passes a database that is only missing
	// reference rows.
	c.Assert(summary.Findings, qt.DeepEquals, []safety.Finding{
		{Category: "data_rows_inserted", Count: 3, Severity: safety.Safe},
	})
}

// declaredWithISO3 adds a column the fixture table does not carry, with a value
// per row that no live row could hold. A read that asks the database for the
// column anyway is what that discriminates against: SQLite answers a quoted
// name it cannot resolve with the name itself, so every row would differ from
// "USA" and "CZE", and PostgreSQL answers 42703 and takes the check down.
const declaredWithISO3 = `
- code: US
  name: United States
  iso3: USA
- code: CZ
  name: Czechia
  iso3: CZE
`

// writeISO3Fixture writes the declaration of a release that adds a column: the
// struct carries the field and the row file carries its value, and the database
// has neither yet. The field is declared as well as the row value because that
// is what makes the missing column structural drift the schema comparison
// reports, rather than a row file naming a column nothing will ever create.
func writeISO3Fixture(t *testing.T, root, yamlRows string) {
	t.Helper()
	c := qt.New(t)

	goSrc := `package fixture

//ptah:schema:data table="regions" key="code" file="regions.yaml"
type Region struct {
	//ptah:schema:field name="code" type="TEXT" primary="true"
	Code string

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string

	//ptah:schema:field name="iso3" type="TEXT"
	ISO3 string
}
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte(yamlRows), 0o600), qt.IsNil)
}

// TestInspect_AColumnTheTableLacksIsNotRowDrift covers the state a release
// passes through: the declaration adds a column and the database has not gained
// it yet.
//
// The missing column is structural drift, and the structural comparison is what
// reports it. The row comparison covers the columns both sides carry, so it
// reports nothing here: no live row holds a value for the column, so applying
// the declaration takes nothing away, and counting every row as an overwrite
// would fail a --severity destructive gate on a database that is only behind.
func TestInspect_AColumnTheTableLacksIsNotRowDrift(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czechia"},
	})
	root := t.TempDir()
	writeISO3Fixture(t, root, declaredWithISO3)

	summary, err := datamigrate.Inspect(context.Background(), conn, datamigrate.Options{
		RootDir: root,
		Live:    liveSchema(c, conn),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(summary.HasChanges(), qt.IsFalse)
	c.Assert(summary.Tables, qt.HasLen, 0)
	c.Assert(summary.Findings, qt.HasLen, 0)
}

// TestInspect_AnEditedRowDriftsBesideAColumnTheTableLacks is the control for
// the test above. Narrowing the comparison to the columns the table carries
// must not narrow away the drift the check exists to find: one live row holds a
// name nobody declared, in a column the table does have.
func TestInspect_AnEditedRowDriftsBesideAColumnTheTableLacks(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
	})
	root := t.TempDir()
	writeISO3Fixture(t, root, declaredWithISO3)

	summary, err := datamigrate.Inspect(context.Background(), conn, datamigrate.Options{
		RootDir: root,
		Live:    liveSchema(c, conn),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(summary.Tables, qt.DeepEquals, []datamigrate.TableDrift{
		{Table: "regions", Inserts: 0, Updates: 1, Deletes: 0},
	})
	c.Assert(summary.Findings, qt.DeepEquals, []safety.Finding{
		{Category: "data_rows_updated", Count: 1, Severity: safety.Destructive},
	})
}

// TestInspect_DesiredReplacesTheRootParse proves a caller that resolved the
// desired schema itself is not made to resolve it again. A drift run merges
// several --root-dir roots with --schema-file sources, and re-parsing one root
// would answer for one source and present it as the whole declaration.
func TestInspect_DesiredReplacesTheRootParse(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, `
- code: US
  name: United States
- code: CZ
  name: Czechia
`)
	desired := parseFixture(c, root)

	// RootDir is left empty on purpose: the declaration records the directory it
	// was authored in, so the row file resolves without it.
	summary, err := datamigrate.Inspect(context.Background(), conn, datamigrate.Options{
		Desired: desired,
		Live:    liveSchema(c, conn),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(summary.Tables, qt.DeepEquals, []datamigrate.TableDrift{
		{Table: "regions", Inserts: 0, Updates: 1, Deletes: 0},
	})
}

// TestInspect_ReadsTheRowsTheDeclarationCarries drives the published-artifact
// path: `ptah schema drift --schema-file oci://...` resolves a declaration that
// carries its rows and no path at all, because an artifact travels without the
// working copy that published it.
//
// The declaration comes out of a real push and pull rather than being written
// by hand, so the test compares against what the artifact decoder produces
// instead of against one reader's idea of it.
func TestInspect_ReadsTheRowsTheDeclarationCarries(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, `
- code: US
  name: United States
- code: CZ
  name: Czechia
`)
	published := parseFixture(c, root)
	c.Assert(schemaload.ReadManagedRows(published), qt.IsNil)

	store := memory.New()
	_, err := schemaartifact.PushTo(ctx, store, published, schemaartifact.PushOptions{Tags: []string{"stable"}})
	c.Assert(err, qt.IsNil)
	pulled, err := schemaartifact.PullFrom(ctx, store, "stable")
	c.Assert(err, qt.IsNil)
	// What the artifact carries and what it does not: the declared rows, and no
	// path into the working copy that published them. What the declaration
	// records resolves against whatever directory the process happens to run
	// in, so a reader that resolves it answers for a file nobody published.
	c.Assert(pulled.Database.ManagedData, qt.HasLen, 1)
	declaration := pulled.Database.ManagedData[0]
	c.Assert(declaration.Rows, qt.HasLen, 2)
	c.Assert(filepath.Join(declaration.SourceDir, declaration.File), qt.Not(qt.Equals), filepath.Join(root, "regions.yaml"))

	// RootDir is empty because the run that reads an artifact has no root, which
	// is what makes reading a file the wrong answer here.
	summary, err := datamigrate.Inspect(ctx, conn, datamigrate.Options{
		Desired: pulled.Database,
		Live:    liveSchema(c, conn),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(summary.Tables, qt.DeepEquals, []datamigrate.TableDrift{
		{Table: "regions", Inserts: 0, Updates: 1, Deletes: 0},
	})
}

// generatedKeyConn opens an in-memory SQLite database whose "regions" table
// carries a generated column, and seeds one row. The generated column is the
// managed key, which is the shape a reversible full delete cannot be written
// for: the database computes the value and refuses an explicit one.
func generatedKeyConn(t *testing.T) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	_, err = conn.ExecContext(ctx,
		`CREATE TABLE regions (code TEXT PRIMARY KEY, name TEXT NOT NULL, upper_code TEXT GENERATED ALWAYS AS (upper(code)) VIRTUAL)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `INSERT INTO regions (code, name) VALUES ('US', 'United States')`)
	c.Assert(err, qt.IsNil)
	return conn
}

// writeGeneratedKeyFixture writes a Go source whose //ptah:schema:data
// annotation keys the table on its generated column, plus the row file.
func writeGeneratedKeyFixture(t *testing.T, root, yamlRows string) {
	t.Helper()
	c := qt.New(t)

	goSrc := `package fixture

//ptah:schema:data table="regions" key="upper_code" file="regions.yaml"
type Region struct {
	//ptah:schema:field name="code" type="TEXT" primary="true"
	Code string
}
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte(yamlRows), 0o600), qt.IsNil)
}

// TestInspect_AnEmptyDeclarationCountsTheRowsItWouldDelete holds Inspect to its
// own contract: it never applies the gates that refuse to write a migration.
//
// A declaration whose row file is empty asks for every live row to go. Writing
// that as a reversible migration needs a column set the database will take an
// explicit value for, and a generated key column means there is none — a
// refusal that belongs to the verb that writes. Counting the rows needs the
// keys, which the table has, so the report answers instead of refusing.
func TestInspect_AnEmptyDeclarationCountsTheRowsItWouldDelete(t *testing.T) {
	c := qt.New(t)

	conn := generatedKeyConn(t)
	root := t.TempDir()
	writeGeneratedKeyFixture(t, root, "")

	summary, err := datamigrate.Inspect(context.Background(), conn, datamigrate.Options{
		RootDir: root,
		Live:    liveSchema(c, conn),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(summary.Tables, qt.DeepEquals, []datamigrate.TableDrift{
		{Table: "regions", Inserts: 0, Updates: 0, Deletes: 1},
	})
	c.Assert(summary.Findings, qt.DeepEquals, []safety.Finding{
		{Category: "data_rows_deleted", Count: 1, Severity: safety.Destructive},
	})
}

// TestGenerate_AnEmptyDeclarationStillRefusesAGeneratedKey is the control for
// the test above. The refusal has to stay where it belongs: a migration body
// whose rollback cannot restore the rows it deleted is the outcome it exists to
// prevent, and moving the read-only report out from under it must not move it.
func TestGenerate_AnEmptyDeclarationStillRefusesAGeneratedKey(t *testing.T) {
	c := qt.New(t)

	conn := generatedKeyConn(t)
	root := t.TempDir()
	writeGeneratedKeyFixture(t, root, "")

	up, down, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:          root,
		AllowDestructive: true,
	})

	c.Assert(err, qt.ErrorMatches,
		`key column "upper_code" of managed table "regions" is not a writable, non-generated column; a reversible full delete needs every key column`)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}

// TestGenerate_RefusesAColumnTheTableDoesNotHave pins what the write path does
// with the state the report tolerates.
//
// The reader quotes the column names it is given, and SQLite answers a quoted
// name it cannot resolve with the name itself. Rendering from that read writes
// the literal "iso3" into the rollback as the value each row is restored to, so
// the migration is refused before any SQL exists.
func TestGenerate_RefusesAColumnTheTableDoesNotHave(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{{"US", "United States"}})
	root := t.TempDir()
	writeISO3Fixture(t, root, declaredWithISO3)

	up, down, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:          root,
		AllowDestructive: true,
	})

	c.Assert(err, qt.ErrorMatches,
		`managed table "regions" does not have declared column\(s\) "iso3"; migrate the schema first or remove the column\(s\) from the row data`)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}

func TestInspect_FailurePath(t *testing.T) {
	t.Run("no connection", func(t *testing.T) {
		c := qt.New(t)

		summary, err := datamigrate.Inspect(context.Background(), nil, datamigrate.Options{RootDir: t.TempDir()})

		c.Assert(err, qt.ErrorMatches, `datamigrate: a database connection is required`)
		c.Assert(summary, qt.IsNil)
	})

	t.Run("missing row file", func(t *testing.T) {
		c := qt.New(t)
		conn := newRegionsConn(t, nil)
		root := t.TempDir()
		writeRegionsFixture(t, root, driftDesiredRows)
		c.Assert(os.Remove(filepath.Join(root, "regions.yaml")), qt.IsNil)

		summary, err := datamigrate.Inspect(context.Background(), conn, datamigrate.Options{
			RootDir: root,
			Live:    liveSchema(c, conn),
		})

		c.Assert(err, qt.ErrorMatches, `read managed data file .*regions.yaml" for table "regions": .*`)
		c.Assert(summary, qt.IsNil)
	})
}
