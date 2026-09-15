package datamigrate_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/datamigrate"
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

// TestInspect_ADeclaredColumnTheTableLacksIsComparedAsAbsent covers the state a
// release passes through: the declaration adds a column and the database has
// not gained it yet.
//
// Reading the column instead is what SQLite answers with a string literal for,
// so the projection drops it and every row reports the update it will need once
// the column lands. The structural comparison reports the column itself.
func TestInspect_ADeclaredColumnTheTableLacksIsComparedAsAbsent(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czechia"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, `
- code: US
  name: United States
  iso3: USA
- code: CZ
  name: Czechia
  iso3: CZE
`)

	summary, err := datamigrate.Inspect(context.Background(), conn, datamigrate.Options{
		RootDir: root,
		Live:    liveSchema(c, conn),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(summary.Tables, qt.DeepEquals, []datamigrate.TableDrift{
		{Table: "regions", Inserts: 0, Updates: 2, Deletes: 0},
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
