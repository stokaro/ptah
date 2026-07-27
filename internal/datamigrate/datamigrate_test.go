package datamigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/datamigrate"
	"github.com/stokaro/ptah/migration/migrator"
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

	up, down, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{RootDir: root, AllowDestructive: true})
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

// TestGenerate_RoundTripApply proves reversibility against a real database:
// applying up reaches the desired state and applying down restores the original,
// splitting each script through the same connection-dialect splitter the
// migrator uses.
func TestGenerate_RoundTripApply(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
		{"XX", "Old Name"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, driftDesiredRows)

	up, down, err := datamigrate.Generate(ctx, conn, datamigrate.Options{RootDir: root, AllowDestructive: true})
	c.Assert(err, qt.IsNil)

	apply := func(script string) {
		for _, stmt := range migrator.SplitSQLStatementsForConnection(conn, script) {
			_, execErr := conn.ExecContext(ctx, stmt)
			c.Assert(execErr, qt.IsNil, qt.Commentf("stmt: %s", stmt))
		}
	}
	readState := func() map[string]string {
		rows, queryErr := conn.QueryContext(ctx, `SELECT code, name FROM regions ORDER BY code`)
		c.Assert(queryErr, qt.IsNil)
		defer func() { _ = rows.Close() }()
		out := map[string]string{}
		for rows.Next() {
			var code, name string
			c.Assert(rows.Scan(&code, &name), qt.IsNil)
			out[code] = name
		}
		c.Assert(rows.Err(), qt.IsNil)
		return out
	}

	apply(up)
	c.Assert(readState(), qt.DeepEquals, map[string]string{
		"US": "United States", "CZ": "Czechia", "DE": "Germany",
	})

	apply(down)
	c.Assert(readState(), qt.DeepEquals, map[string]string{
		"US": "United States", "CZ": "Czech Republic", "XX": "Old Name",
	})
}

// TestGenerate_SchemaQualifiedTable proves an annotation's schema="..." flows
// through the whole pipeline: the live rows are read from the schema-qualified
// table, the generated DML targets it, and applying up then down round-trips.
func TestGenerate_SchemaQualifiedTable(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	// Attach a second in-memory database as the "reference" schema and create a
	// schema-qualified regions table in it. Ptah caps in-memory SQLite to a
	// single connection, so the ATTACH persists for every subsequent query.
	_, err = conn.ExecContext(ctx, `ATTACH DATABASE ':memory:' AS reference`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `CREATE TABLE reference.regions (code TEXT PRIMARY KEY, name TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	for _, r := range [][2]string{{"US", "United States"}, {"CZ", "Czech Republic"}, {"XX", "Old Name"}} {
		_, execErr := conn.ExecContext(ctx, `INSERT INTO reference.regions (code, name) VALUES (?, ?)`, r[0], r[1])
		c.Assert(execErr, qt.IsNil)
	}

	root := t.TempDir()
	goSrc := `package fixture

//migrator:schema:data table="regions" schema="reference" key="code" file="regions.yaml"
type Region struct {
	//migrator:schema:field name="code" type="TEXT" primary="true"
	Code string

	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte(driftDesiredRows), 0o600), qt.IsNil)

	up, down, err := datamigrate.Generate(ctx, conn, datamigrate.Options{RootDir: root, AllowDestructive: true})
	c.Assert(err, qt.IsNil)
	// The live rows were read from reference.regions (main has no regions table),
	// and the generated DML targets the schema-qualified table.
	c.Assert(up, qt.Contains, `"reference"."regions"`)
	c.Assert(down, qt.Contains, `"reference"."regions"`)

	apply := func(script string) {
		for _, stmt := range migrator.SplitSQLStatementsForConnection(conn, script) {
			_, execErr := conn.ExecContext(ctx, stmt)
			c.Assert(execErr, qt.IsNil, qt.Commentf("stmt: %s", stmt))
		}
	}
	readState := func() map[string]string {
		rows, queryErr := conn.QueryContext(ctx, `SELECT code, name FROM reference.regions ORDER BY code`)
		c.Assert(queryErr, qt.IsNil)
		defer func() { _ = rows.Close() }()
		out := map[string]string{}
		for rows.Next() {
			var code, name string
			c.Assert(rows.Scan(&code, &name), qt.IsNil)
			out[code] = name
		}
		c.Assert(rows.Err(), qt.IsNil)
		return out
	}

	apply(up)
	c.Assert(readState(), qt.DeepEquals, map[string]string{
		"US": "United States", "CZ": "Czechia", "DE": "Germany",
	})

	apply(down)
	c.Assert(readState(), qt.DeepEquals, map[string]string{
		"US": "United States", "CZ": "Czech Republic", "XX": "Old Name",
	})
}

// TestGenerate_ProtectedSchemaQualifiedTable proves the protected gate is
// schema-aware: a schema-qualified managed table is protected by both its
// qualified "schema.table" name and its bare table name, and the refusal names
// the qualified table.
func TestGenerate_ProtectedSchemaQualifiedTable(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	// setup builds a fresh connection with an insert-only drift on
	// reference.regions, so only the protected gate (not the destructive gate)
	// can refuse the run.
	setup := func() (*dbschema.DatabaseConnection, string) {
		conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
		c.Assert(err, qt.IsNil)
		t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
		_, err = conn.ExecContext(ctx, `ATTACH DATABASE ':memory:' AS reference`)
		c.Assert(err, qt.IsNil)
		_, err = conn.ExecContext(ctx, `CREATE TABLE reference.regions (code TEXT PRIMARY KEY, name TEXT NOT NULL)`)
		c.Assert(err, qt.IsNil)
		_, err = conn.ExecContext(ctx, `INSERT INTO reference.regions (code, name) VALUES ('US', 'United States')`)
		c.Assert(err, qt.IsNil)

		root := t.TempDir()
		goSrc := `package fixture

//migrator:schema:data table="regions" schema="reference" key="code" file="regions.yaml"
type Region struct {
	//migrator:schema:field name="code" type="TEXT" primary="true"
	Code string

	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`
		c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte(insertOnlyDesiredRows), 0o600), qt.IsNil)
		return conn, root
	}

	// A qualified protected entry refuses and reports the qualified name.
	conn, root := setup()
	_, _, err := datamigrate.Generate(ctx, conn, datamigrate.Options{
		RootDir:         root,
		ProtectedTables: []string{"reference.regions"},
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "protected table(s) reference.regions")

	// A bare protected entry also protects the table in its schema (safe-side),
	// preserving the pre-schema behavior.
	conn, root = setup()
	_, _, err = datamigrate.Generate(ctx, conn, datamigrate.Options{
		RootDir:         root,
		ProtectedTables: []string{"regions"},
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "protected table(s) reference.regions")

	// --allow-prod clears the gate and the insert-only migration is generated.
	conn, root = setup()
	up, _, err := datamigrate.Generate(ctx, conn, datamigrate.Options{
		RootDir:         root,
		ProtectedTables: []string{"reference.regions"},
		AllowProd:       true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, `INSERT INTO "reference"."regions"`)
}

// TestGenerate_ForeignKeyOrdering proves the migration is ordered by the
// schema's foreign-key dependency graph: INSERTs run parents-first and DELETEs
// children-first, so applying up and then down never violates a foreign key.
// The test enforces foreign keys (PRAGMA foreign_keys=ON) so a wrong order would
// actually fail rather than pass silently.
func TestGenerate_ForeignKeyOrdering(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	_, err = conn.ExecContext(ctx, `PRAGMA foreign_keys = ON`)
	c.Assert(err, qt.IsNil)
	for _, ddl := range []string{
		`CREATE TABLE countries (code TEXT PRIMARY KEY, name TEXT NOT NULL)`,
		`CREATE TABLE regions (code TEXT PRIMARY KEY, country_code TEXT NOT NULL REFERENCES countries(code), name TEXT NOT NULL)`,
		// Live: US + XX countries; CA(->US) + ZZ(->XX) regions.
		`INSERT INTO countries (code, name) VALUES ('US', 'United States'), ('XX', 'Old Country')`,
		`INSERT INTO regions (code, country_code, name) VALUES ('CA', 'US', 'California'), ('ZZ', 'XX', 'Old Region')`,
	} {
		_, execErr := conn.ExecContext(ctx, ddl)
		c.Assert(execErr, qt.IsNil)
	}

	root := t.TempDir()
	goSrc := `package fixture

//migrator:schema:table name="countries"
type Country struct {
	//migrator:schema:field name="code" type="TEXT" primary="true"
	Code string
	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string
}

//migrator:schema:table name="regions"
type Region struct {
	//migrator:schema:field name="code" type="TEXT" primary="true"
	Code string
	//migrator:schema:field name="country_code" type="TEXT" not_null="true" foreign="countries(code)"
	CountryCode string
	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string
}

//migrator:schema:data table="countries" key="code" file="countries.yaml"
type countryData struct{ _ int }

//migrator:schema:data table="regions" key="code" file="regions.yaml"
type regionData struct{ _ int }
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	// Desired: drop the XX country and ZZ region, add the DE country and the BY
	// region that references DE.
	c.Assert(os.WriteFile(filepath.Join(root, "countries.yaml"),
		[]byte("- code: US\n  name: United States\n- code: DE\n  name: Germany\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"),
		[]byte("- code: CA\n  country_code: US\n  name: California\n- code: BY\n  country_code: DE\n  name: Bavaria\n"), 0o600), qt.IsNil)

	up, down, err := datamigrate.Generate(ctx, conn, datamigrate.Options{RootDir: root, AllowDestructive: true})
	c.Assert(err, qt.IsNil)

	// Parent INSERT before child INSERT; child DELETE before parent DELETE.
	c.Assert(strings.Index(up, `INSERT INTO "countries"`) < strings.Index(up, `INSERT INTO "regions"`), qt.IsTrue,
		qt.Commentf("up:\n%s", up))
	c.Assert(strings.Index(up, `DELETE FROM "regions"`) < strings.Index(up, `DELETE FROM "countries"`), qt.IsTrue,
		qt.Commentf("up:\n%s", up))

	apply := func(script string) {
		for _, stmt := range migrator.SplitSQLStatementsForConnection(conn, script) {
			_, execErr := conn.ExecContext(ctx, stmt)
			c.Assert(execErr, qt.IsNil, qt.Commentf("stmt: %s", stmt))
		}
	}
	readState := func() (map[string]string, map[string]string) {
		countries := map[string]string{}
		rows, qErr := conn.QueryContext(ctx, `SELECT code, name FROM countries ORDER BY code`)
		c.Assert(qErr, qt.IsNil)
		for rows.Next() {
			var k, v string
			c.Assert(rows.Scan(&k, &v), qt.IsNil)
			countries[k] = v
		}
		c.Assert(rows.Err(), qt.IsNil)
		_ = rows.Close()
		regions := map[string]string{}
		rows2, qErr := conn.QueryContext(ctx, `SELECT code, country_code FROM regions ORDER BY code`)
		c.Assert(qErr, qt.IsNil)
		for rows2.Next() {
			var k, v string
			c.Assert(rows2.Scan(&k, &v), qt.IsNil)
			regions[k] = v
		}
		c.Assert(rows2.Err(), qt.IsNil)
		_ = rows2.Close()
		return countries, regions
	}

	apply(up)
	countries, regions := readState()
	c.Assert(countries, qt.DeepEquals, map[string]string{"US": "United States", "DE": "Germany"})
	c.Assert(regions, qt.DeepEquals, map[string]string{"CA": "US", "BY": "DE"})

	apply(down)
	countries, regions = readState()
	c.Assert(countries, qt.DeepEquals, map[string]string{"US": "United States", "XX": "Old Country"})
	c.Assert(regions, qt.DeepEquals, map[string]string{"CA": "US", "ZZ": "XX"})
}

// TestGenerate_EmptyDesiredWithLiveRowsGeneratesFullDelete proves that emptying a
// populated table's desired set now produces a reversible full-table delete
// rather than a refusal: up deletes every live row and down re-inserts it with
// its full non-key columns, not the keys alone. The all-delete change is
// destructive, so the destructive gate still guards it.
func TestGenerate_EmptyDesiredWithLiveRowsGeneratesFullDelete(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{{"US", "United States"}})
	root := t.TempDir()
	writeRegionsFixture(t, root, "[]\n")

	// Without the flag, the all-delete change is still refused as destructive.
	_, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{RootDir: root})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "destructive")

	// With the flag, the full-fidelity delete is generated: up deletes the row and
	// down re-inserts it with the full non-key columns (code and name), so the
	// rollback restores the whole row rather than the key alone.
	up, down, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{RootDir: root, AllowDestructive: true})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, `DELETE FROM "regions" WHERE "code" = 'US';`)
	c.Assert(down, qt.Contains, `INSERT INTO "regions" ("code", "name") VALUES ('US', 'United States');`)
}

func TestGenerate_EmptyDesiredEmptyTableIsNoOp(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, nil)
	root := t.TempDir()
	writeRegionsFixture(t, root, "[]\n")

	up, down, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{RootDir: root})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}

// TestGenerate_EmptyDesiredFullFidelityDeleteRoundTrip proves the empty-desired
// full-table delete is reversible even when the table has a column that appears
// in no desired row and a generated column. down re-inserts the full
// non-generated column set (id, label, weight) and omits the generated label_len
// — inserting an explicit value for a generated column would error. Applying up
// then down restores the original rows, with the generated column recomputed from
// the re-inserted base columns.
func TestGenerate_EmptyDesiredFullFidelityDeleteRoundTrip(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	_, err = conn.ExecContext(ctx, `CREATE TABLE widgets (
		id TEXT PRIMARY KEY,
		label TEXT NOT NULL,
		weight INTEGER NOT NULL,
		label_len INTEGER GENERATED ALWAYS AS (length(label)) STORED
	)`)
	c.Assert(err, qt.IsNil)
	seed := []struct {
		id     string
		label  string
		weight int
	}{
		{"A", "Alpha", 10},
		{"B", "Beta", 20},
	}
	for _, r := range seed {
		_, execErr := conn.ExecContext(ctx,
			`INSERT INTO widgets (id, label, weight) VALUES (?, ?, ?)`, r.id, r.label, r.weight)
		c.Assert(execErr, qt.IsNil)
	}

	root := t.TempDir()
	goSrc := `package fixture

//migrator:schema:data table="widgets" key="id" file="widgets.yaml"
type widgetData struct{ _ int }
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "widgets.yaml"), []byte("[]\n"), 0o600), qt.IsNil)

	up, down, err := datamigrate.Generate(ctx, conn, datamigrate.Options{RootDir: root, AllowDestructive: true})
	c.Assert(err, qt.IsNil)
	// up deletes every live row; down re-inserts the full non-generated columns
	// and never names the generated column.
	c.Assert(up, qt.Contains, `DELETE FROM "widgets" WHERE "id" = 'A';`)
	c.Assert(down, qt.Contains, `INSERT INTO "widgets" ("id", "label", "weight") VALUES ('A', 'Alpha', 10);`)
	c.Assert(down, qt.Not(qt.Contains), "label_len")

	apply := func(script string) {
		for _, stmt := range migrator.SplitSQLStatementsForConnection(conn, script) {
			_, execErr := conn.ExecContext(ctx, stmt)
			c.Assert(execErr, qt.IsNil, qt.Commentf("stmt: %s", stmt))
		}
	}
	readState := func() map[string][3]any {
		rows, queryErr := conn.QueryContext(ctx, `SELECT id, label, weight, label_len FROM widgets ORDER BY id`)
		c.Assert(queryErr, qt.IsNil)
		defer func() { _ = rows.Close() }()
		out := map[string][3]any{}
		for rows.Next() {
			var id, label string
			var weight, labelLen int
			c.Assert(rows.Scan(&id, &label, &weight, &labelLen), qt.IsNil)
			out[id] = [3]any{label, weight, labelLen}
		}
		c.Assert(rows.Err(), qt.IsNil)
		return out
	}

	// The generated column is populated from the seed (len("Alpha")=5, len("Beta")=4).
	original := readState()
	c.Assert(original, qt.DeepEquals, map[string][3]any{
		"A": {"Alpha", 10, 5},
		"B": {"Beta", 20, 4},
	})

	apply(up)
	c.Assert(readState(), qt.HasLen, 0)

	apply(down)
	// The rows are restored and the generated label_len recomputes from the
	// re-inserted label, so the full original state (generated column included)
	// comes back.
	c.Assert(readState(), qt.DeepEquals, original)
}

// TestGenerate_EmptyDesiredRestoresTimestampColumn proves the widened full read
// handles a timestamp column: drivers scan it as time.Time, which the renderer
// now emits as a quoted literal rather than failing with "unsupported value
// type". up deletes every row and down re-inserts the full row; applying up then
// down restores the original timestamps.
func TestGenerate_EmptyDesiredRestoresTimestampColumn(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	_, err = conn.ExecContext(ctx, `CREATE TABLE events (id TEXT PRIMARY KEY, created_at TIMESTAMP NOT NULL)`)
	c.Assert(err, qt.IsNil)
	seed := []struct {
		id string
		ts time.Time
	}{
		{"A", time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC)},
		{"B", time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)},
	}
	for _, r := range seed {
		_, execErr := conn.ExecContext(ctx, `INSERT INTO events (id, created_at) VALUES (?, ?)`, r.id, r.ts)
		c.Assert(execErr, qt.IsNil)
	}

	root := t.TempDir()
	goSrc := `package fixture

//migrator:schema:data table="events" key="id" file="events.yaml"
type eventData struct{ _ int }
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "events.yaml"), []byte("[]\n"), 0o600), qt.IsNil)

	up, down, err := datamigrate.Generate(ctx, conn, datamigrate.Options{RootDir: root, AllowDestructive: true})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, `DELETE FROM "events" WHERE "id" = 'A';`)
	c.Assert(down, qt.Contains, `INSERT INTO "events" ("created_at", "id") VALUES ('2024-03-05 06:07:08+00:00', 'A');`)

	apply := func(script string) {
		for _, stmt := range migrator.SplitSQLStatementsForConnection(conn, script) {
			_, execErr := conn.ExecContext(ctx, stmt)
			c.Assert(execErr, qt.IsNil, qt.Commentf("stmt: %s", stmt))
		}
	}
	readState := func() map[string]string {
		rows, queryErr := conn.QueryContext(ctx, `SELECT id, created_at FROM events ORDER BY id`)
		c.Assert(queryErr, qt.IsNil)
		defer func() { _ = rows.Close() }()
		out := map[string]string{}
		for rows.Next() {
			var id string
			var ts time.Time
			c.Assert(rows.Scan(&id, &ts), qt.IsNil)
			out[id] = ts.UTC().Format(time.RFC3339Nano)
		}
		c.Assert(rows.Err(), qt.IsNil)
		return out
	}

	original := readState()
	c.Assert(original, qt.HasLen, 2)

	apply(up)
	c.Assert(readState(), qt.HasLen, 0)

	apply(down)
	c.Assert(readState(), qt.DeepEquals, original)
}

// TestGenerate_EmptyDesiredPreservesAutoincrementKey proves an auto-increment key
// that accepts explicit inserts (SQLite AUTOINCREMENT) is re-inserted with its
// original value, not refused and not regenerated, so the round-trip restores the
// exact ids.
func TestGenerate_EmptyDesiredPreservesAutoincrementKey(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	_, err = conn.ExecContext(ctx, `CREATE TABLE tickets (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	for _, label := range []string{"alpha", "beta"} {
		_, execErr := conn.ExecContext(ctx, `INSERT INTO tickets (label) VALUES (?)`, label)
		c.Assert(execErr, qt.IsNil)
	}

	root := t.TempDir()
	goSrc := `package fixture

//migrator:schema:data table="tickets" key="id" file="tickets.yaml"
type ticketData struct{ _ int }
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "tickets.yaml"), []byte("[]\n"), 0o600), qt.IsNil)

	up, down, err := datamigrate.Generate(ctx, conn, datamigrate.Options{RootDir: root, AllowDestructive: true})
	c.Assert(err, qt.IsNil)
	// The key is re-inserted with its explicit original value.
	c.Assert(down, qt.Contains, `INSERT INTO "tickets" ("id", "label") VALUES (1, 'alpha');`)

	apply := func(script string) {
		for _, stmt := range migrator.SplitSQLStatementsForConnection(conn, script) {
			_, execErr := conn.ExecContext(ctx, stmt)
			c.Assert(execErr, qt.IsNil, qt.Commentf("stmt: %s", stmt))
		}
	}
	readState := func() map[int64]string {
		rows, queryErr := conn.QueryContext(ctx, `SELECT id, label FROM tickets ORDER BY id`)
		c.Assert(queryErr, qt.IsNil)
		defer func() { _ = rows.Close() }()
		out := map[int64]string{}
		for rows.Next() {
			var id int64
			var label string
			c.Assert(rows.Scan(&id, &label), qt.IsNil)
			out[id] = label
		}
		c.Assert(rows.Err(), qt.IsNil)
		return out
	}

	original := readState()
	c.Assert(original, qt.DeepEquals, map[int64]string{1: "alpha", 2: "beta"})

	apply(up)
	c.Assert(readState(), qt.HasLen, 0)

	apply(down)
	c.Assert(readState(), qt.DeepEquals, original)
}

// TestGenerate_EmptyDesiredExplicitDefaultSchema proves a table declared with the
// connection's default schema (schema="main" on SQLite) resolves during the
// empty-desired introspection, which blanks a default-schema table's reported
// schema — previously a spurious "table not found".
func TestGenerate_EmptyDesiredExplicitDefaultSchema(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	_, err = conn.ExecContext(ctx, `CREATE TABLE regions (code TEXT PRIMARY KEY, name TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `INSERT INTO regions (code, name) VALUES ('US', 'United States')`)
	c.Assert(err, qt.IsNil)

	root := t.TempDir()
	goSrc := `package fixture

//migrator:schema:data table="regions" schema="main" key="code" file="regions.yaml"
type regionData struct{ _ int }
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte("[]\n"), 0o600), qt.IsNil)

	up, down, err := datamigrate.Generate(ctx, conn, datamigrate.Options{RootDir: root, AllowDestructive: true})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, `DELETE FROM "main"."regions" WHERE "code" = 'US';`)
	c.Assert(down, qt.Contains, `INSERT INTO "main"."regions" ("code", "name") VALUES ('US', 'United States');`)
}

func TestGenerate_NilConnection(t *testing.T) {
	c := qt.New(t)

	_, _, err := datamigrate.Generate(context.Background(), nil, datamigrate.Options{RootDir: t.TempDir()})
	c.Assert(err, qt.ErrorMatches, `datamigrate: a database connection is required`)
}

// insertOnlyDesiredRows keeps the live US row unchanged and adds DE, so the
// diff is a single additive INSERT with no update or delete.
const insertOnlyDesiredRows = `
- code: US
  name: United States
- code: DE
  name: Germany
`

func TestGenerate_DestructiveRefusedByDefault(t *testing.T) {
	c := qt.New(t)

	// driftDesiredRows updates CZ and deletes XX against this live state.
	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
		{"XX", "Old Name"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, driftDesiredRows)

	_, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{RootDir: root})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "destructive")
	c.Assert(err.Error(), qt.Contains, "--allow-destructive")
	// The summary names the affected table and its update/delete volume.
	c.Assert(err.Error(), qt.Contains, `"regions" (1 update(s), 1 delete(s))`)
}

func TestGenerate_InsertOnlyAllowedWithoutFlag(t *testing.T) {
	c := qt.New(t)

	// Only US exists live; the desired set keeps US and adds DE, so the diff is
	// insert-only and the destructive gate must not fire.
	conn := newRegionsConn(t, [][2]string{{"US", "United States"}})
	root := t.TempDir()
	writeRegionsFixture(t, root, insertOnlyDesiredRows)

	up, down, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{RootDir: root})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, `INSERT INTO "regions" ("code", "name") VALUES ('DE', 'Germany');`)
	c.Assert(down, qt.Contains, `DELETE FROM "regions" WHERE "code" = 'DE';`)
	// An insert-only up contains no destructive statement.
	c.Assert(up, qt.Not(qt.Contains), "DELETE")
	c.Assert(up, qt.Not(qt.Contains), "UPDATE")
}

func TestGenerate_ProtectedTableRefused(t *testing.T) {
	c := qt.New(t)

	// Insert-only drift, so it is the protected-table gate — not the destructive
	// gate — that refuses the run.
	conn := newRegionsConn(t, [][2]string{{"US", "United States"}})
	root := t.TempDir()
	writeRegionsFixture(t, root, insertOnlyDesiredRows)

	_, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:         root,
		ProtectedTables: []string{"regions"},
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "protected table(s) regions")
	c.Assert(err.Error(), qt.Contains, "--allow-prod")
}

func TestGenerate_ProtectedTableMatchIsCaseInsensitive(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{{"US", "United States"}})
	root := t.TempDir()
	writeRegionsFixture(t, root, insertOnlyDesiredRows)

	_, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:         root,
		ProtectedTables: []string{"REGIONS"},
	})
	c.Assert(err, qt.IsNotNil)
	// The match is case-insensitive, and the message names the actual managed
	// table (as declared), not the operator's protected-entry casing.
	c.Assert(err.Error(), qt.Contains, "protected table(s) regions")
}

func TestGenerate_ProtectedTablePrecedesDestructiveGate(t *testing.T) {
	c := qt.New(t)

	// The drift is destructive AND touches a protected table; the protected-table
	// refusal must win so the operator sees the protection first.
	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
		{"XX", "Old Name"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, driftDesiredRows)

	_, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:         root,
		ProtectedTables: []string{"regions"},
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "protected")
	c.Assert(err.Error(), qt.Not(qt.Contains), "destructive")
}

func TestGenerate_ProtectedAndDestructiveAllowed(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"CZ", "Czech Republic"},
		{"XX", "Old Name"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, driftDesiredRows)

	up, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:          root,
		ProtectedTables:  []string{"regions"},
		AllowProd:        true,
		AllowDestructive: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, `DELETE FROM "regions" WHERE "code" = 'XX';`)
}

func TestGenerate_ProtectedTableWithNoDriftIsNoOp(t *testing.T) {
	c := qt.New(t)

	// The protected table has no drift, so there is no change to refuse and the
	// run is a clean no-op even without --allow-prod.
	conn := newRegionsConn(t, [][2]string{
		{"US", "United States"},
		{"DE", "Germany"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, insertOnlyDesiredRows)

	up, down, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:         root,
		ProtectedTables: []string{"regions"},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}

func TestGenerate_AllowProdAloneStillRefusesDestructive(t *testing.T) {
	c := qt.New(t)

	// The drift is destructive and touches the protected table. --allow-prod
	// clears the protected gate, but the destructive gate must still refuse,
	// proving --allow-prod cannot mask a destructive change.
	conn := newRegionsConn(t, [][2]string{
		{"CZ", "Czech Republic"},
		{"XX", "Old Name"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, driftDesiredRows)

	_, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:         root,
		ProtectedTables: []string{"regions"},
		AllowProd:       true,
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "destructive")
	c.Assert(err.Error(), qt.Not(qt.Contains), "protected")
}

func TestGenerate_AllowDestructiveAloneStillRefusesProtected(t *testing.T) {
	c := qt.New(t)

	// Symmetric to the above: --allow-destructive clears the destructive gate,
	// but the protected gate must still refuse (and take precedence).
	conn := newRegionsConn(t, [][2]string{
		{"CZ", "Czech Republic"},
		{"XX", "Old Name"},
	})
	root := t.TempDir()
	writeRegionsFixture(t, root, driftDesiredRows)

	_, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:          root,
		ProtectedTables:  []string{"regions"},
		AllowDestructive: true,
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "protected")
	c.Assert(err.Error(), qt.Not(qt.Contains), "destructive")
}

// newMultiTableConn opens an in-memory SQLite database with "regions" and
// "countries" tables seeded with the given code/name rows.
func newMultiTableConn(t *testing.T, regions, countries [][2]string) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)

	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	for _, ddl := range []string{
		`CREATE TABLE regions (code TEXT PRIMARY KEY, name TEXT NOT NULL)`,
		`CREATE TABLE countries (code TEXT PRIMARY KEY, name TEXT NOT NULL)`,
	} {
		_, err := conn.ExecContext(context.Background(), ddl)
		c.Assert(err, qt.IsNil)
	}
	seed := func(table string, rows [][2]string) {
		for _, r := range rows {
			_, err := conn.ExecContext(context.Background(),
				`INSERT INTO `+table+` (code, name) VALUES (?, ?)`, r[0], r[1])
			c.Assert(err, qt.IsNil)
		}
	}
	seed("regions", regions)
	seed("countries", countries)
	return conn
}

// writeMultiTableFixture writes //migrator:schema:data annotations for both
// "regions" and "countries" and their YAML rows files into root.
func writeMultiTableFixture(t *testing.T, root, regionsYAML, countriesYAML string) {
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

//migrator:schema:data table="countries" key="code" file="countries.yaml"
type Country struct {
	//migrator:schema:field name="code" type="TEXT" primary="true"
	Code string

	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte(regionsYAML), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "countries.yaml"), []byte(countriesYAML), 0o600), qt.IsNil)
}

const countriesUpdateRows = `
- code: CZ
  name: Czechia
`

func TestGenerate_MultiTableDestructiveSummaryNamesOnlyChangedTable(t *testing.T) {
	c := qt.New(t)

	// regions: keeps US and adds DE — insert-only. countries: CZ live name
	// differs from desired — a destructive UPDATE. Only countries must appear in
	// the destructive summary.
	conn := newMultiTableConn(t,
		[][2]string{{"US", "United States"}},
		[][2]string{{"CZ", "Old Name"}},
	)
	root := t.TempDir()
	writeMultiTableFixture(t, root, insertOnlyDesiredRows, countriesUpdateRows)

	_, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{RootDir: root})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"countries" (1 update(s), 0 delete(s))`)
	// regions is insert-only, so it must not be named as destructive.
	c.Assert(err.Error(), qt.Not(qt.Contains), "regions")

	// With the flag, both tables' changes are generated.
	up, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{RootDir: root, AllowDestructive: true})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, `INSERT INTO "regions" ("code", "name") VALUES ('DE', 'Germany');`)
	c.Assert(up, qt.Contains, `UPDATE "countries" SET "name" = 'Czechia' WHERE "code" = 'CZ';`)
}

func TestGenerate_MultiTableProtectedNamesOnlyProtectedTable(t *testing.T) {
	c := qt.New(t)

	// Both tables have only insert drift; protecting countries must refuse and
	// name countries alone, leaving the insert-only regions change unmentioned.
	conn := newMultiTableConn(t,
		[][2]string{{"US", "United States"}},
		nil,
	)
	root := t.TempDir()
	writeMultiTableFixture(t, root, insertOnlyDesiredRows, countriesUpdateRows)

	_, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{
		RootDir:         root,
		ProtectedTables: []string{"countries"},
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "protected table(s) countries")
	c.Assert(err.Error(), qt.Not(qt.Contains), "regions")
}
