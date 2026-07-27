package datamigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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

func TestGenerate_EmptyDesiredWithLiveRowsErrors(t *testing.T) {
	c := qt.New(t)

	conn := newRegionsConn(t, [][2]string{{"US", "United States"}})
	root := t.TempDir()
	writeRegionsFixture(t, root, "[]\n")

	_, _, err := datamigrate.Generate(context.Background(), conn, datamigrate.Options{RootDir: root})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "empty desired row set")
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
	c.Assert(err.Error(), qt.Contains, "protected table(s) REGIONS")
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
