//go:build integration

package dbschema_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestSQLServerLiveDocumentAppliesEveryPropertyScope is stokaro/ptah#1999 on a
// real server, and stokaro/ptah#1031's live-coverage criterion for the four
// scopes plus idempotence.
//
// The path is the operator's: a schema FILE, loaded the way every CLI verb
// loads one -- through [schemafile.LoadSources], which merges each parsed file
// into one description. Two object families were missing from that merge, so a
// document declaring four extended properties and a synonym was compared as a
// document declaring none, and `schema apply` answered `Schema is synced, no
// changes to be made`.
//
// The library path planned all of it, which is why nothing in the suite could
// see this: every other test builds a [schemamodel.Database] in Go, or loads one
// file through [schemafile.Load], and both skip the merge.
//
// The second apply is half the test. A property added but not readable back --
// or read back under a different address -- would pass the first one and plan
// the same statement forever.
func TestSQLServerLiveDocumentAppliesEveryPropertyScope(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_doc_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(schemaName)
	_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoted+"')")
	c.Assert(err, qt.IsNil)
	databaseProperty := "ptah_db_" + schemaName
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoted+".[gauge]")
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted)
		// The database-scoped property is in no schema, so dropping the schema
		// does not take it: left behind it accumulates on a shared server, one
		// per run.
		_, _ = conn.ExecContext(ctx,
			"EXEC sp_dropextendedproperty @name = N'"+databaseProperty+"'")
	}()

	path := writeFourScopeDocument(c, schemaName)
	declared, err := schemafile.LoadSources(
		[]schemafile.Source{{URL: path}},
		schemafile.Options{Dialect: platform.SQLServer},
	)
	c.Assert(err, qt.IsNil)
	// Non-vacuity: the merge really carries them, so an empty plan below cannot
	// pass as agreement.
	c.Assert(declared.ExtendedProperties, qt.HasLen, 4)

	statements := planDocumentAgainstLive(c, conn, declared, schemaName)
	c.Assert(statements, qt.Not(qt.HasLen), 0)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// All four scopes are back, addressed the way each was written. The
	// database-scoped one is in a read narrowed to a schema on purpose: it
	// belongs to no schema, so dropping it from a narrowed description would
	// plan sp_dropextendedproperty for a property the declaration still names.
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(extendedPropertyNames(live.ExtendedProperties), qt.DeepEquals,
		[]string{"ptah_column", databaseProperty, "ptah_schema", "ptah_table"})

	// And the same document is now a no-op.
	c.Assert(planDocumentAgainstLive(c, conn, declared, schemaName), qt.HasLen, 0)
}

// extendedPropertyNames is the read's property names, sorted, so the assertion
// says which properties rather than how many.
func extendedPropertyNames(properties []catalog.ExtendedProperty) []string {
	names := make([]string, 0, len(properties))
	for _, property := range properties {
		names = append(names, property.Name)
	}
	slices.Sort(names)
	return names
}

// planDocumentAgainstLive plans the loaded document against the live database
// through the shipping reader, comparator and planner.
func planDocumentAgainstLive(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	declared *schemamodel.Database,
	schemaName string,
) []string {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)

	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, declared, live, nil)
	c.Assert(err, qt.IsNil)

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, declared, conn.Info().Dialect)
	c.Assert(err, qt.IsNil)
	return statements
}

// writeFourScopeDocument writes one table and a property at each of the four
// addresses SQL Server has: database, schema, table and column.
func writeFourScopeDocument(c *qt.C, schemaName string) string {
	c.Helper()
	document := fmt.Sprintf(`
schema %[1]q {
}

table "gauge" {
  schema = schema.%[1]s
  column "id" {
    type = INT
  }
  column "title" {
    type = NVARCHAR(50)
  }
  primary_key {
    columns = [column.id]
  }
}

extended_property "ptah_db_%[2]s" {
  value = "database scope"
}

extended_property "ptah_schema" {
  schema = schema.%[1]s
  value  = "schema scope"
}

extended_property "ptah_table" {
  schema = schema.%[1]s
  table  = "gauge"
  value  = "table scope"
}

extended_property "ptah_column" {
  schema = schema.%[1]s
  table  = "gauge"
  column = "title"
  value  = "column scope"
}
`, schemaName, schemaName)

	path := filepath.Join(c.TB.(*testing.T).TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}
