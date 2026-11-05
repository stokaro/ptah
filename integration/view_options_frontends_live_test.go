//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/goschema"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// viewOptionsFrontendCases declare the table, view and materialized view of
// [optionsSchema] with the same options through each frontend that is not
// SQL: a view with security_invoker and security_barrier, and a materialized
// view with fillfactor.
var viewOptionsFrontendCases = []struct {
	name, file, document string
}{
	{
		name: "Go annotations",
		file: "schema.go",
		document: `package schema

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" not_null
	ID int
	//ptah:schema:field name="amount" type="NUMERIC" not_null
	Amount float64
}

//ptah:schema:view name="report" body="SELECT id, amount FROM orders" options="security_invoker=true,security_barrier"
type Report struct{}

//ptah:schema:matview name="totals" body="SELECT sum(amount) AS total FROM orders" options="fillfactor=70"
type Totals struct{}
`,
	},
	{
		name: "YAML",
		file: "schema.yaml",
		document: `tables:
  orders:
    columns:
      id:
        type: INTEGER
        primary: true
        not_null: true
      amount:
        type: NUMERIC
        not_null: true
views:
  report:
    body: SELECT id, amount FROM orders
    options: [security_invoker=true, security_barrier]
matviews:
  totals:
    body: SELECT sum(amount) AS total FROM orders
    options: [fillfactor=70]
`,
	},
}

// loadFrontendSchema reads document, written to a file named file, through the
// frontend its extension selects.
func loadFrontendSchema(c *qt.C, file, document string) *schemamodel.Database {
	c.Helper()
	if strings.HasSuffix(file, ".go") {
		desired, err := goschema.ParseSource(builtintest.Annotations(), file, document)
		c.Assert(err, qt.IsNil)
		return &desired
	}
	path := filepath.Join(c.TempDir(), file)
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	runtime := builtintest.Runtime()
	desired, err := schemafile.LoadPath(c.Context(), path, schemafile.Options{
		YAML: runtime.YAML(), SQL: runtime.SQL(), HCL: runtime.HCL(), Dialect: platform.Postgres,
	})
	c.Assert(err, qt.IsNil)
	return desired
}

// compareDeclaredLive compares desired against the database at url, as
// [diffViewRebuild] does for a SQL schema.
func compareDeclaredLive(c *qt.C, url string, desired *schemamodel.Database) *difftypes.SchemaDiff {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, desired, live, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	return diff
}

// TestViewOptionsFromEveryFrontendLive applies each of
// [viewOptionsFrontendCases] to a PostgreSQL database that holds the table
// alone, and reads back the options the view and the materialized view were
// created with. A comparison afterwards finds nothing to do, and so does one
// against a database that created them from the same options in SQL.
func TestViewOptionsFromEveryFrontendLive(t *testing.T) {
	want := []string{"report security_invoker=true,security_barrier=true", "totals fillfactor=70"}
	for _, test := range viewOptionsFrontendCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url, db, _ := viewRebuildTarget(c, optionsTable)
			desired := loadFrontendSchema(c, test.file, test.document)

			up, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()),
				compareDeclaredLive(c, url, desired), platform.Postgres,
				planext.Options{Capabilities: capability.ForDialect(platform.Postgres)})
			c.Assert(err, qt.IsNil)
			runRebuildStatements(c, db, up)

			c.Assert(heldViewOptions(c, db), qt.DeepEquals, want)
			c.Assert(compareDeclaredLive(c, url, desired).HasChanges(), qt.IsFalse)
			sqlURL, _, _ := viewRebuildTarget(c, optionsSchema(optionsView, "security_invoker = 'true', security_barrier", "fillfactor = 70"))
			c.Assert(compareDeclaredLive(c, sqlURL, desired).HasChanges(), qt.IsFalse)
		})
	}
}

// TestViewOptionsFromHCLLive applies an HCL schema to a PostgreSQL database
// whose view holds security_invoker and whose materialized view holds
// fillfactor. The view's security block sets security_barrier and turns
// security_invoker off, which is how a document removes it. The materialized view's body
// changes, which drops it and creates it again, and the HCL block has no
// attribute for storage parameters, so it is created again with the one the
// database held. A second comparison finds nothing to do.
func TestViewOptionsFromHCLLive(t *testing.T) {
	c := qt.New(t)
	url, db, _ := viewRebuildTarget(c, optionsSchema(optionsView, "security_invoker = on", "fillfactor = 70"))
	desired := loadFrontendSchema(c, "schema.hcl", `schema "public" {}

table "orders" {
  schema = schema.public
  column "id" {
    type = integer
    null = false
  }
  column "amount" {
    type = numeric
    null = false
  }
  primary_key {
    columns = [column.id]
  }
}

view "report" {
  schema = schema.public
  as     = "SELECT id, amount FROM orders"
  security {
    invoker = false
    barrier = true
  }
}

materialized "totals" {
  schema = schema.public
  as     = "SELECT sum(amount) AS total, count(*) AS n FROM orders"
}
`)

	up, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()),
		compareDeclaredLive(c, url, desired), platform.Postgres,
		planext.Options{Capabilities: capability.ForDialect(platform.Postgres)})
	c.Assert(err, qt.IsNil)
	c.Logf("up:\n%s", strings.Join(up, ";\n"))
	runRebuildStatements(c, db, up)

	c.Assert(heldViewOptions(c, db), qt.DeepEquals, []string{"report security_invoker=false,security_barrier=true", "totals fillfactor=70"})
	c.Assert(compareDeclaredLive(c, url, desired).HasChanges(), qt.IsFalse)
}
