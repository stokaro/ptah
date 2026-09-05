package dbtest_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/migration/dbtest"
)

// ExampleRunMigrationTest runs a complete migration test with zero
// infrastructure: the migration history is an in-memory [fstest.MapFS] and the
// empty DBURL provisions an ephemeral SQLite database per case, so the loop the
// CLI runs as `ptah migrations test` is exercised without a server, a
// migrations directory on disk, or a database URL of your own. MigrationsDir
// names the history a migrate_to step moves through; with MigrationsFS set the
// snapshot supplies every byte and the directory is never opened.
func ExampleRunMigrationTest() {
	migrations := fstest.MapFS{
		"0000000001_create_products.up.sql": {Data: []byte(
			"CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
		)},
		"0000000001_create_products.down.sql": {Data: []byte(
			"DROP TABLE products;",
		)},
	}

	cases := []dbtest.Case{{
		Name: "products table accepts rows",
		Steps: []dbtest.Step{
			{Name: "migrate to latest", MigrateTo: "latest"},
			{Name: "insert a product", Exec: "INSERT INTO products (name) VALUES ('widget')"},
			{Name: "exactly one product exists", Assert: &dbtest.Assertion{
				Query: "SELECT id FROM products", RowCount: new(1),
			}},
			{Name: "the product is named widget", Assert: &dbtest.Assertion{
				Query: "SELECT name FROM products LIMIT 1", Scalar: new("widget"),
			}},
		},
	}}

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases:         cases,
		MigrationsDir: "migrations",
		MigrationsFS:  migrations,
	})
	if err != nil {
		fmt.Println("run failed:", err)
		return
	}
	fmt.Print(report.Text())

	// Output:
	// === MIGRATION TEST ===
	// PASS  case "products table accepts rows"
	//     PASS    step "migrate to latest" — migrated to latest
	//     PASS    step "insert a product" — exec ok
	//     PASS    step "exactly one product exists" — row_count 1
	//     PASS    step "the product is named widget" — scalar "widget"
	//
	// 1 cases, 1 passed, 0 failed, 0 skipped
}

// ExampleRunSchemaTest exercises a desired schema without authoring
// migrations: the schema parsed from the Go entity annotations under RootDir
// is converged onto each case's fresh ephemeral database before that case's
// steps run, so the first step can insert immediately — no apply_schema step
// is needed.
func ExampleRunSchemaTest() {
	rootDir := must.Must(os.MkdirTemp("", "dbtest-example"))
	defer os.RemoveAll(rootDir)

	entity := []byte(`package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`)
	must.Assert(os.WriteFile(filepath.Join(rootDir, "user.go"), entity, 0o600))

	cases := []dbtest.Case{{
		Name: "users accepts a row",
		Steps: []dbtest.Step{
			{Name: "insert a user", Exec: "INSERT INTO users (id, name) VALUES (1, 'ada')"},
			{Name: "the user is retrievable", Assert: &dbtest.Assertion{
				Query: "SELECT name FROM users WHERE id = 1", Scalar: new("ada"),
			}},
		},
	}}

	report, err := dbtest.RunSchemaTest(context.Background(), dbtest.SchemaOptions{
		Cases:   cases,
		RootDir: rootDir,
	})
	if err != nil {
		fmt.Println("run failed:", err)
		return
	}
	fmt.Print(report.Text())

	// Output:
	// === SCHEMA TEST ===
	// PASS  case "users accepts a row"
	//     PASS    step "insert a user" — exec ok
	//     PASS    step "the user is retrievable" — scalar "ada"
	//
	// 1 cases, 1 passed, 0 failed, 0 skipped
}

// ExampleParseCases loads the YAML authoring format the package doc describes:
// a document with a top-level cases: list, each case an ordered list of steps.
// It is the entry point for cases kept beside the code they test rather than
// constructed in Go.
func ExampleParseCases() {
	document := []byte(`
cases:
  - name: products accepts rows
    steps:
      - name: migrate to latest
        migrate_to: latest
      - name: insert a product
        exec: INSERT INTO products (name) VALUES ('widget')
  - name: unknown table errors
    steps:
      - name: query a missing table
        assert:
          query: SELECT * FROM does_not_exist
          error_contains: does_not_exist
`)

	cases := must.Must(dbtest.ParseCases(document))
	for _, testCase := range cases {
		fmt.Printf("%s: %d step(s)\n", testCase.Name, len(testCase.Steps))
	}

	// Output:
	// products accepts rows: 2 step(s)
	// unknown table errors: 1 step(s)
}

// ExampleParseCases_unknownKey shows that unknown fields are rejected: a typo
// in a step key is a parse error naming the line, not a step that silently
// performs no action while the case passes around it.
func ExampleParseCases_unknownKey() {
	_, err := dbtest.ParseCases([]byte(`
cases:
  - name: typo
    steps:
      - name: migrate
        migrate_too: latest
`))
	fmt.Println(err)

	// Output:
	// parse test cases: yaml: unmarshal errors:
	//   line 6: field migrate_too not found in type dbtest.Step
}

// ExampleFilterCases selects cases by name the way the CLI's --run flag does:
// the pattern is an unanchored Go regular expression, so it may match anywhere
// in the name, and the empty pattern selects every case.
func ExampleFilterCases() {
	cases := []dbtest.Case{
		{Name: "products: insert"},
		{Name: "products: delete"},
		{Name: "orders: insert"},
	}

	matched := must.Must(dbtest.FilterCases(cases, "insert"))
	for _, testCase := range matched {
		fmt.Println(testCase.Name)
	}

	all := must.Must(dbtest.FilterCases(cases, ""))
	fmt.Println(len(all), "cases with the empty pattern")

	// Output:
	// products: insert
	// orders: insert
	// 3 cases with the empty pattern
}

// ExampleParseAtlasTestCases translates an Atlas-format `.test.hcl` document
// into native cases — the entry point for embedders porting an Atlas test
// suite. An exec block carrying `output =` becomes a scalar assertion, because
// that is what it means in Atlas; an exec without it stays a plain statement.
func ExampleParseAtlasTestCases() {
	document := []byte(`
test "schema" "users_insert_select" {
  exec {
    sql = "INSERT INTO users (id, name) VALUES (1, 'ada')"
  }
  exec {
    sql    = "SELECT name FROM users WHERE id = 1"
    output = "ada"
  }
}
`)

	cases := must.Must(dbtest.ParseAtlasTestCases(document, "users.test.hcl", dbtest.AtlasTestKindSchema))
	for _, testCase := range cases {
		fmt.Println("case:", testCase.Name)
		for _, step := range testCase.Steps {
			switch {
			case step.Exec != "":
				fmt.Println("  exec:", step.Exec)
			case step.Assert != nil:
				fmt.Printf("  assert result set %q for: %s\n", *step.Assert.ResultSet, step.Assert.Query)
			}
		}
	}

	// Output:
	// case: users_insert_select
	//   exec: INSERT INTO users (id, name) VALUES (1, 'ada')
	//   assert result set "ada" for: SELECT name FROM users WHERE id = 1
}
