package schemafile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemafile"
)

const (
	oneSchemaHCL = `schema "main" {}
table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`
	twoSchemasHCL = `schema "main" {}
schema "other" {}
table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`
	sameSchemaTwiceHCL = `schema "main" {}
schema "main" {}
table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`
	postsHCL = `schema "main" {}
table "posts" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`
)

// TestLoadRefusesMoreSchemasThanTheRunCanReach is the regression test for a
// document whose extra schemas were silently dropped.
//
// Every row is one measurement against the pinned Atlas community binary
// v1.3.0, not a restatement of this implementation. The command was
// `schema inspect -u file://<source> --dev-url <url>`, exit codes read from
// unpiped invocations, with a throwaway PostgreSQL 17.10 database recreated
// between runs:
//
//	two schema blocks, --dev-url sqlite://dv?mode=memory      exit 1
//	two schema blocks, --dev-url postgres://…?search_path=…   exit 1
//	two schema blocks, --dev-url postgres://…  (no search_path) exit 0
//	`schema "main"` twice, --dev-url sqlite://dv?mode=memory   exit 1
//	a directory of two files that each open with `schema "main"`,
//	  --dev-url sqlite://dv?mode=memory                        exit 1
//
// Ptah exited 0 on all of the refusing ones, describing only the schema the URL
// reached: `schema diff --from one.hcl --to two-schemas.hcl` answered "Schemas
// are synced, no changes to be made" and `migrate diff` wrote a migration file
// covering half the document.
//
// The last two rows carry the model. Counting DISTINCT SCHEMA NAMES instead of
// BLOCKS passes every other row and exits 0 on both of them, where that binary
// exits 1 -- and the directory row is the layout a per-file count also gets
// wrong, since each file there declares exactly one.
func TestLoadRefusesMoreSchemasThanTheRunCanReach(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		opts    schemafile.Options
		wantErr string
	}{
		{
			name:  "two schemas under a limited run refuse and name both blocks",
			files: map[string]string{"schema.hcl": twoSchemasHCL},
			opts:  schemafile.Options{Dialect: "sqlite", SchemaScope: "main", SchemaScopeFlag: "dev-url"},
			wantErr: `cannot use HCL with more than 1 schema when dev-url is limited to schema "main": ` +
				`2 top-level schema blocks are declared: "main" at .*schema\.hcl:1, "other" at .*schema\.hcl:2`,
		},
		{
			name:  "the same schema declared twice is two blocks and refuses",
			files: map[string]string{"schema.hcl": sameSchemaTwiceHCL},
			opts:  schemafile.Options{Dialect: "sqlite", SchemaScope: "main", SchemaScopeFlag: "dev-url"},
			wantErr: `cannot use HCL with more than 1 schema when dev-url is limited to schema "main": ` +
				`2 top-level schema blocks are declared: "main" at .*, "main" at .*`,
		},
		{
			name: "a directory whose files each open with a schema block refuses",
			files: map[string]string{
				"a.hcl": oneSchemaHCL,
				"b.hcl": postsHCL,
			},
			opts: schemafile.Options{Dialect: "sqlite", SchemaScope: "main", SchemaScopeFlag: "dev-url"},
			wantErr: `cannot use HCL with more than 1 schema when dev-url is limited to schema "main": ` +
				`2 top-level schema blocks are declared: "main" at .*a\.hcl:1, "main" at .*b\.hcl:1`,
		},
		{
			name:    "the flag the run was limited by is the one quoted",
			files:   map[string]string{"schema.hcl": twoSchemasHCL},
			opts:    schemafile.Options{Dialect: "sqlite", SchemaScope: "public", SchemaScopeFlag: "url"},
			wantErr: `cannot use HCL with more than 1 schema when url is limited to schema "public": .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaDir(c, test.files)

			db, err := schemafile.LoadPath(dir, test.opts)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			// A refusal that still handed back a document would let a caller
			// that ignores the error migrate the half the run can reach.
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestLoadAcceptsADocumentTheRunCanReach is the boundary of the refusal above:
// every row here declares no more schemas than the run reaches, so the gate must
// stay out of the way and the whole document must arrive.
//
// The last two rows are the ones that keep the gate from being written as "a
// limited run refuses a document with no schema block of its own" and as "a
// limited run refuses a directory of several files", both of which pass every
// refusing row.
func TestLoadAcceptsADocumentTheRunCanReach(t *testing.T) {
	tests := []struct {
		name        string
		files       map[string]string
		opts        schemafile.Options
		wantSchemas []string
		wantTables  []string
	}{
		{
			name:        "two schemas on a realm-scoped run load both",
			files:       map[string]string{"schema.hcl": twoSchemasHCL},
			opts:        schemafile.Options{Dialect: "sqlite"},
			wantSchemas: []string{"main", "other"},
			wantTables:  []string{"users"},
		},
		{
			name:        "one schema under a limited run loads",
			files:       map[string]string{"schema.hcl": oneSchemaHCL},
			opts:        schemafile.Options{Dialect: "sqlite", SchemaScope: "main", SchemaScopeFlag: "dev-url"},
			wantSchemas: []string{"main"},
			wantTables:  []string{"users"},
		},
		{
			name:        "a document with no schema block at all loads under a limited run",
			files:       map[string]string{"schema.hcl": postsHCL[len("schema \"main\" {}\n"):]},
			opts:        schemafile.Options{Dialect: "sqlite", SchemaScope: "main", SchemaScopeFlag: "dev-url"},
			wantSchemas: make([]string, 0),
			wantTables:  []string{"posts"},
		},
		{
			name: "a SQL directory is unaffected by the gate",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "CREATE TABLE posts (id INTEGER PRIMARY KEY);\n",
			},
			opts:        schemafile.Options{Dialect: "sqlite", SchemaScope: "main", SchemaScopeFlag: "dev-url"},
			wantSchemas: make([]string, 0),
			// Sorted rather than in file order: the SQL path finishes through
			// goschema.Finalize, which orders tables by their dependencies.
			wantTables: []string{"posts", "users"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaDir(c, test.files)

			db, err := schemafile.LoadPath(dir, test.opts)

			c.Assert(err, qt.IsNil)
			c.Assert(schemaNames(db), qt.DeepEquals, test.wantSchemas)
			c.Assert(tableNames(db), qt.DeepEquals, test.wantTables)
		})
	}
}

// TestLoadAllCountsSchemaBlocksAcrossEveryURL pins that several --to files are
// one desired state for the gate.
//
// It is a separate test because it goes through LoadAll rather than LoadPath:
// `schema apply --to a.hcl --to b.hcl` reaches the loader once per URL, and a
// gate that ran per URL would count one block twice instead of two blocks once.
func TestLoadAllCountsSchemaBlocksAcrossEveryURL(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{"a.hcl": oneSchemaHCL, "b.hcl": postsHCL})

	limited := schemafile.Options{Dialect: "sqlite", SchemaScope: "main", SchemaScopeFlag: "dev-url"}
	_, err := schemafile.LoadAll([]string{dir + "/a.hcl", dir + "/b.hcl"}, limited)
	c.Assert(err, qt.ErrorMatches,
		`cannot use HCL with more than 1 schema when dev-url is limited to schema "main": `+
			`2 top-level schema blocks are declared: "main" at .*a\.hcl:1, "main" at .*b\.hcl:1`)

	db, err := schemafile.LoadAll([]string{dir + "/a.hcl"}, limited)
	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(db), qt.DeepEquals, []string{"users"})
}

// TestScopeFromURLsPrefersTheDevURL pins the precedence and the flag spelling
// against the pinned community binary's own diagnostic. Measured on PostgreSQL
// 17.10, `schema apply --to <two-schema file>` with one throwaway database per
// side:
//
//	--url realm, --dev-url ?search_path=public   "…when dev-url is limited to schema \"public\""
//	--url ?search_path=public, --dev-url realm   "…when url is limited to schema \"public\""
//
// A derivation that consulted the target first would report `url` for the first
// row, which is a different sentence than the one a script may match on.
func TestScopeFromURLsPrefersTheDevURL(t *testing.T) {
	const (
		realm  = "postgres://localhost:5432/db?sslmode=disable"
		scoped = "postgres://localhost:5432/db?sslmode=disable&search_path=public"
	)

	tests := []struct {
		name       string
		devURL     string
		targetURL  string
		targetFlag string
		scope      string
		flag       string
	}{
		{name: "both limited reports the dev url", devURL: scoped, targetURL: scoped, targetFlag: "url", scope: "public", flag: "dev-url"},
		{name: "only the dev url is limited", devURL: scoped, targetURL: realm, targetFlag: "url", scope: "public", flag: "dev-url"},
		{name: "only the target is limited", devURL: realm, targetURL: scoped, targetFlag: "url", scope: "public", flag: "url"},
		{name: "the target flag is the caller's", devURL: realm, targetURL: scoped, targetFlag: "from", scope: "public", flag: "from"},
		{name: "neither is limited", devURL: realm, targetURL: realm, targetFlag: "url", scope: "", flag: ""},
		{name: "no target url to consult", devURL: realm, targetURL: "", targetFlag: "", scope: "", flag: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			scope, flag := schemafile.ScopeFromURLs(test.devURL, test.targetURL, test.targetFlag)

			c.Assert(scope, qt.Equals, test.scope)
			c.Assert(flag, qt.Equals, test.flag)
		})
	}
}

func schemaNames(db *goschema.Database) []string {
	names := make([]string, 0, len(db.Schemas))
	for _, schema := range db.Schemas {
		names = append(names, schema.Name)
	}
	return names
}
