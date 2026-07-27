package schemaload_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/schemaload"
	"github.com/stokaro/ptah/cmd/internal/schemasource"
	"github.com/stokaro/ptah/core/renderer"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestLoad_YAMLSchemaFile(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.yaml")
	c.Assert(
		os.WriteFile(path, []byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
`), 0o600),
		qt.IsNil,
	)

	db, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
}

func TestLoad_AtlasHCLSchemaFile(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`
table "users" {
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`), 0o600), qt.IsNil)

	db, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Fields[0].Primary, qt.IsTrue)
}

func TestLoad_SQLSchemaFile(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
`), 0o600), qt.IsNil)

	db, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Fields, qt.HasLen, 2)
}

func TestLoadResult_LocalSchemaHasNoOCIProvenance(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	result, err := schemaload.LoadResult(context.Background(), schemaload.Options{
		SchemaFiles: []string{path},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Database.Tables, qt.HasLen, 1)
	c.Assert(result.OCI, qt.IsNil)
}

func TestLoad_RejectsUnsupportedExtension(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.json")
	c.Assert(os.WriteFile(path, []byte(`{}`), 0o600), qt.IsNil)

	_, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})
	c.Assert(err, qt.ErrorMatches, `unsupported schema file extension ".json": only .yaml, .yml, .hcl, and .sql are supported`)
}

func TestLoad_MergesMultipleRoots(t *testing.T) {
	c := qt.New(t)

	rootA := t.TempDir()
	rootB := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(rootA, "users.go"), []byte(`package entities

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(rootB, "orders.go"), []byte(`package entities

//migrator:schema:table name="orders"
type Order struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	db, err := schemaload.Load(schemaload.Options{RootDirs: []string{rootA, rootB}})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 2)
}

func TestLoad_RejectsMissingRoot(t *testing.T) {
	c := qt.New(t)

	_, err := schemaload.Load(schemaload.Options{RootDirs: []string{filepath.Join(t.TempDir(), "does-not-exist")}})
	c.Assert(err, qt.ErrorMatches, `directory does not exist: .*does-not-exist`)
}

func TestLoad_MergesGoRootAndSchemaFile(t *testing.T) {
	c := qt.New(t)

	root := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "users.go"), []byte(`package entities

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	yamlPath := filepath.Join(t.TempDir(), "orders.yaml")
	c.Assert(os.WriteFile(yamlPath, []byte(`
tables:
  orders:
    columns:
      id: { type: SERIAL, primary: true }
`), 0o600), qt.IsNil)

	db, err := schemaload.Load(schemaload.Options{RootDirs: []string{root}, SchemaFiles: []string{yamlPath}})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 2)
}

func TestLoad_MergesGoYAMLAndAtlasHCLSources(t *testing.T) {
	c := qt.New(t)

	root := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "users.go"), []byte(`package entities

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "orders.yaml")
	c.Assert(os.WriteFile(yamlPath, []byte(`
tables:
  orders:
    columns:
      id: { type: SERIAL, primary: true }
`), 0o600), qt.IsNil)
	hclPath := filepath.Join(dir, "products.hcl")
	c.Assert(os.WriteFile(hclPath, []byte(`
table "products" {
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`), 0o600), qt.IsNil)

	db, err := schemaload.Load(schemaload.Options{
		RootDirs:    []string{root},
		SchemaFiles: []string{yamlPath, hclPath},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 3)
}

func TestLoad_MergesMultipleSchemaFiles(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	usersPath := filepath.Join(dir, "users.yaml")
	c.Assert(os.WriteFile(usersPath, []byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
`), 0o600), qt.IsNil)
	ordersPath := filepath.Join(dir, "orders.sql")
	c.Assert(os.WriteFile(ordersPath, []byte(`
CREATE TABLE orders (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)

	db, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{usersPath, ordersPath}})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 2)
}

func TestLoad_ReportsProgressThroughLogf(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.yaml")
	c.Assert(os.WriteFile(path, []byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
`), 0o600), qt.IsNil)

	var messages []string
	_, err := schemaload.Load(schemaload.Options{
		SchemaFiles: []string{path},
		Logf:        func(format string, args ...any) { messages = append(messages, format) },
	})
	c.Assert(err, qt.IsNil)
	c.Assert(messages, qt.HasLen, 1)
	c.Assert(messages[0], qt.Equals, "Reading schema file: %s")
}

// runHelperProcess emits a fixed SQL schema when this test binary is re-executed
// as an external schema command. It is not itself a test, which keeps
// TestHelperProcess free of control flow.
func runHelperProcess() {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	fmt.Fprint(os.Stdout, "CREATE TABLE orders (\n  id INTEGER PRIMARY KEY\n);\n")
	os.Exit(0)
}

// TestHelperProcess is not a real test; the command tests re-execute this binary
// with -test.run=TestHelperProcess to stand in for an external schema program.
func TestHelperProcess(t *testing.T) {
	runHelperProcess()
}

func schemaCommand() schemasource.Command {
	return schemasource.Command{
		Args: []string{os.Args[0], "-test.run=TestHelperProcess"},
		Env:  []string{"GO_WANT_HELPER_PROCESS=1"},
	}
}

func TestLoad_RunsSchemaCommand(t *testing.T) {
	c := qt.New(t)

	db, err := schemaload.LoadContext(context.Background(), schemaload.Options{
		Commands: []schemasource.Command{schemaCommand()},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "orders")
}

func TestLoad_MergesGoRootAndSchemaCommand(t *testing.T) {
	c := qt.New(t)

	root := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "users.go"), []byte(`package entities

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	db, err := schemaload.LoadContext(context.Background(), schemaload.Options{
		RootDirs: []string{root},
		Commands: []schemasource.Command{schemaCommand()},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 2)
}

func TestLoadContext_OCIReferenceFailurePath(t *testing.T) {
	c := qt.New(t)

	db, err := schemaload.LoadContext(context.Background(), schemaload.Options{
		SchemaFiles: []string{"oci://registry.invalid"},
	})

	c.Assert(err, qt.ErrorMatches, "invalid OCI reference: invalid reference: missing registry or repository")
	c.Assert(db, qt.IsNil)
}

func TestLoad_IdenticalGoAndYAMLSourcesDeduplicate(t *testing.T) {
	c := qt.New(t)

	root := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "users.go"), []byte(`package entities

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	yamlPath := filepath.Join(t.TempDir(), "users.yaml")
	c.Assert(os.WriteFile(yamlPath, []byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
`), 0o600), qt.IsNil)

	db, err := schemaload.Load(schemaload.Options{
		RootDirs:    []string{root},
		SchemaFiles: []string{yamlPath},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Fields, qt.HasLen, 1)
}

func TestLoad_ConflictingGoAndYAMLSourcesFail(t *testing.T) {
	c := qt.New(t)

	root := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "users.go"), []byte(`package entities

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	yamlPath := filepath.Join(t.TempDir(), "users.yaml")
	c.Assert(os.WriteFile(yamlPath, []byte(`
tables:
  users:
    columns:
      id: { type: UUID, primary: true }
`), 0o600), qt.IsNil)

	db, err := schemaload.Load(schemaload.Options{
		RootDirs:    []string{root},
		SchemaFiles: []string{yamlPath},
	})

	c.Assert(err, qt.ErrorMatches, `error merging composite schema: conflicting field "id" definitions on table "users"`)
	c.Assert(db, qt.IsNil)
}

func TestLoad_CompositeMatchesHandMergedAcrossConsumers(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	usersPath := filepath.Join(dir, "users.yaml")
	c.Assert(os.WriteFile(usersPath, []byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
      email: { type: VARCHAR(255), not_null: true, unique: true }
`), 0o600), qt.IsNil)
	ordersPath := filepath.Join(dir, "orders.yaml")
	c.Assert(os.WriteFile(ordersPath, []byte(`
tables:
  orders:
    columns:
      id: { type: SERIAL, primary: true }
      reference: { type: VARCHAR(64), not_null: true }
    indexes:
      idx_orders_reference: { fields: [reference], unique: true }
`), 0o600), qt.IsNil)
	handMergedPath := filepath.Join(dir, "hand-merged.yaml")
	c.Assert(os.WriteFile(handMergedPath, []byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
      email: { type: VARCHAR(255), not_null: true, unique: true }
  orders:
    columns:
      id: { type: SERIAL, primary: true }
      reference: { type: VARCHAR(64), not_null: true }
    indexes:
      idx_orders_reference: { fields: [reference], unique: true }
`), 0o600), qt.IsNil)

	composite, err := schemaload.Load(schemaload.Options{
		SchemaFiles: []string{usersPath, ordersPath},
	})
	c.Assert(err, qt.IsNil)
	handMerged, err := schemaload.Load(schemaload.Options{
		SchemaFiles: []string{handMergedPath},
	})
	c.Assert(err, qt.IsNil)

	compositeRender, err := renderer.GetOrderedCreateStatements(composite, "postgres")
	c.Assert(err, qt.IsNil)
	handMergedRender, err := renderer.GetOrderedCreateStatements(handMerged, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(compositeRender, qt.DeepEquals, handMergedRender)

	emptyDatabase := &dbschematypes.DBSchema{}
	compositeDiff := schemadiff.CompareWithDialect(composite, emptyDatabase, "postgres")
	handMergedDiff := schemadiff.CompareWithDialect(handMerged, emptyDatabase, "postgres")
	c.Assert(compositeDiff, qt.DeepEquals, handMergedDiff)

	compositeMigration, err := planner.GenerateSchemaDiffSQL(compositeDiff, composite, "postgres")
	c.Assert(err, qt.IsNil)
	handMergedMigration, err := planner.GenerateSchemaDiffSQL(handMergedDiff, handMerged, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(compositeMigration, qt.Equals, handMergedMigration)
}
