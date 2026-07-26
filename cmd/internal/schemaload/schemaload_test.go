package schemaload_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/schemaload"
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
