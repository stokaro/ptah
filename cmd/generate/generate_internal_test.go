package generate

// White-box testing required: loadSchema is the command package's internal
// schema-source dispatcher, and its extension routing can be verified directly
// without invoking the whole CLI command.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestLoadSchemaFile_YAML(t *testing.T) {
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

	db, err := loadSchema(nil, []string{path})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
}

func TestLoadSchemaFile_AtlasHCL(t *testing.T) {
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

	db, err := loadSchema(nil, []string{path})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Fields[0].Primary, qt.IsTrue)
}

func TestLoadSchemaFile_SQL(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
`), 0o600), qt.IsNil)

	db, err := loadSchema(nil, []string{path})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Fields, qt.HasLen, 2)
}

func TestLoadSchemaFile_RejectsUnsupportedExtension(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.json")
	c.Assert(os.WriteFile(path, []byte(`{}`), 0o600), qt.IsNil)

	_, err := loadSchema(nil, []string{path})
	c.Assert(err, qt.ErrorMatches, `unsupported schema file extension ".json": only .yaml, .yml, .hcl, and .sql are supported`)
}

func TestLoadSchemaMergesMultipleRoots(t *testing.T) {
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

	db, err := loadSchema([]string{rootA, rootB}, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 2)
}

func TestLoadSchemaRejectsMissingRoot(t *testing.T) {
	c := qt.New(t)

	_, err := loadSchema([]string{filepath.Join(t.TempDir(), "does-not-exist")}, nil)
	c.Assert(err, qt.ErrorMatches, `directory does not exist: .*does-not-exist`)
}

func TestLoadSchemaMergesGoRootAndSchemaFile(t *testing.T) {
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

	db, err := loadSchema([]string{root}, []string{yamlPath})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 2)
}
