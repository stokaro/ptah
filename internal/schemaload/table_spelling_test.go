package schemaload_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaload"
	"ptah.run/migration/schemadiff"
)

// loadSpellingComposite loads the composite stokaro/ptah#3721 measured: a Go
// root declaring accounts without a schema and an HCL file declaring
// public.accounts. The merge keeps both, because it keys a table by the name
// as written; what the target makes of them is decided later.
func loadSpellingComposite(c *qt.C) *schemamodel.Database {
	c.Helper()
	root := c.TempDir()
	models := filepath.Join(root, "models")
	c.Assert(os.MkdirAll(models, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(models, "account.go"), []byte(`package models

//ptah:schema:table name="accounts"
type Account struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//ptah:schema:field name="email" type="TEXT" not_null="true"
	Email string
}
`), 0o600), qt.IsNil)
	hcl := filepath.Join(root, "accounts.hcl")
	c.Assert(os.WriteFile(hcl, []byte(`schema "public" {}

table "accounts" {
  schema = schema.public
  column "id" {
    type = bigint
    null = false
  }
  column "email" {
    type = varchar(320)
    null = false
  }
  primary_key {
    columns = [column.id]
  }
}
`), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{RootDirs: []string{models}, SchemaFiles: []string{hcl}})
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 2)
	return database
}

// On PostgreSQL a table without a schema is created in public, so the two
// declarations name one table and the render refuses them rather than creating
// it twice.
func TestComposite_BareAndDefaultSchemaTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := loadSpellingComposite(c)

	statements, err := renderer.GetOrderedCreateStatements(database, platform.Postgres)

	c.Assert(err, qt.ErrorMatches, `(?s).*table "accounts" is declared twice, once without a schema and once as "public\.accounts".*`)
	c.Assert(statements, qt.IsNil)
}

// The comparison refuses on the same terms, with the default schema the server
// reports: public here.
func TestComposite_BareAndDefaultSchemaTableCompared_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := loadSpellingComposite(c)

	diff, err := schemadiff.CompareWithDatabaseInfo(database, &catalog.Database{}, catalog.ServerInfo{
		Dialect:             platform.Postgres,
		IdentifierSemantics: identifier.ForDialect(platform.Postgres),
	}, nil)

	c.Assert(err, qt.ErrorMatches, `(?s).*table "accounts" is declared twice.*`)
	c.Assert(diff, qt.IsNil)
}

// A server whose search path puts another schema first makes the bare table
// app.accounts, and public.accounts a second table. The comparison uses the
// server's answer, not the catalog default, and accepts the pair.
func TestComposite_BareAndDefaultSchemaTableOnAnotherSearchPath_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := loadSpellingComposite(c)
	semantics := identifier.ForDialect(platform.Postgres)
	semantics.DefaultSchema = "app"

	diff, err := schemadiff.CompareWithDatabaseInfo(database, &catalog.Database{}, catalog.ServerInfo{
		Dialect:             platform.Postgres,
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded, qt.HasLen, 2)
}

// MySQL has no default schema: public is a database of its own, and the two
// declarations are two tables. The render keeps both.
func TestComposite_BareAndQualifiedTableOnMySQL_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := loadSpellingComposite(c)

	statements, err := renderer.GetOrderedCreateStatements(database, platform.MySQL)

	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "")
	c.Assert(rendered, qt.Contains, "CREATE TABLE `accounts` (")
	c.Assert(rendered, qt.Contains, "CREATE TABLE `public`.`accounts` (")
}
