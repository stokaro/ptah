//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// MySQL and MariaDB change no part of an index's key in place. A key part
// that gains a direction, a prefix or another expression rebuilds the index,
// and the diff after the apply is in sync. MariaDB has no functional key part,
// so a declared one is refused by name before the plan reaches the server.
// A key declared both on its column and as a PRIMARY KEY constraint is the
// one key the table holds, and the table is created with it.

// keyPartDocument declares t (id, a) in database with the index t_a over
// keyPart.
func keyPartDocument(database, keyPart string) string {
	return fmt.Sprintf(`schema %q {}
table "t" {
  schema = schema.%s
  column "id" {
    type = bigint
    null = false
  }
  column "a" {
    type = varchar(100)
    null = true
  }
  primary_key {
    columns = [column.id]
  }
  index "t_a" {
    %s
  }
}
`, database, database, keyPart)
}

// keyPartEngines are the two engines, their admin connection and whether they
// build a functional key part.
var keyPartEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "MySQL", engine: dbtarget.MySQLAdmin},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin},
}

// applyKeyPartDocument applies the index over keyPart and diffs the database
// against it again.
func applyKeyPartDocument(c *qt.C, database, url, keyPart string) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(keyPartDocument(database, keyPart)), 0o600), qt.IsNil)
	out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", path, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("schema", "diff", "--from", url, "--to", path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced", qt.Commentf("%s", out))
}

// heldKeyPart reads t_a's key part from information_schema.STATISTICS: its
// prefix length and its direction.
func heldKeyPart(c *qt.C, scratch mysqlScratch, database string) string {
	c.Helper()
	var held string
	c.Assert(scratch.admin.QueryRowContext(c.Context(), `
		SELECT CONCAT(COALESCE(SUB_PART, 0), ' ', COALESCE(COLLATION, ''))
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 't' AND INDEX_NAME = 't_a'`, database).Scan(&held), qt.IsNil)
	return held
}

// TestSchemaApplyRebuildsAChangedKeyPartE2E gives t_a a direction, then a
// prefix, then both, and reads each back.
func TestSchemaApplyRebuildsAChangedKeyPartE2E(t *testing.T) {
	steps := []struct {
		keyPart string
		want    string
	}{
		{keyPart: "columns = [column.a]", want: "0 A"},
		{keyPart: "on {\n      column = column.a\n      desc = true\n    }", want: "0 D"},
		{keyPart: "on {\n      column = column.a\n      prefix = 10\n    }", want: "10 A"},
		{keyPart: "on {\n      column = column.a\n      prefix = 20\n      desc = true\n    }", want: "20 D"},
	}
	for _, engine := range keyPartEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			database, url := scratch.database(c, "keypart")

			for _, step := range steps {
				applyKeyPartDocument(c, database, url, step.keyPart)
				c.Assert(heldKeyPart(c, scratch, database), qt.Equals, step.want)
			}
		})
	}
}

// TestSchemaApplyRebuildsAChangedFunctionalKeyPartMySQLE2E changes the
// expression of a functional key part on MySQL, which keeps it in its own
// spelling.
func TestSchemaApplyRebuildsAChangedFunctionalKeyPartMySQLE2E(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
	database, url := scratch.database(c, "keyexpr")

	steps := []struct {
		expression string
		want       string
	}{
		{expression: "lower(a)", want: "lower(`a`)"},
		{expression: "upper(a)", want: "upper(`a`)"},
	}
	for _, step := range steps {
		applyKeyPartDocument(c, database, url, "on {\n      expr = \""+step.expression+"\"\n    }")
		var held string
		c.Assert(scratch.admin.QueryRowContext(c.Context(), `
			SELECT EXPRESSION FROM information_schema.STATISTICS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 't' AND INDEX_NAME = 't_a'`, database).Scan(&held), qt.IsNil)
		c.Assert(held, qt.Equals, step.want)
	}
}

// TestSchemaApplyRefusesAFunctionalKeyPartMariaDBE2E refuses the functional
// key part on MariaDB by name.
func TestSchemaApplyRefusesAFunctionalKeyPartMariaDBE2E(t *testing.T) {
	c := qt.New(t)
	database, url := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin).database(c, "keyexpr")
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(keyPartDocument(database, "on {\n      expr = \"lower(a)\"\n    }")), 0o600), qt.IsNil)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", path, "--auto-approve")

	c.Assert(err, qt.ErrorMatches, `(?s).*unsupported feature: mariadb: index t_a indexes the expression lower\(a\), `+
		`and MariaDB has no functional key part.*`, qt.Commentf("%s", out))
}

// TestSchemaApplyCreatesAListedPrimaryKeyOnceE2E creates a table whose
// primary key is a PRIMARY KEY constraint named PRIMARY, declared on the
// column too or not, with an AUTO_INCREMENT column or without one, and applies
// it twice: the second apply plans nothing.
func TestSchemaApplyCreatesAListedPrimaryKeyOnceE2E(t *testing.T) {
	models := []struct {
		name   string
		column string
	}{
		{name: "on the column too", column: `name="id" type="INT" primary="true"`},
		{name: "on the column too, auto increment", column: `name="id" type="INT" primary="true" auto_increment="true"`},
		{name: "as the constraint alone, auto increment", column: `name="id" type="INT" not_null="true" auto_increment="true"`},
	}
	for _, engine := range keyPartEngines {
		for _, model := range models {
			t.Run(engine.name+"/"+model.name, func(t *testing.T) {
				c := qt.New(t)
				_, url := newMySQLFamilyScratch(c, engine.engine).database(c, "pk_listed")
				dir := c.TempDir()
				c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(`package models

//ptah:schema:table name="hk"
type HK struct {
	//ptah:schema:field `+model.column+`
	ID int
	//ptah:schema:field name="a" type="INT"
	A int
	//ptah:schema:constraint name="PRIMARY" type="PRIMARY KEY" columns="id"
	_ int
}
`), 0o600), qt.IsNil)

				runPtahNative(c, "schema", "apply", "--db-url", url, "--root-dir", dir, "--auto-approve")

				c.Assert(runPtahNative(c, "schema", "apply", "--db-url", url, "--root-dir", dir, "--dry-run"),
					qt.Contains, "Schema is synced")
			})
		}
	}
}
