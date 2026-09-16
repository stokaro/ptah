//go:build integration

package integration_test

// The mirror of table_element_keyword_column_live_test.go: the engines that
// read one of MySQL's index keywords as an index rather than as a column name.
//
// There a column carrying the word is what the engine accepts, and Ptah has to
// read it as a column. Here the engine refuses the statement, and Ptah has to
// refuse it too. What made this worth a test is how it failed: a type carrying
// a length has the shape of `KEY name (columns)`, so `key varchar(32)` parsed
// as an index named `varchar` over a column named `32`, the column the author
// wrote was gone from the model, and `ptah sql lint` reported nothing
// (stokaro/ptah#3329).
//
// The engine supplies the expectation in both directions: each row executes its
// own DDL and asserts the server refused it, so a row whose engine turns out to
// accept the statement fails here rather than pinning a refusal Ptah invented.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemaload"
)

// keywordRefusedTarget is one engine and the words it will not take as a column
// name, each with a type carrying a length.
type keywordRefusedTarget struct {
	name     string
	engine   dbtarget.Engine
	dialect  string
	idColumn string
	// columnType carries parentheses on purpose: that is the shape that parsed
	// silently.
	columnType string
	words      []string
}

var keywordRefusedTargets = []keywordRefusedTarget{
	{
		name: "mysql", engine: dbtarget.MySQL, dialect: platform.MySQL,
		idColumn: "id INT PRIMARY KEY", columnType: "varchar(32)",
		words: []string{"key", "spatial", "fulltext", "index"},
	},
	{
		name: "mariadb", engine: dbtarget.MariaDB, dialect: platform.MariaDB,
		idColumn: "id INT PRIMARY KEY", columnType: "varchar(32)",
		words: []string{"key", "spatial", "fulltext", "index"},
	},
	{
		// SQL Server takes spatial and fulltext as columns, which the sibling
		// file covers. These two it reads as an index: KEY is reserved, and
		// INDEX opens its own inline index element.
		name: "sqlserver", engine: dbtarget.SQLServer, dialect: platform.SQLServer,
		idColumn: "id int PRIMARY KEY", columnType: "nvarchar(32)",
		words: []string{"key", "index"},
	},
	{
		name: "oracle", engine: dbtarget.Oracle, dialect: platform.Oracle,
		idColumn: "id NUMBER PRIMARY KEY", columnType: "VARCHAR2(32)",
		words: []string{"index"},
	},
}

// TestTableElementKeywordRefusedLive holds Ptah to the engine's refusal.
func TestTableElementKeywordRefusedLive(t *testing.T) {
	for _, target := range keywordRefusedTargets {
		t.Run(target.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(t.Context(), dbtarget.URL(c, target.engine))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			for _, word := range target.words {
				t.Run(word, func(t *testing.T) {
					c := qt.New(t)
					table := fmt.Sprintf("ptah_kwr_%s", word)
					ddl := fmt.Sprintf("CREATE TABLE %s (%s, %s %s)",
						table, target.idColumn, word, target.columnType)

					// The engine's own answer, first. Nothing is created, so
					// nothing needs dropping.
					_, execErr := conn.ExecContext(t.Context(), ddl)
					c.Assert(execErr, qt.IsNotNil, qt.Commentf("the engine accepted %s", ddl))

					loadErr := loadSchemaError(c, ddl, target.dialect)
					c.Assert(loadErr, qt.IsNotNil,
						qt.Commentf("the engine refused %s with %v, and Ptah read it", ddl, execErr))
					c.Assert(loadErr, qt.ErrorMatches,
						fmt.Sprintf("(?s).*a table element opening with %s .*is a number.*", strings.ToUpper(word)),
						qt.Commentf("engine said: %v", execErr))
				})
			}
		})
	}
}

// loadSchemaError returns what the loader answered for one CREATE TABLE.
func loadSchemaError(c *qt.C, ddl, dialect string) error {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(ddl+";\n"), 0o600), qt.IsNil)

	_, err := schemaload.LoadContext(c.Context(), schemaload.Options{
		SchemaFiles: []string{path},
		Dialect:     dialect,
	})
	return err
}
