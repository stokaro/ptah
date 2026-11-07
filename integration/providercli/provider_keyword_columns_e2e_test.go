//go:build integration

package providercli_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// keywordColumnsDocument declares a table whose columns are named after the
// words that open a table-level index on MySQL. No provider states that a word
// opens an index inside CREATE TABLE, so each is a column of an engine a
// provider serves.
const keywordColumnsDocument = "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, key TEXT, spatial TEXT, fulltext TEXT, index TEXT);\n"

// A SQL schema document of an engine a provider serves creates the columns it
// names after MySQL's index keywords, and the next plan finds nothing to do.
// The fixture's SQLite is the engine, and the columns are read back from the
// database file.
func TestProviderColumnsNamedAfterIndexKeywords_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	f.schemaPath = filepath.Join(filepath.Dir(f.schemaPath), "schema.sql")
	f.declare(c, keywordColumnsDocument)

	f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(f.columns(c), qt.DeepEquals, []string{"id", "key", "spatial", "fulltext", "index"})
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}
