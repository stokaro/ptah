package schemafile_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

// tableGrammar is an owner of target "custom" that reads every document of a
// load itself: each document declares the one table its text names. It
// records the requests it is given.
type tableGrammar struct {
	requests *[]sqlext.GrammarRequest
}

func (g tableGrammar) ReadSQL(_ context.Context, request sqlext.GrammarRequest) (sqlext.GrammarResult, error) {
	*g.requests = append(*g.requests, request)
	var result sqlext.GrammarResult
	for _, file := range request.Files {
		name := strings.TrimSpace(file.Text)
		result.Database.Tables = append(result.Database.Tables, schemamodel.Table{Name: name, StructName: name})
		result.Files = append(result.Files, sqlext.FileDeclarations{Path: file.Path,
			Objects: []sqlext.Declaration{{Kind: "table", Name: name}},
			Tables:  []sqlext.TableStatement{{Name: name}}})
	}
	return result, nil
}

// grammarOptions loads documents of target "custom", whose documents
// tableGrammar reads.
func grammarOptions(requests *[]sqlext.GrammarRequest) schemafile.Options {
	runtime := must.Must(engine.New(engine.Provider{
		ID:      "example.org/custom",
		Targets: []engine.Target{{Name: "custom"}},
		SQL:     []sqlext.Extension{{Owner: "example.org/custom", Targets: []string{"custom"}, Grammar: tableGrammar{requests: requests}}},
	}))
	return schemafile.Options{YAML: yamlext.None(), SQL: runtime.SQL(), HCL: runtime.HCL(), Dialect: "custom"}
}

// requestedFiles lists each file of request by its base name and its text.
// The loader names a file by its resolved path, which the temporary directory
// a test writes to may not be.
func requestedFiles(request sqlext.GrammarRequest) []string {
	listed := make([]string, 0, len(request.Files))
	for _, file := range request.Files {
		listed = append(listed, filepath.Base(file.Path)+": "+file.Text)
	}
	return listed
}

func writeFiles(c *qt.C, dir string, files map[string]string) {
	c.Helper()
	for name, text := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600), qt.IsNil)
	}
}

// TestLoadPath_ReadsAGrammarDirectoryInOneCall hands every file of a schema
// directory to the target's grammar in one call, in filename order, and
// returns what the owner declares.
func TestLoadPath_ReadsAGrammarDirectoryInOneCall(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeFiles(c, dir, map[string]string{"001.sql": "first", "002.sql": "second"})
	var requests []sqlext.GrammarRequest

	database, err := schemafile.LoadPath(t.Context(), dir, grammarOptions(&requests))

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Target, qt.Equals, "custom")
	c.Assert(requestedFiles(requests[0]), qt.DeepEquals, []string{"001.sql: first", "002.sql: second"})
	c.Assert(database.Tables, qt.HasLen, 2)
}

// TestLoadPath_ChecksTheOwnersDeclarationsOfADirectory refuses a directory
// whose later file declares again what an earlier one did, from the
// declarations the owner answers, as it does for a directory the shared
// grammar reads.
func TestLoadPath_ChecksTheOwnersDeclarationsOfADirectory(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeFiles(c, dir, map[string]string{"001.sql": "same", "002.sql": "same"})
	var requests []sqlext.GrammarRequest

	database, err := schemafile.LoadPath(t.Context(), dir, grammarOptions(&requests))

	c.Assert(err, qt.ErrorMatches, `read state from "002.sql": table "same" already exists`)
	c.Assert(database, qt.IsNil)
}

// TestLoadAll_ReadsEveryGrammarSourceInOneCall reads the SQL sources of a
// source list, a file and a directory, as one load, in the list's order.
func TestLoadAll_ReadsEveryGrammarSourceInOneCall(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "dir")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	writeFiles(c, root, map[string]string{"main.sql": "head"})
	writeFiles(c, dir, map[string]string{"001.sql": "tail"})
	var requests []sqlext.GrammarRequest

	database, err := schemafile.LoadAll(t.Context(), []string{filepath.Join(root, "main.sql"), dir}, grammarOptions(&requests))

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requestedFiles(requests[0]), qt.DeepEquals, []string{"main.sql: head", "001.sql: tail"})
	c.Assert(database.Tables, qt.HasLen, 2)
}

const (
	yqlUsers    = "CREATE TABLE users (id Int64 NOT NULL, PRIMARY KEY (id));\n"
	yqlFeed     = "ALTER TABLE users ADD CHANGEFEED updates WITH (mode = 'UPDATES', format = 'JSON');\n"
	hclUsersYDB = "schema \"main\" {\n}\ntable \"users\" {\n  schema = schema.main\n  column \"id\" {\n    null = false\n    type = Int64\n  }\n  primary_key {\n    columns = [column.id]\n  }\n}\n"
)

// TestLoadAll_AYQLFileChangesWhatTheLoadDeclares is the control for the
// refusal below: a YQL file of a source list may change a table another YQL
// source of the list declares, a file or a directory, before or after it.
func TestLoadAll_AYQLFileChangesWhatTheLoadDeclares(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "tables")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	writeFiles(c, dir, map[string]string{"001.sql": yqlUsers})
	writeFiles(c, root, map[string]string{"feed.sql": yqlFeed})
	opts := schemafile.Options{YAML: builtintest.Runtime().YAML(), SQL: builtintest.Runtime().SQL(), HCL: builtintest.Runtime().HCL(), Dialect: "ydb"}

	database, err := schemafile.LoadAll(t.Context(), []string{dir, filepath.Join(root, "feed.sql")}, opts)

	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(ydbschema.DesiredChangefeeds(database.FeatureObjects, "", "users")), qt.HasLen, 1)
}

// TestLoadAll_AYQLFileCannotChangeWhatAnHCLSourceDeclares refuses a YQL file
// that changes a table an HCL source of the same list declares. The YDB owner
// reads the YQL files of a load together and sees none of the HCL source, so
// the refusal says that a YQL file changes only what the YQL files of its load
// declare, and names the source it cannot see.
func TestLoadAll_AYQLFileCannotChangeWhatAnHCLSourceDeclares(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeFiles(c, root, map[string]string{"users.hcl": hclUsersYDB, "feed.sql": yqlFeed})
	opts := schemafile.Options{YAML: builtintest.Runtime().YAML(), SQL: builtintest.Runtime().SQL(), HCL: builtintest.Runtime().HCL(), Dialect: "ydb"}

	database, err := schemafile.LoadAll(t.Context(), []string{filepath.Join(root, "users.hcl"), filepath.Join(root, "feed.sql")}, opts)

	c.Assert(err, qt.ErrorMatches, `read SQL schema file .*feed\.sql: the schema model has no place for this statement: `+
		`ALTER TABLE users ADD CHANGEFEED updates names a table this schema does not declare; `+
		`a ydb SQL file changes only what the SQL files of the same load declare, not what users\.hcl declares`)
	c.Assert(err, qt.ErrorIs, sqlext.ErrUndeclared)
	c.Assert(database, qt.IsNil)
}
