package schemaload_test

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
	"ptah.run/engine"
	"ptah.run/internal/schemaload"
)

type sourceListGrammar struct{ requests *[]sqlext.GrammarRequest }

func (g sourceListGrammar) ReadSQL(_ context.Context, request sqlext.GrammarRequest) (sqlext.GrammarResult, error) {
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

// A CLI composite source list must give every SQL document to its owner in
// one call, whether an entry names a directory or a file.
func TestLoad_GrammarReadsSQLSourceListTogether(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "tables")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	first := filepath.Join(dir, "001.sql")
	second := filepath.Join(root, "feed.sql")
	c.Assert(os.WriteFile(first, []byte("users"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(second, []byte("orders"), 0o600), qt.IsNil)
	var requests []sqlext.GrammarRequest
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/custom", Targets: []engine.Target{{Name: "custom"}},
		SQL: []sqlext.Extension{{Owner: "example.org/custom", Targets: []string{"custom"}, Grammar: sourceListGrammar{requests: &requests}}}}))

	db, err := schemaload.Load(schemaload.Options{Owners: runtime, Dialect: "custom", SchemaFiles: []string{dir, second}})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Files, qt.HasLen, 2)
	c.Assert(filepath.Base(requests[0].Files[0].Path), qt.Equals, "001.sql")
	c.Assert(filepath.Base(requests[0].Files[1].Path), qt.Equals, "feed.sql")
	c.Assert(db.Tables, qt.HasLen, 2)
}
