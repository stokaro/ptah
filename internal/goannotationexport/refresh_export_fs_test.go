package goannotationexport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/goannotationexport"
)

func TestExportPreservesRefreshInHCL(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "model.go")
	output := filepath.Join(root, "schema.hcl")
	c.Assert(os.WriteFile(source, []byte(`package models

//ptah:schema:matview name="events" body="SELECT 1 AS id" refresh="EVERY 1 DAY OFFSET 120 MINUTE RANDOMIZE FOR 1800 SECOND APPEND"
type Events struct{}
`), 0o600), qt.IsNil)
	result, err := goannotationexport.Export(t.Context(), goannotationexport.Options{
		HCL: builtintest.Runtime().HCL(), Annotations: builtintest.Annotations(), RootDir: root, OutputPath: output, Cleanup: false,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Path, qt.Equals, "materialized_views.events")
	c.Assert(result.Diagnostics[0].Message, qt.Contains, "raw SQL body")
	c.Assert(result.Cleanup, qt.HasLen, 1)
	c.Assert(result.Cleanup[0].RemovedLines, qt.Equals, 1)
	data := must.Must(os.ReadFile(output))
	parsed, err := atlashcl.ParseWithOptions(t.Context(), data, output, atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.MaterializedViews, qt.HasLen, 1)
	c.Assert(parsed.MaterializedViews[0].Facets, qt.DeepEquals, must.Must(chsource.RefreshFacets("EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 30 MINUTE APPEND")))
	c.Assert(string(must.Must(os.ReadFile(source))), qt.Contains, `refresh="EVERY 1 DAY OFFSET 120 MINUTE RANDOMIZE FOR 1800 SECOND APPEND"`)
}
