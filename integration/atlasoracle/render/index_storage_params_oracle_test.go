//go:build integration

package render_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestOracleKeepsTheIndexStorageParameterPtahRenders measures the one thing an
// exit status cannot see here.
//
// The pinned community binary v1.3.0 ACCEPTS both spellings of this attribute at
// exit 0. It honors only one. Measured on PostgreSQL 17.10 with two documents
// differing in that single token and nothing else:
//
//	page_per_range  = 32  ->  CREATE INDEX "i" ON "t" USING brin ("ts") WITH (pages_per_range = 32)
//	pages_per_range = 32  ->  CREATE INDEX "i" ON "t" USING brin ("ts")
//
// So a run that only asked whether the binary read the document would pass on
// the spelling that throws the parameter away. This run asks what the binary
// came back with, and the second row is the control that proves the first is
// measuring the spelling rather than the presence of a BRIN index.
//
// It matters because #1242 made `schema inspect` of a live database emit this
// attribute for the first time: before it, no inspected document carried one and
// the spelling could not be wrong in a way anyone reached.
func TestOracleKeepsTheIndexStorageParameterPtahRenders(t *testing.T) {
	oracle := requireTypeOracle(t)
	devURL := requireDevURL(t, platform.Postgres)
	schema := schemaNameByDialect[platform.Postgres]

	result, err := atlashclrender.RenderInspectedForAtlasCLI(brinStorageParamDocument(schema), platform.Postgres, schema)
	c := qt.New(t)
	c.Assert(err, qt.IsNil)
	rendered := string(result.Data)
	c.Assert(rendered, qt.Contains, "page_per_range = 32",
		qt.Commentf("the document no longer carries the spelling this run measures:\n%s", rendered))

	t.Run("rendered", func(t *testing.T) {
		c := qt.New(t)

		out, code := runReferenceOracle(c.TB, oracle, devURL, rendered)

		c.Assert(code, qt.Equals, 0,
			qt.Commentf("the binary refuses the document ptah-compat renders: %s\n%s", out, rendered))
		c.Assert(out, qt.Contains, "page_per_range",
			qt.Commentf("the binary read the document and came back without the storage parameter,"+
				" so the index it would build is not the index that was inspected: %s", out))
	})

	t.Run("plural spelling is dropped in silence", func(t *testing.T) {
		c := qt.New(t)

		mutated := strings.Replace(rendered, "page_per_range = 32", "pages_per_range = 32", 1)
		c.Assert(mutated, qt.Not(qt.Equals), rendered,
			qt.Commentf("substituting the attribute changed nothing, so this row measures nothing"))

		out, code := runReferenceOracle(c.TB, oracle, devURL, mutated)

		c.Assert(code, qt.Equals, 0,
			qt.Commentf("the plural spelling is now refused rather than ignored,"+
				" which would make the singular one merely a preference: %s", out))
		c.Assert(out, qt.Not(qt.Contains), "page_per_range",
			qt.Commentf("the binary now honors the plural spelling too,"+
				" so the rule this run guards can go: %s", out))
	})
}

// brinStorageParamDocument is one BRIN index carrying the only index storage
// parameter every surface in the chain can write.
func brinStorageParamDocument(schema string) *goschema.Database {
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t", Schema: schema}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "integer", Primary: true},
			{StructName: "T", Name: "ts", Type: "timestamptz", Nullable: true},
		},
		Indexes: []goschema.Index{{
			StructName:    "T",
			Name:          "i",
			Fields:        []string{"ts"},
			Type:          "brin",
			StorageParams: map[string]string{"pages_per_range": "32"},
		}},
	}
	goschema.Finalize(db)
	return db
}
