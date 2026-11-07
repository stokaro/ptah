package schemaload_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"oras.land/oras-go/v2/content/memory"

	"ptah.run/core/coverage"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/ociartifact"
	"ptah.run/internal/schemaartifact"
	"ptah.run/internal/schemaload"
)

const (
	regionsSchema = `schema "public" {}
table "regions" {
  schema = schema.public
  column "code" {
    null = false
    type = text
  }
  primary_key {
    columns = [column.code]
  }
}
`
	regionsDataBlock = `data {
  table = table.regions
  keys  = ["code"]
  file  = "regions.yaml"
}
`
)

// publishLayers tags an artifact in store holding exactly these layers, with no
// parse in front of it, the way a newer or a hostile publisher would.
func publishLayers(c *qt.C, store *memory.Store, schema, rows string) {
	c.Helper()
	files := fstest.MapFS{schemaartifact.FileName: {Data: []byte(schema)}}
	if rows != "" {
		files[schemaartifact.ManagedDataFileName] = &fstest.MapFile{Data: []byte(rows)}
	}
	_, err := ociartifact.PushTo(c.Context(), store, files, ociartifact.PushOptions{
		ArtifactType:   ociartifact.SchemaArtifactType,
		LayerMediaType: schemaartifact.LayerMediaType,
		LayerMediaTypes: map[string]string{
			schemaartifact.ManagedDataFileName: schemaartifact.ManagedDataLayerMediaType,
		},
		Annotations: map[string]string{"io.stokaro.ptah.schema-format": "hcl"},
		Tags:        []string{"v1"},
	})
	c.Assert(err, qt.IsNil)
}

// materializeUnparsed is what `ptah schema pull --skip-parse` does with the
// artifact in store. It returns the path of the schema.
func materializeUnparsed(c *qt.C, store *memory.Store) string {
	c.Helper()
	pulled, err := schemaartifact.PullUnparsedFrom(c.Context(), store, "v1")
	c.Assert(err, qt.IsNil)
	written, err := schemaartifact.MaterializeUnparsed(pulled, filepath.Join(c.TempDir(), "schema.hcl"))
	c.Assert(err, qt.IsNil)
	return written[0]
}

// What a default pull refuses by reading the content, the next command refuses
// after an unparsed pull, in the same words. Each row runs both, so the refusal
// moved rather than disappeared, and a reader that stops refusing one of these
// turns the flag into a way past it.
func TestLoad_FailurePath_RefusesWhatADefaultPullRefuses(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		rows    string
		wantErr string
	}{
		{
			name:    "a schema this build cannot parse",
			schema:  "table \"t\" {\n  column \"id\" { type = bigint \"a second value\" }\n}\n",
			wantErr: `.*parse HCL schema: .*schema\.hcl:2,.*Invalid single-argument block definition.*`,
		},
		{
			name:    "a data block and no row layer",
			schema:  regionsSchema + regionsDataBlock,
			wantErr: `.*schema artifact declares managed data for table public\.regions and carries no rows for it`,
		},
		{
			name:    "a row layer for another table",
			schema:  regionsSchema + regionsDataBlock,
			rows:    `{"sets":[{"schema":"public","table":"countries","keys":["code"],"columns":["code"],"rows":[]}]}` + "\n",
			wantErr: `.*schema artifact declares managed data for table public\.regions and carries no rows for it`,
		},
		{
			name:    "a row layer whose keys disagree",
			schema:  regionsSchema + regionsDataBlock,
			rows:    `{"sets":[{"schema":"public","table":"regions","keys":["name"],"columns":["name"],"rows":[]}]}` + "\n",
			wantErr: `.*managed data layer keys \[name\] for table public\.regions do not match the declared keys \[code\]`,
		},
		{
			name:    "a row layer that also carries a table no block declares",
			schema:  regionsSchema + regionsDataBlock,
			rows:    `{"sets":[{"schema":"public","table":"regions","keys":["code"],"columns":["code"],"rows":[]},{"schema":"public","table":"countries","keys":["code"],"columns":["code"],"rows":[]}]}` + "\n",
			wantErr: `.*managed data layer carries rows for table public\.countries, which no data block declares`,
		},
		{
			name:    "a row layer that is not JSON",
			schema:  regionsSchema + regionsDataBlock,
			rows:    "regions: yes\n",
			wantErr: `.*parse managed data layer: .*`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			store := memory.New()
			publishLayers(c, store, tt.schema, tt.rows)

			_, defaultErr := schemaartifact.PullFrom(c.Context(), store, "v1", coverage.Vocabulary{}, builtintest.Runtime().HCL())
			_, nextErr := schemaload.Load(schemaload.Options{Owners: builtintest.Runtime(), SchemaFiles: []string{materializeUnparsed(c, store)}})

			c.Assert(defaultErr, qt.ErrorMatches, tt.wantErr)
			c.Assert(nextErr, qt.ErrorMatches, tt.wantErr)
		})
	}
}

// An artifact with no rows reads as it does after a default pull: the layer that
// states it carries none is not read, because no `data` block asks.
func TestLoad_AnUnparsedPullOfASchemaWithoutRowsLoads(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	publishLayers(c, store, regionsSchema, "")
	path := materializeUnparsed(c, store)
	_, err := os.Stat(filepath.Join(filepath.Dir(path), schemaartifact.ManagedDataFileName))
	c.Assert(err, qt.IsNil)

	db, err := schemaload.Load(schemaload.Options{Owners: builtintest.Runtime(), SchemaFiles: []string{path}})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "regions")
	c.Assert(db.ManagedData, qt.HasLen, 0)
}

// An artifact's rows arrive through its layer after an unparsed pull too, so the
// flag changes when an artifact is read and nothing about what it says.
func TestLoad_AnUnparsedPullCarriesTheDeclaredRows(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	publishLayers(c, store, regionsSchema+regionsDataBlock,
		`{"sets":[{"schema":"public","table":"regions","keys":["code"],"columns":["code"],"rows":[{"code":{"tag":"str","text":"CZ"}}]}]}`+"\n")

	db, err := schemaload.Load(schemaload.Options{Owners: builtintest.Runtime(), SchemaFiles: []string{materializeUnparsed(c, store)}})

	c.Assert(err, qt.IsNil)
	c.Assert(db.ManagedData, qt.HasLen, 1)
	c.Assert(db.ManagedData[0].Rows, qt.HasLen, 1)
	c.Assert(db.ManagedData[0].Rows[0]["code"].Text, qt.Equals, "CZ")
}
