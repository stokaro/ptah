package atlashclrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/pgindexstorage"
)

// indexedSchema is one table with one index carrying storage parameters.
func indexedSchema(params map[string]string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Doc", Name: "documents"}},
		Fields: []goschema.Field{
			{StructName: "Doc", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Doc", Name: "body", Type: "TEXT"},
		},
		Indexes: []goschema.Index{{
			StructName:    "Doc",
			Name:          "documents_body_idx",
			Fields:        []string{"body"},
			StorageParams: params,
		}},
	}
}

// A storage parameter written under the switch is read back as the same value.
//
// This is the property the whole change turns on. schemadiff treats a
// difference in the recorded set as a reason to rebuild the index, so a
// parameter the writer emits and the parser does not read back would make every
// such index differ from its own inspected document forever -- a permanent
// rebuild, and a silent one until somebody watches an index get rebuilt on
// every apply (stokaro/ptah#2183).
func TestIndexStorageParams_SurviveTheHCLRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
	}{
		{name: "the compatible one keeps its own attribute", params: map[string]string{"pages_per_range": "32"}},
		{name: "an HNSW pair", params: map[string]string{"m": "32", "ef_construction": "128"}},
		{name: "fillfactor", params: map[string]string{"fillfactor": "70"}},
		{name: "an IVFFlat list count", params: map[string]string{"lists": "250"}},
		{
			name:   "the compatible one beside two that are not",
			params: map[string]string{"pages_per_range": "32", "m": "32", "fillfactor": "70"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(pgindexstorage.EnvVar, "true")

			rendered, err := atlashclrender.Render(indexedSchema(test.params))
			c.Assert(err, qt.IsNil)

			parsed, err := atlashcl.Parse([]byte(string(rendered.Data)), "round-trip.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Indexes, qt.HasLen, 1)
			c.Assert(parsed.Indexes[0].StorageParams, qt.DeepEquals, test.params,
				qt.Commentf("document was:\n%s", string(rendered.Data)))
		})
	}
}

// With the switch off, a declaration carrying a parameter the document cannot
// hold is named rather than dropped in silence.
//
// The reader does not record these unless the switch is on, so reaching this
// path means the declaration carried them -- from a Go annotation or a SQL
// file, where they have always been expressible. Dropping them without a word
// is the failure this issue is about.
func TestIndexStorageParams_TheDefaultOmissionIsAnnounced(t *testing.T) {
	c := qt.New(t)
	t.Setenv(pgindexstorage.EnvVar, "false")

	rendered, err := atlashclrender.Render(indexedSchema(map[string]string{"m": "32", "fillfactor": "70"}))

	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Not(qt.Contains), "storage_params")
	warnings := diagnosticText(rendered)
	c.Assert(warnings, qt.Contains, "fillfactor")
	c.Assert(warnings, qt.Contains, "m")
	c.Assert(warnings, qt.Contains, pgindexstorage.EnvVar)
}

// The compatible parameter is never announced as omitted, because it is not.
//
// Without this control, a writer that warned about every parameter would pass
// the test above.
func TestIndexStorageParams_TheCompatibleOneIsNotAnnouncedAsLost(t *testing.T) {
	c := qt.New(t)
	t.Setenv(pgindexstorage.EnvVar, "false")

	rendered, err := atlashclrender.Render(indexedSchema(map[string]string{"pages_per_range": "32"}))

	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Contains, "page_per_range")
	c.Assert(diagnosticText(rendered), qt.Not(qt.Contains), "pages_per_range have no attribute")
}

// A document carrying storage_params keeps loading when the switch is off.
//
// The attribute is only WRITTEN under the switch; refusing to READ it without
// the switch would make a document produced yesterday fail to parse today
// because an environment variable changed.
func TestIndexStorageParams_ADocumentKeepsLoadingWhenTheSwitchIsOff(t *testing.T) {
	c := qt.New(t)
	t.Setenv(pgindexstorage.EnvVar, "true")
	rendered, err := atlashclrender.Render(indexedSchema(map[string]string{"m": "32"}))
	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Contains, "storage_params")

	t.Setenv(pgindexstorage.EnvVar, "false")
	parsed, err := atlashcl.Parse([]byte(string(rendered.Data)), "round-trip.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Indexes[0].StorageParams, qt.DeepEquals, map[string]string{"m": "32"})
}

// diagnosticText joins what the render reported, so a test can assert on the
// message a user reads rather than on the diagnostic struct.
func diagnosticText(result atlashclrender.Result) string {
	messages := make([]string, 0, len(result.Diagnostics))
	for _, diagnostic := range result.Diagnostics {
		messages = append(messages, diagnostic.Path+": "+diagnostic.Message)
	}
	return strings.Join(messages, "\n")
}
