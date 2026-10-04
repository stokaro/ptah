package ydbgap_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbgap"
)

// Each refusal names YDB, the plan and the phase that implements the layer,
// which is what a reader needs to know whether to wait or to work around it.
func TestLayer_Message_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		layer ydbgap.Layer
		want  string
	}{
		{name: "schema files", layer: ydbgap.SchemaFiles, want: "reading a YDB schema file is not implemented yet (stokaro/ptah#4015, phase 10)"},
		{name: "comments", layer: ydbgap.Comments, want: "storing a comment on a YDB object is not implemented yet (stokaro/ptah#4015, phase 10)"},
		{name: "access control", layer: ydbgap.AccessControl, want: "managing YDB users, groups and permissions is not implemented yet (stokaro/ptah#4015, phase 10)"},
		{name: "table settings", layer: ydbgap.TableSettings, want: "setting YDB table options (partitioning, column families, changefeeds) is not implemented yet (stokaro/ptah#4015, phase 10)"},
		{name: "index families", layer: ydbgap.IndexFamilies, want: "reading or creating a YDB vector, full-text, JSON or column-table index is not implemented yet (stokaro/ptah#4015, phase 10)"},
		{name: "inference", layer: ydbgap.Inference, want: "running an embedding generation against YDB is not implemented yet (stokaro/ptah#4015, phase 12)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.layer.Message(), qt.Equals, test.want)
		})
	}
}

// A value outside the declared layers names no phase rather than borrowing
// another layer's.
func TestLayer_Message_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Assert(ydbgap.Layer(0).Phase(), qt.Equals, 0)
	c.Assert(ydbgap.Layer(0).Message(), qt.Equals, "this YDB operation is not implemented yet (stokaro/ptah#4015, phase 0)")
}

// Every declared layer names its phase and says what the YDB page lists for
// it, so the page's generated list cannot leave a layer out.
func TestLayers_EveryLayerNamesAPhaseAndAPageEntry(t *testing.T) {
	c := qt.New(t)
	layers := ydbgap.Layers()
	c.Assert(layers, qt.Not(qt.HasLen), 0)
	c.Assert(layers[0], qt.Equals, ydbgap.SchemaFiles)
	c.Assert(layers[len(layers)-1], qt.Equals, ydbgap.Inference)
	for _, layer := range layers {
		t.Run(layer.Message(), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(layer.Phase(), qt.Not(qt.Equals), 0)
			c.Assert(layer.Unsupported(), qt.Not(qt.Equals), "")
		})
	}
}

// A value outside the declared layers has no page entry.
func TestLayer_Unsupported_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbgap.Layer(0).Unsupported(), qt.Equals, "")
}

// The page's list carries one item per layer, in declaration order, as one
// sentence: items end with a semicolon and the last with a period.
func TestWriteUnsupportedMarkdown(t *testing.T) {
	c := qt.New(t)
	var out strings.Builder
	ydbgap.WriteUnsupportedMarkdown(&out)
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	c.Assert(lines, qt.HasLen, len(ydbgap.Layers()))
	c.Assert(lines[0], qt.Equals, "- "+ydbgap.SchemaFiles.Unsupported()+";")
	c.Assert(lines[len(lines)-1], qt.Equals, "- "+ydbgap.Inference.Unsupported()+".")
}
