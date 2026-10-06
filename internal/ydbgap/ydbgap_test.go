package ydbgap_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbgap"
)

func TestLayer_Message(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbgap.Inference.Message(), qt.Equals,
		"running an embedding generation against YDB is not implemented yet (stokaro/ptah#4181)",
	)
	c.Assert(ydbgap.Layer(0).Message(), qt.Equals, "this YDB operation is not implemented yet")
	c.Assert(ydbgap.Layer(0).Unsupported(), qt.Equals, "")
}

// Completed schema and migration layers must not remain in the generated gap list.
func TestLayers_OnlyDeferredInferenceRemains(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbgap.Layers(), qt.DeepEquals, []ydbgap.Layer{ydbgap.Inference})
	c.Assert(ydbgap.Inference.Unsupported(), qt.Equals,
		"`ptah inference` and the inference tools of `ptah mcp`, which store their vectors through pgvector",
	)
}

func TestWriteUnsupportedMarkdown(t *testing.T) {
	c := qt.New(t)
	var out strings.Builder
	ydbgap.WriteUnsupportedMarkdown(&out)
	c.Assert(out.String(), qt.Equals, "- "+ydbgap.Inference.Unsupported()+".\n")
}
