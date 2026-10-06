// Package ydbgap names unimplemented YDB workflows and shares their refusal
// messages with the native CLI, agent surface, and generated documentation.
// Schema and migration support is implemented. The inference lifecycle still
// needs a YDB backend and is tracked separately in stokaro/ptah#4181.
package ydbgap

import (
	"fmt"
	"io"
)

// Layer is a workflow that YDB does not reach yet.
type Layer int

const (
	// Inference is an embedding generation on YDB. Its durable run state and
	// vectors need a YDB backend behind the shared inference engine boundary.
	Inference Layer = iota + 1

	// endOfLayers keeps Layers derived from the declarations.
	endOfLayers
)

// Layers returns every unimplemented workflow, in declaration order.
func Layers() []Layer {
	layers := make([]Layer, 0, int(endOfLayers)-int(Inference))
	for layer := Inference; layer < endOfLayers; layer++ {
		layers = append(layers, layer)
	}
	return layers
}

// Message names the unimplemented workflow and its tracking issue. An unknown
// value returns a generic refusal without borrowing another workflow's issue.
func (l Layer) Message() string {
	if l == Inference {
		return "running an embedding generation against YDB is not implemented yet (stokaro/ptah#4181)"
	}
	return "this YDB operation is not implemented yet"
}

// Unsupported describes the workflow for the YDB page. It is empty for an
// unknown layer.
func (l Layer) Unsupported() string {
	if l == Inference {
		return "`ptah inference` and the inference tools of `ptah mcp`, which store their vectors through pgvector"
	}
	return ""
}

// WriteUnsupportedMarkdown writes one item per unimplemented workflow for
// the YDB page's generated block, ending the last item with a period.
func WriteUnsupportedMarkdown(w io.Writer) {
	layers := Layers()
	for i, layer := range layers {
		end := ";"
		if i == len(layers)-1 {
			end = "."
		}
		fmt.Fprintf(w, "- %s%s\n", layer.Unsupported(), end)
	}
}
