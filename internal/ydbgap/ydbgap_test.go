package ydbgap_test

import (
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
		{name: "rendering", layer: ydbgap.Rendering, want: "rendering a YDB schema is not implemented yet (stokaro/ptah#4015, phase 3)"},
		{name: "planning", layer: ydbgap.Planning, want: "planning a YDB migration is not implemented yet (stokaro/ptah#4015, phase 3)"},
		{name: "schema files", layer: ydbgap.SchemaFiles, want: "reading a YDB schema file is not implemented yet (stokaro/ptah#4015, phase 2)"},
		{name: "connecting", layer: ydbgap.Connecting, want: "connecting to a YDB server is not implemented yet (stokaro/ptah#4015, phase 4)"},
		{name: "linting", layer: ydbgap.Linting, want: "linting YQL for YDB is not implemented yet (stokaro/ptah#4015, phase 8)"},
		{name: "creating databases", layer: ydbgap.CreatingDatabases, want: "creating a YDB database is not implemented yet (stokaro/ptah#4015, phase 9)"},
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
