package schemasource

// White-box testing required: parseOutput forwards the target to a format
// reader. Calling Run crosses a process boundary; this test isolates that
// deterministic dispatch from program execution.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

type hclDialectReader struct {
	Formats
	dialect string
}

func (r *hclDialectReader) ReadHCL(_ context.Context, _ []byte, _, dialect string) (*schemamodel.Database, error) {
	r.dialect = dialect
	return &schemamodel.Database{}, nil
}

func TestParseOutput_PassesTheHCLDialect(t *testing.T) {
	c := qt.New(t)
	reader := &hclDialectReader{}
	db, err := parseOutput(t.Context(), reader, []byte(`shape "s" {}`), "hcl", "oracle")
	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.IsNotNil)
	c.Assert(reader.dialect, qt.Equals, "oracle")
}
