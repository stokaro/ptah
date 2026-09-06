package dbschema

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

type scopedReaderStub struct {
	scopes [][]string
}

func (r *scopedReaderStub) SetSchemas(schemas []string) {
	r.scopes = append(r.scopes, slices.Clone(schemas))
}

func (r *scopedReaderStub) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}

func (r *scopedReaderStub) ReadSchemaContext(context.Context) (*catalog.Database, error) {
	return &catalog.Database{}, nil
}

// TestReadSchemaWithSchemas_ResetsScopedReader covers the fallback path: a
// connection assembled without a reader factory scopes the reader it was given
// and puts the scope back. Every connection ConnectToDatabase returns carries a
// factory and takes the other branch, where the scope goes on a reader nothing
// else can see -- see
// TestReadSchemaWithSchemas_ConcurrentScopesDoNotShareAReader.
func TestReadSchemaWithSchemas_ResetsScopedReader(t *testing.T) {
	c := qt.New(t)

	reader := &scopedReaderStub{}
	conn := &DatabaseConnection{reader: reader}

	schema, err := ReadSchemaWithSchemasContext(t.Context(), conn, []string{"auth", "billing"})

	c.Assert(err, qt.IsNil)
	c.Assert(schema, qt.IsNotNil)
	c.Assert(reader.scopes, qt.DeepEquals, [][]string{
		{"auth", "billing"},
		nil,
	})
}
