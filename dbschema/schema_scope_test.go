package dbschema

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	dbschematypes "github.com/stokaro/ptah/dbschema/types"
)

type scopedReaderStub struct {
	scopes [][]string
}

func (r *scopedReaderStub) SetSchemas(schemas []string) {
	r.scopes = append(r.scopes, slices.Clone(schemas))
}

func (r *scopedReaderStub) ReadSchema() (*dbschematypes.DBSchema, error) {
	return &dbschematypes.DBSchema{}, nil
}

func TestReadSchemaWithSchemas_ResetsScopedReader(t *testing.T) {
	c := qt.New(t)

	reader := &scopedReaderStub{}
	conn := &DatabaseConnection{reader: reader}

	schema, err := ReadSchemaWithSchemas(conn, []string{"auth", "billing"})

	c.Assert(err, qt.IsNil)
	c.Assert(schema, qt.IsNotNil)
	c.Assert(reader.scopes, qt.DeepEquals, [][]string{
		{"auth", "billing"},
		nil,
	})
}
