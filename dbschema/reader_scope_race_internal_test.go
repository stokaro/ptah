package dbschema

// White-box testing required: the shared dialect reader and the factory that
// replaces it are unexported connection fields, and the scope a read runs under
// is not observable through the public API.

import (
	"context"
	"slices"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/sqlrunner"
)

// raceReaderStub records the scope each read ran under. Its fields are written
// and read without synchronization on purpose: that is exactly what the six
// dialect readers do with their own schema allow-list, so a reader shared
// between two concurrent reads is a data race the detector can see.
type raceReaderStub struct {
	schemas []string
	read    []string
}

func (r *raceReaderStub) SetSchemas(schemas []string) {
	r.schemas = slices.Clone(schemas)
}

func (r *raceReaderStub) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}

func (r *raceReaderStub) ReadSchemaContext(context.Context) (*catalog.Database, error) {
	r.read = slices.Clone(r.schemas)
	return &catalog.Database{}, nil
}

// TestReadSchemaWithSchemas_ConcurrentScopesDoNotShareAReader is the test the
// "thread-safe for read operations" claim in doc.go needs. Run under -race it
// fails on a build where ReadSchemaWithSchemas scopes the connection's shared
// reader, because both calls write and read the same allow-list fields.
func TestReadSchemaWithSchemas_ConcurrentScopesDoNotShareAReader(t *testing.T) {
	c := qt.New(t)

	conn := &DatabaseConnection{
		reader: &raceReaderStub{},
		newReader: func(sqlrunner.Runner) catalog.SchemaReader {
			return &raceReaderStub{}
		},
	}

	scopes := [][]string{{"auth"}, {"billing"}}
	var wg sync.WaitGroup
	errs := make([]error, len(scopes))
	for i, scope := range scopes {
		wg.Go(func() {
			_, errs[i] = ReadSchemaWithSchemasContext(t.Context(), conn, scope)
		})
	}
	wg.Wait()

	for _, err := range errs {
		c.Assert(err, qt.IsNil)
	}
}
