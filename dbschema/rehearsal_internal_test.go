package dbschema

// White-box testing required: the connection's reader factory is private, so
// its selection cannot be controlled through the public connection API.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/sqlrunner"
)

type rehearsalReaderStub struct {
	raceReaderStub
	ctx    context.Context
	result *catalog.Database
	err    error
	calls  int
}

func (r *rehearsalReaderStub) ReadRehearsalSchemaContext(ctx context.Context) (*catalog.Database, error) {
	r.ctx = ctx
	r.calls++
	return r.result, r.err
}

func TestRehearsalReadUsesPrivateSelectedReader(t *testing.T) {
	c := qt.New(t)
	shared := &rehearsalReaderStub{}
	selected := &rehearsalReaderStub{result: &catalog.Database{Tables: []catalog.Table{{Name: "observed"}}}}
	conn := &DatabaseConnection{reader: shared, newReader: func(sqlrunner.Runner) catalog.SchemaReader { return selected }}
	got, err := ReadRehearsalSchemaContext(t.Context(), conn)
	c.Assert(err, qt.IsNil)
	c.Assert(got.Tables, qt.DeepEquals, selected.result.Tables)
	c.Assert(selected.calls, qt.Equals, 1)
	c.Assert(selected.ctx, qt.Equals, t.Context())
	c.Assert(shared.calls, qt.Equals, 0)
}

func TestRehearsalReadUsesOrdinaryReadWithoutEnvironmentCapability(t *testing.T) {
	c := qt.New(t)
	reader := &raceReaderStub{read: []string{"not read"}}
	conn := &DatabaseConnection{reader: reader}
	got, err := ReadRehearsalSchemaContext(t.Context(), conn)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.IsNotNil)
	c.Assert(reader.read, qt.IsNil)
}

func TestRehearsalReadDiscardsPartialEnvironmentOnError(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("environment read failed")
	reader := &rehearsalReaderStub{result: &catalog.Database{Tables: []catalog.Table{{Name: "partial"}}}, err: failure}
	got, err := ReadRehearsalSchemaContext(t.Context(), &DatabaseConnection{reader: reader})
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(got, qt.IsNil)
}
