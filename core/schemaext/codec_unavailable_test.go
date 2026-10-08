package schemaext_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestRegistryDistinguishesMissingCodecFromCallbackFailure(t *testing.T) {
	c := qt.New(t)
	codec := widgetCodec(widgetKind, schemaext.Desired)
	failure := fmt.Errorf("owner callback failed: %w", schemaext.ErrUnknownCodec)
	calls := 0
	codec.Clone = func(schemaext.Payload) (schemaext.Payload, error) {
		calls++
		return nil, failure
	}
	registry, err := schemaext.NewRegistry(owned(codec))
	c.Assert(err, qt.IsNil)

	values, err := registry.SnapshotValues(t.Context(), schemaext.Observed, []schemaext.Value{&widget{ID: widgetKind}})
	c.Assert(values, qt.IsNil)
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	var unavailable *schemaext.UnknownCodecError
	c.Assert(err, qt.ErrorAs, &unavailable)
	c.Assert(unavailable.Kind, qt.Equals, widgetKind)
	c.Assert(unavailable.Representation, qt.Equals, schemaext.Observed)
	c.Assert(calls, qt.Equals, 0)

	values, err = registry.SnapshotValues(t.Context(), schemaext.Desired, []schemaext.Value{&widget{ID: widgetKind}})
	c.Assert(values, qt.IsNil)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(err, qt.Not(qt.ErrorAs), &unavailable)
	c.Assert(calls, qt.Equals, 1)
}
