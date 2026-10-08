package schemaext_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestSnapshotPayloadsClonesThroughLocalCodecWithoutEncoding(t *testing.T) {
	c := qt.New(t)
	codec := widgetCodec(widgetKind, schemaext.Operation)
	encodes := 0
	codec.Encode = func(schemaext.Payload) (json.RawMessage, error) {
		encodes++
		return nil, errors.New("encoder must not run")
	}
	codec.Canonical = codec.Encode
	registry, err := schemaext.NewRegistry(owned(codec))
	c.Assert(err, qt.IsNil)
	input := &widget{ID: widgetKind, Names: []string{"original"}}
	result, err := registry.SnapshotPayloads(t.Context(), schemaext.Operation, []schemaext.Payload{input})
	c.Assert(err, qt.IsNil)
	c.Assert(encodes, qt.Equals, 0)
	input.Names[0] = "changed"
	c.Assert(result[0].(*widget).Names, qt.DeepEquals, []string{"original"})
}

func TestSnapshotPayloadsRefusesWholeBatch(t *testing.T) {
	for _, test := range []struct {
		name  string
		value schemaext.Payload
		want  error
	}{
		{"nil", nil, schemaext.ErrInvalidValue},
		{"typed nil", (*widget)(nil), schemaext.ErrInvalidValue},
		{"unknown kind", &widget{ID: otherKind}, schemaext.ErrUnknownCodec},
		{"wrong type", &otherWidget{ID: widgetKind}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Operation)))
			c.Assert(err, qt.IsNil)
			result, err := registry.SnapshotPayloads(t.Context(), schemaext.Operation, []schemaext.Payload{&widget{ID: widgetKind}, test.value})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestSnapshotPayloadsChecksEmptyBatchAndCancellation(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry()
	c.Assert(err, qt.IsNil)
	result, err := registry.SnapshotPayloads(t.Context(), "unknown", nil)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
	c.Assert(result, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	codec := widgetCodec(widgetKind, schemaext.Operation)
	codec.Clone = func(value schemaext.Payload) (schemaext.Payload, error) {
		cancel()
		return value.(*widget).Clone(), nil
	}
	registry, err = schemaext.NewRegistry(owned(codec))
	c.Assert(err, qt.IsNil)
	result, err = registry.SnapshotPayloads(ctx, schemaext.Operation, []schemaext.Payload{&widget{ID: widgetKind}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
}
