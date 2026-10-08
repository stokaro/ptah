package chast_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func ttlOperation() *chast.AlterTTL {
	before := &chschema.ObservedTable{Engine: "MergeTree", TTL: "created_at + toIntervalDay(7)"}
	after := before.Desired()
	after.TTL.Value = ""
	return &chast.AlterTTL{Change: chdiff.Table{Before: before, After: after}}
}

func TestTTLCodecPreservesCapturedOperandsAndExplicitRemoval(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(engine.New(engine.Provider{ID: "example.org/storage", Codecs: chast.Codecs()})).Codecs()
	operation := ttlOperation()
	data, err := registry.Marshal(t.Context(), schemaext.Operation, []schemaext.Payload{operation})
	c.Assert(err, qt.IsNil)
	decoded, err := registry.Unmarshal(t.Context(), data)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{operation})
	decoded[0].(*chast.AlterTTL).Change.Before.TTL = "mutated"
	c.Assert(operation.Change.Before.TTL, qt.Equals, "created_at + toIntervalDay(7)")
	c.Assert(operation.Change.After.TTL, qt.DeepEquals, chschema.Setting{State: chschema.Explicit})
}

func TestTTLCodecRefusesIncompleteOrDuplicateWireFields(t *testing.T) {
	codec := chast.Codecs()[0]
	for _, data := range []string{`null`, `{}`, `{"change":null}`, `{"change":{}}`, `{"change":{},"change":{}}`, `{"change":{},"unknown":true}`} {
		t.Run(data, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := codec.Decode(json.RawMessage(data))
			c.Assert(err, qt.IsNotNil)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

func TestTTLCodecCannotAcknowledgeAnUnrelatedStorageChange(t *testing.T) {
	c := qt.New(t)
	op := ttlOperation()
	op.Change.After.Engine.Value = "ReplacingMergeTree"
	data := must.Must(json.Marshal(op))
	decoded, err := chast.Codecs()[0].Decode(data)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(decoded, qt.IsNil)
}

func TestTTLCodecRefusesWhitespaceRules(t *testing.T) {
	for _, test := range []struct {
		name   string
		before string
		after  string
	}{
		{name: "before", before: " \t\n", after: "created_at + INTERVAL 7 DAY"},
		{name: "after", before: "created_at + INTERVAL 7 DAY", after: " \t\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			op := ttlOperation()
			op.Change.Before.TTL, op.Change.After.TTL.Value = test.before, test.after
			codec := chast.Codecs()[0]
			encoded, err := codec.Encode(op)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(encoded, qt.IsNil)
			decoded, err := codec.Decode(must.Must(json.Marshal(op)))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}
