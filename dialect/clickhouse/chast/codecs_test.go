package chast_test

import (
	"encoding/json"
	"regexp"
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

func ttlOperation() *chast.AlterStorage {
	before := &chschema.ObservedTable{Engine: "MergeTree", TTL: "created_at + toIntervalDay(7)"}
	after := before.Desired()
	after.TTL.Value = ""
	return &chast.AlterStorage{Change: chdiff.Table{Before: before, After: after}}
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
	decoded[0].(*chast.AlterStorage).Change.Before.TTL = "mutated"
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

// keyOperation appends k to the sorting key and adds k and a column with a
// default in the same operation.
func keyOperation() *chast.AlterStorage {
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a", PrimaryKey: "a"}
	after := before.Desired()
	after.OrderBy.Value = "a, k"
	return &chast.AlterStorage{Change: chdiff.Table{Before: before, After: after}, Columns: []chast.Column{
		{Name: "k", Type: "Int32", Comment: "key"},
		{Name: "note", Type: "String", Value: "DEFAULT", Expression: "'n'"},
	}}
}

func TestStorageCodecPreservesAddedColumns(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(engine.New(engine.Provider{ID: "example.org/storage", Codecs: chast.Codecs()})).Codecs()
	operation := keyOperation()
	data, err := registry.Marshal(t.Context(), schemaext.Operation, []schemaext.Payload{operation})
	c.Assert(err, qt.IsNil)
	decoded, err := registry.Unmarshal(t.Context(), data)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{operation})
	decoded[0].(*chast.AlterStorage).Columns[0].Name = "mutated"
	c.Assert(operation.Columns[0].Name, qt.Equals, "k")
}

func TestStorageCodecRefusesColumnsTheKeyDoesNotNeed(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*chast.AlterStorage)
		want   string
	}{
		{name: "no new key expression", change: func(op *chast.AlterStorage) {
			op.Change.After.OrderBy.Value = "a"
			op.Change.After.TTL.Value = "toDateTime(a)"
		},
			want: "adds columns only with a new sorting-key expression"},
		{name: "a value on a key column", change: func(op *chast.AlterStorage) { op.Columns[0].Value, op.Columns[0].Expression = "MATERIALIZED", "a + 1" },
			want: "refuses a MATERIALIZED value"},
		{name: "an unknown value kind", change: func(op *chast.AlterStorage) { op.Columns[1].Value = "EPHEMERAL" }, want: `value kind "EPHEMERAL"`},
		{name: "an expression without a kind", change: func(op *chast.AlterStorage) { op.Columns[1].Value = "" }, want: "an expression without DEFAULT"},
		{name: "a column without a type", change: func(op *chast.AlterStorage) { op.Columns[1].Type = "" }, want: "requires a name and a type"},
		{name: "a column twice", change: func(op *chast.AlterStorage) { op.Columns[1] = op.Columns[0] }, want: "adds column k twice"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			op := keyOperation()
			test.change(op)
			encoded, err := chast.Codecs()[0].Encode(op)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want)+".*")
			c.Assert(encoded, qt.IsNil)
			decoded, err := chast.Codecs()[0].Decode(must.Must(json.Marshal(op)))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

func TestStorageCodecRefusesMalformedColumnLists(t *testing.T) {
	change := string(must.Must(json.Marshal(keyOperation().Change)))
	for _, data := range []string{
		`{"change":` + change + `,"columns":[]}`,
		`{"change":` + change + `,"columns":null}`,
		`{"change":` + change + `,"columns":[{"name":"k","type":"Int32","codec":"ZSTD"}]}`,
	} {
		t.Run(data, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := chast.Codecs()[0].Decode(json.RawMessage(data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}
