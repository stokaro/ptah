package chast_test

import (
	"encoding/json"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func modifyRefresh() *chast.ModifyRefresh {
	return &chast.ModifyRefresh{Schedule: chschema.Schedule{
		Mode: chschema.RefreshEvery, Interval: "1 DAY", Offset: "2 HOUR", DependsOn: []string{"analytics.source"},
	}}
}

// The operation travels through the registry without the bundled runtime, and
// a decoded or cloned operation shares no dependency list with its source.
func TestModifyRefreshCodecPreservesTheSchedule(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(engine.New(engine.Provider{ID: "example.org/refresh", Codecs: chast.Codecs()})).Codecs()
	op := modifyRefresh()

	data, err := registry.Marshal(t.Context(), schemaext.Operation, []schemaext.Payload{op})
	c.Assert(err, qt.IsNil)
	decoded, err := registry.Unmarshal(t.Context(), data)

	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{op})
	decoded[0].(*chast.ModifyRefresh).Schedule.DependsOn[0] = "mutated"
	cloned, err := ast.CloneExtensionPayload(op)
	c.Assert(err, qt.IsNil)
	cloned.(*chast.ModifyRefresh).Schedule.DependsOn[0] = "mutated"
	c.Assert(op.Schedule.DependsOn, qt.DeepEquals, []string{"analytics.source"})
	c.Assert(op.Effect().Impact, qt.Equals, schemaext.Behavioral)
}

// The operation sets a schedule, so it carries exactly one, and one the server
// would accept.
func TestModifyRefreshCodecRefusesIncompleteAndInvalidSchedules(t *testing.T) {
	codecs := chast.Codecs()
	codec := codecs[slices.IndexFunc(codecs, func(candidate schemaext.Codec) bool { return candidate.Prototype.Kind() == chast.ModifyRefreshKind })]
	for _, data := range []string{
		`null`, `{}`, `{"schedule":null}`,
		`{"schedule":{"mode":"EVERY"}}`,
		`{"schedule":{"mode":"AFTER","interval":"1 HOUR","offset":"5 MINUTE"}}`,
		`{"schedule":{"mode":"EVERY","interval":"1 HOUR"},"extra":true}`,
	} {
		t.Run(data, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := codec.Decode(json.RawMessage(data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
	for _, payload := range []schemaext.Payload{(*chast.ModifyRefresh)(nil), &chast.ModifyRefresh{}, ttlOperation()} {
		c := qt.New(t)
		encoded, err := codec.Encode(payload)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(encoded, qt.IsNil)
	}
}
