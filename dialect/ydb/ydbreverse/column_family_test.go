package ydbreverse_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
)

// familyReversalRuntime selects only this owner's column family service, so
// the engine's reply validation runs without the bundled runtime.
func familyReversalRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: ydbschema.Owner, Targets: []engine.Target{{Name: "ydb"}},
		Codecs: append(ydbschema.ColumnFamiliesCodecs(), ydbdiff.ColumnFamiliesCodec()),
		Reversals: []engine.Reversal{{Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.ColumnFamiliesKind},
			Service: ydbreverse.ColumnFamiliesService{}}},
	}))
}

func familyRecord(change *ydbdiff.ColumnFamilies) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).Table("docs"), Value: change}
}

// familyChange is a forward change from a table holding before, or found
// without families when before is nil, to after.
func familyChange(before, after []ydbschema.ColumnFamily) *ydbdiff.ColumnFamilies {
	change := &ydbdiff.ColumnFamilies{After: &ydbschema.DesiredColumnFamilies{Families: after}}
	if before != nil {
		change.Before = &ydbschema.ObservedColumnFamilies{Families: before}
	}
	return change
}

// TestReverseColumnFamilies_RestoresThePriorFamilies pins each reversal: the
// table goes back from what the forward change leaves to each column in the
// family it held and each setting it held, the forward state predicts what the
// forward change leaves, and what YQL cannot undo -- a family the forward
// change added, a setting it stated where the table held none -- is reported.
func TestReverseColumnFamilies_RestoresThePriorFamilies(t *testing.T) {
	tests := []struct {
		name            string
		before, after   []ydbschema.ColumnFamily
		wantAfter       []ydbschema.ColumnFamily
		wantLimitations []string
	}{
		{
			name:      "a moved column and a changed setting",
			before:    []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}, {Name: "cold", Data: "hdd", Compression: "off", Columns: []string{"body"}}},
			after:     []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}, {Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"blob", "body"}}},
			wantAfter: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "off", Columns: []string{"body"}}, {Name: "default", Compression: "off"}},
		},
		{
			name:      "a family the forward change added",
			before:    []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}},
			after:     []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}, {Name: "warm", CacheMode: "in_memory", Columns: []string{"blob"}}},
			wantAfter: []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}, {Name: "warm", CacheMode: "in_memory"}},
			wantLimitations: []string{"YQL drops no column family and resets no family setting, so the rollback keeps family warm, " +
				"which the forward change added."},
		},
		{
			name:      "families and a setting the table held none of",
			before:    []ydbschema.ColumnFamily{{Name: "default"}},
			after:     []ydbschema.ColumnFamily{{Name: "default", Data: "hdd"}, {Name: "cold", Columns: []string{"body"}}, {Name: "warm"}},
			wantAfter: []ydbschema.ColumnFamily{{Name: "cold"}, {Name: "default", Data: "hdd"}, {Name: "warm"}},
			wantLimitations: []string{"YQL drops no column family and resets no family setting, so the rollback keeps families cold, warm " +
				"and the DATA of family default, which the forward change added."},
		},
		{
			name:      "a setting of a default family the read did not list",
			before:    []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}},
			after:     []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}, {Name: "cold", Columns: []string{"body"}}},
			wantAfter: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}, {Name: "default", Compression: "lz4"}},
			wantLimitations: []string{"YQL drops no column family and resets no family setting, so the rollback keeps " +
				"the COMPRESSION of family default, which the forward change added."},
		},
		{
			name:      "families on a table the read found without them",
			after:     []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}},
			wantAfter: []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4"}},
			wantLimitations: []string{"YQL drops no column family and resets no family setting, so the rollback keeps family cold, " +
				"which the forward change added."},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := familyRecord(familyChange(test.before, test.after))

			result, err := familyReversalRuntime().ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "ydb", Changes: []schemaext.ChangeRecord{input}})

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			c.Assert(result[0].Change.Subject, qt.DeepEquals, input.Subject)
			reversed := result[0].Change.Value.(*ydbdiff.ColumnFamilies)
			c.Assert(reversed.Before, qt.DeepEquals, &ydbschema.ObservedColumnFamilies{Families: test.after})
			c.Assert(reversed.After.Families, qt.DeepEquals, test.wantAfter)
			c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement,
				Kind: ydbschema.ColumnFamiliesKind, Value: &ydbschema.ObservedColumnFamilies{Families: test.after}}})
			c.Assert(result[0].Strategy, qt.Equals, "move each column back to its family and restore each family setting in place")
			c.Assert(result[0].Limitations, qt.DeepEquals, test.wantLimitations)
		})
	}
}

func TestReverseColumnFamilies_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	tests := []struct {
		name    string
		ctx     context.Context
		request schemaext.ReversalRequest
		wantErr string
	}{
		{name: "canceled", ctx: canceled, request: schemaext.ReversalRequest{Target: "ydb"}, wantErr: "context canceled"},
		{name: "another target", ctx: t.Context(), request: schemaext.ReversalRequest{Target: "postgres"}, wantErr: `.*YDB column family reversal on "postgres".*`},
		{name: "another change", ctx: t.Context(), request: schemaext.ReversalRequest{Target: "ydb", Changes: []schemaext.ChangeRecord{
			{Subject: familyRecord(nil).Subject, Value: &ydbdiff.TTL{}},
		}}, wantErr: `.*reversal requires a YDB column family change`},
		{name: "a change without after", ctx: t.Context(), request: schemaext.ReversalRequest{Target: "ydb", Changes: []schemaext.ChangeRecord{
			familyRecord(&ydbdiff.ColumnFamilies{}),
		}}, wantErr: `.*requires the families the table ends up holding`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := ydbreverse.ColumnFamiliesService{}.ReverseChanges(test.ctx, test.request)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.IsNil)
		})
	}
}
