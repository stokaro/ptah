package chsource_test

import (
	"context"
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine"
)

func sourceRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{ID: "example.org/clickhouse", Targets: []engine.Target{{Name: "clickhouse"}},
		Codecs: chschema.Codecs(), Properties: []engine.PropertySource{{
			Target: "clickhouse", Format: schemaext.TablePlatformProperties, Definitions: chsource.Definitions(), Service: chsource.Service{},
		}},
	}))
}

func TestEverySettingPreservesAllIntentStates(t *testing.T) {
	for _, field := range reflect.VisibleFields(reflect.TypeFor[chschema.DesiredTable]()) {
		t.Run(field.Name, func(t *testing.T) {
			for _, setting := range []chschema.Setting{{}, {State: chschema.Default}, {State: chschema.Explicit, Value: "value"}} {
				c := qt.New(t)
				value := &chschema.DesiredTable{}
				reflect.ValueOf(value).Elem().FieldByIndex(field.Index).Set(reflect.ValueOf(setting))
				runtime := sourceRuntime()
				fragments, err := runtime.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{
					Target: "clickhouse", Format: schemaext.TablePlatformProperties, Values: []schemaext.Value{value},
				})
				c.Assert(err, qt.IsNil)
				c.Assert(fragments, qt.HasLen, 1)
				decoded, err := runtime.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{
					Target: "clickhouse", Format: schemaext.TablePlatformProperties, Fragments: fragments,
				})
				c.Assert(err, qt.IsNil)
				c.Assert(decoded, qt.DeepEquals, []schemaext.Value{value})
			}
		})
	}
}

func TestExplicitEmptyKeysStayDistinctFromOmittedAndDefault(t *testing.T) {
	c := qt.New(t)
	runtime := sourceRuntime()
	values := []schemaext.Value{
		&chschema.DesiredTable{},
		&chschema.DesiredTable{PrimaryKey: chschema.Setting{State: chschema.Default}},
		&chschema.DesiredTable{PrimaryKey: chschema.Setting{State: chschema.Explicit}},
	}
	fragments, err := runtime.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{Target: "clickhouse", Format: schemaext.TablePlatformProperties, Values: values})
	c.Assert(err, qt.IsNil)
	c.Assert(fragments, qt.DeepEquals, []schemaext.PropertyFragment{
		{Kind: chschema.TableKind, Properties: make(map[string]string)},
		{Kind: chschema.TableKind, Properties: map[string]string{"primary_key.state": "default"}},
		{Kind: chschema.TableKind, Properties: map[string]string{"primary_key": ""}},
	})
	decoded, err := runtime.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: "clickhouse", Format: schemaext.TablePlatformProperties, Fragments: fragments})
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, values)
}

func TestPropertyDecodingRefusesUnknownAndContradictoryInputAtomically(t *testing.T) {
	for _, properties := range []map[string]string{
		{"primary_key": "", "primary_key.state": "default"}, {"primary_key.state": "explicit"},
		{"primary_key.state": ""}, {"unknown": "value"}, {"engine": ""}, {"ttl": "bad\x00"},
	} {
		c := qt.New(t)
		values, err := (chsource.Service{}).DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{
			Target: "clickhouse", Format: schemaext.TablePlatformProperties, Fragments: []schemaext.PropertyFragment{
				{Kind: chschema.TableKind, Properties: map[string]string{"engine": "Memory"}}, {Kind: chschema.TableKind, Properties: properties},
			},
		})
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(values, qt.IsNil)
	}
}

func TestPropertyEncodingRefusesInvalidValuesAtomically(t *testing.T) {
	for _, value := range []schemaext.Value{(*chschema.DesiredTable)(nil), &chschema.ObservedTable{Engine: "Memory"}, &chschema.DesiredTable{TTL: chschema.Setting{State: "invalid"}}} {
		c := qt.New(t)
		fragments, err := (chsource.Service{}).EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{
			Target: "clickhouse", Format: schemaext.TablePlatformProperties, Values: []schemaext.Value{&chschema.DesiredTable{}, value},
		})
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(fragments, qt.IsNil)
	}
}

func TestPropertyServiceValidatesEmptyBatch(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, test := range []struct {
		ctx    context.Context
		target string
		format schemaext.PropertyFormat
		want   error
	}{
		{nil, "clickhouse", schemaext.TablePlatformProperties, schemaext.ErrInvalidValue},
		{ctx, "clickhouse", schemaext.TablePlatformProperties, context.Canceled},
		{t.Context(), "postgres", schemaext.TablePlatformProperties, ptaherr.ErrUnsupportedDialect},
		{t.Context(), "clickhouse", "example.org/unknown", ptaherr.ErrUnsupportedFeature},
	} {
		values, err := (chsource.Service{}).DecodeProperties(test.ctx, schemaext.PropertyDecodeRequest{Target: test.target, Format: test.format})
		c.Assert(err, qt.ErrorIs, test.want)
		c.Assert(values, qt.IsNil)
		fragments, err := (chsource.Service{}).EncodeProperties(test.ctx, schemaext.PropertyEncodeRequest{Target: test.target, Format: test.format})
		c.Assert(err, qt.ErrorIs, test.want)
		c.Assert(fragments, qt.IsNil)
	}
}
