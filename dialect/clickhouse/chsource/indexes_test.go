package chsource_test

import (
	"context"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine"
)

func indexSourceRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{ID: "example.org/clickhouse", Targets: []engine.Target{{Name: "clickhouse"}},
		Codecs: chschema.IndexCodecs(), Properties: []engine.PropertySource{{
			Target: "clickhouse", Format: schemaext.IndexPlatformProperties, Definitions: chsource.IndexDefinitions(), Service: chsource.IndexService{},
		}},
	}))
}

func TestIndexPropertiesPreserveSettingIntentAndPrecision(t *testing.T) {
	for _, test := range []struct {
		name       string
		properties map[string]string
		want       *chschema.DesiredIndex
	}{
		{"unmanaged", make(map[string]string), &chschema.DesiredIndex{}},
		{"default type", map[string]string{"type.state": "default"}, &chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Default}}},
		{"default granularity", map[string]string{"granularity.state": "default"}, &chschema.DesiredIndex{Granularity: chschema.GranularitySetting{State: chschema.Default}}},
		{"explicit type", map[string]string{"type": "bloom_filter(0.01)"}, &chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Explicit, Value: "bloom_filter(0.01)"}}},
		{"maximum granularity", map[string]string{"type": "set(100)", "granularity": "18446744073709551615"},
			(&chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}).Desired()},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := indexSourceRuntime()
			fragments := []schemaext.PropertyFragment{{Kind: chschema.IndexKind, Properties: test.properties}}
			values, err := runtime.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{
				Target: "clickhouse", Format: schemaext.IndexPlatformProperties, Fragments: fragments,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.DeepEquals, []schemaext.Value{test.want})
			encoded, err := runtime.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{
				Target: "clickhouse", Format: schemaext.IndexPlatformProperties, Values: values,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(encoded, qt.DeepEquals, fragments)
			encoded[0].Properties["type"] = "changed"
			c.Assert(fragments[0].Properties["type"], qt.Not(qt.Equals), "changed")
		})
	}
}

func TestIndexPropertyDecodingRejectsInvalidBatchAtomically(t *testing.T) {
	for _, test := range []struct {
		name       string
		properties map[string]string
	}{
		{"empty type", map[string]string{"type": ""}},
		{"NUL in type", map[string]string{"type": "minmax\x00"}},
		{"type and state", map[string]string{"type": "minmax", "type.state": "default"}},
		{"granularity and state", map[string]string{"granularity": "8", "granularity.state": "default"}},
		{"unsupported state", map[string]string{"type.state": "explicit"}},
		{"empty state", map[string]string{"granularity.state": ""}},
		{"unknown key", map[string]string{"granularities": "8"}},
		{"empty granularity", map[string]string{"granularity": ""}},
		{"zero granularity", map[string]string{"granularity": "0"}},
		{"negative granularity", map[string]string{"granularity": "-1"}},
		{"fraction", map[string]string{"granularity": "1.5"}},
		{"overflow", map[string]string{"granularity": "18446744073709551616"}},
		{"hexadecimal", map[string]string{"granularity": "0x10"}},
		{"exponent", map[string]string{"granularity": "1e3"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := (chsource.IndexService{}).DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{
				Target: "clickhouse", Format: schemaext.IndexPlatformProperties, Fragments: []schemaext.PropertyFragment{
					{Kind: chschema.IndexKind, Properties: map[string]string{"type": "minmax", "granularity": "1"}},
					{Kind: chschema.IndexKind, Properties: test.properties},
				},
			})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(values, qt.IsNil)
		})
	}
}

func TestIndexPropertyEncodingRejectsInvalidBatchAtomically(t *testing.T) {
	for _, test := range []struct {
		name  string
		value schemaext.Value
	}{
		{"typed nil", (*chschema.DesiredIndex)(nil)},
		{"observed representation", &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}},
		{"table model", &chschema.DesiredTable{}},
		{"zero explicit granularity", &chschema.DesiredIndex{Granularity: chschema.GranularitySetting{State: chschema.Explicit}}},
		{"value without explicit intent", &chschema.DesiredIndex{Granularity: chschema.GranularitySetting{State: chschema.Default, Value: 8}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fragments, err := (chsource.IndexService{}).EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{
				Target: "clickhouse", Format: schemaext.IndexPlatformProperties, Values: []schemaext.Value{&chschema.DesiredIndex{}, test.value},
			})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(fragments, qt.IsNil)
		})
	}
}

func TestIndexSourceChecksEmptyRequests(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, test := range []struct {
		ctx    context.Context
		target string
		format schemaext.PropertyFormat
		want   error
	}{
		{nil, "clickhouse", schemaext.IndexPlatformProperties, schemaext.ErrInvalidValue},
		{ctx, "clickhouse", schemaext.IndexPlatformProperties, context.Canceled},
		{t.Context(), "postgres", schemaext.IndexPlatformProperties, ptaherr.ErrUnsupportedDialect},
		{t.Context(), "clickhouse", schemaext.TablePlatformProperties, ptaherr.ErrUnsupportedFeature},
	} {
		values, err := (chsource.IndexService{}).DecodeProperties(test.ctx, schemaext.PropertyDecodeRequest{Target: test.target, Format: test.format})
		c.Assert(err, qt.ErrorIs, test.want)
		c.Assert(values, qt.IsNil)
		fragments, err := (chsource.IndexService{}).EncodeProperties(test.ctx, schemaext.PropertyEncodeRequest{Target: test.target, Format: test.format})
		c.Assert(err, qt.ErrorIs, test.want)
		c.Assert(fragments, qt.IsNil)
	}
}
