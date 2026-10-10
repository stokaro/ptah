package ydbconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/engine"
)

// TestExternalConversion_KeepsEverySetting converts declarations of both kinds
// into the observations a read would report once they are applied, without
// the holder and in input order, and those observations back into
// declarations that keep the objects.
func TestExternalConversion_KeepsEverySetting(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: ydbexternal.Codecs(),
		Conversions: []engine.Conversion{{Target: "ydb", Kinds: []schemaext.Kind{ydbexternal.SourceKind, ydbexternal.TableKind}, Service: ydbconvert.ExternalService{}}}})
	c.Assert(err, qt.IsNil)
	source := ydbexternal.DataSource{SourceType: "PostgreSQL", AuthMethod: "BASIC", Options: map[string]string{"PASSWORD_SECRET_PATH": "/local/ext/pw"}} // #nosec G101 -- a secret's path, not a credential
	table := ydbexternal.Table{DataSource: "/local/ext/s3", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}

	observed, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
		Values: []schemaext.Value{&ydbexternal.DesiredTable{Spec: table, StructName: "Event"}, &ydbexternal.DesiredSource{Spec: source, StructName: "Warehouse"}}})
	c.Assert(err, qt.IsNil)
	restored, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Observed, To: schemaext.Desired, Values: observed})
	c.Assert(err, qt.IsNil)

	c.Assert(observed, qt.DeepEquals, []schemaext.Value{&ydbexternal.ObservedTable{Spec: table}, &ydbexternal.ObservedSource{Spec: source}})
	c.Assert(restored, qt.DeepEquals, []schemaext.Value{&ydbexternal.DesiredTable{Spec: table}, &ydbexternal.DesiredSource{Spec: source}})
}
