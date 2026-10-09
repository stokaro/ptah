package ydbconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine"
)

// TestSecretConversion_CarriesNoVariableIntoAnObservation converts a
// declaration into the empty observation a read lists, and an observation
// into a declaration that keeps the secret with the default variable and asks
// for no rotation.
func TestSecretConversion_CarriesNoVariableIntoAnObservation(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: ydbsecret.Codecs(),
		Conversions: []engine.Conversion{{Target: "ydb", Kinds: []schemaext.Kind{ydbsecret.Kind}, Service: ydbconvert.SecretService{}}}})
	c.Assert(err, qt.IsNil)
	declared := &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW", StructName: "Credentials", Rotate: true}

	observed, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
		Values: []schemaext.Value{declared}})
	c.Assert(err, qt.IsNil)
	restored, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Observed, To: schemaext.Desired, Values: observed})
	c.Assert(err, qt.IsNil)

	c.Assert(observed, qt.DeepEquals, []schemaext.Value{&ydbsecret.Observed{}})
	c.Assert(restored, qt.DeepEquals, []schemaext.Value{&ydbsecret.Desired{}})
}
