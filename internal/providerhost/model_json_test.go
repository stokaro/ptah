package providerhost_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/modeljson"

	"ptah.run/core/schemaext"
)

// The SDK cannot import the host, and public host contracts cannot import the
// SDK. This application boundary can compare both independent implementations.
func TestModelDefinitionsNormalizeIdenticallyAcrossModules(t *testing.T) {
	for _, input := range []string{
		` { "z": 9007199254740993, "a": -18446744073709551616 } `,
		`{"fraction":0.123456789012345678901234567890,"exponent":1e400,"negative_zero":-0}`,
		`{"a":[3,1,2],"b":null,"c":{},"d":[],"e":false}`,
		`{"escaped":"\ud83d\ude00","literal":"😀","text":"\\ud800"}`,
		`{"quoted":"\"\\","html":"<>&","line":"\u2028\u2029"}`,
		strings.Repeat("[", 255) + "0" + strings.Repeat("]", 255),
	} {
		t.Run(input, func(t *testing.T) {
			c := qt.New(t)
			host, err := schemaext.CanonicalJSON([]byte(input))
			c.Assert(err, qt.IsNil)
			sdk, err := modeljson.Canonical([]byte(input))
			c.Assert(err, qt.IsNil)
			c.Assert(sdk, qt.DeepEquals, host)
		})
	}
}

func TestModelDefinitionsRefuseAmbiguityInBothModules(t *testing.T) {
	for _, input := range []string{
		`{"x":1,"x":2}`, `{"outer":{"x":1,"\u0078":2}}`, `[] []`, `{`,
		`"\ud800"`, `"\udc00"`, `"\ud800\u0041"`, "\"\xff\"", `NaN`, `01`,
		strings.Repeat("[", 258) + "0" + strings.Repeat("]", 258),
	} {
		t.Run(input, func(t *testing.T) {
			c := qt.New(t)
			host, err := schemaext.CanonicalJSON([]byte(input))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(host, qt.IsNil)
			sdk, err := modeljson.Canonical([]byte(input))
			c.Assert(err, qt.ErrorIs, modeljson.ErrInvalidJSON)
			c.Assert(sdk, qt.IsNil)
		})
	}
}
