package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine/builtin"
)

// BenchmarkValidateSchemaWithInlineIndexes includes the table render required
// to validate an inline index, with provider selection outside the measured call.
func BenchmarkValidateSchemaWithInlineIndexes(b *testing.B) {
	c := qt.New(b)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	request := schemavalidation.Request{
		Target: "ydb", Capabilities: capability.YDB262(), Schema: ydbIndexedSchema("label"),
	}
	b.ReportAllocs()
	for b.Loop() {
		result, err := runtime.ValidateSchema(b.Context(), request)
		c.Assert(err, qt.IsNil)
		c.Assert(result.Err(request.Target), qt.IsNil)
	}
}
