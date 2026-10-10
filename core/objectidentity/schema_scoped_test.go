package objectidentity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
)

// A declared name carries its schema when it reads as qualified, and a name
// whose own quoted text holds a dot stays one name. Each form is the identity
// the components give, so a materialized view declared either way meets the
// one a reader reports.
func TestBuilderSchemaScoped(t *testing.T) {
	semantics := identifier.ForDialect("clickhouse")
	semantics.DefaultSchema = "analytics"
	builder := objectidentity.NewBuilder(semantics)
	for _, test := range []struct {
		name, declared, schema, object string
	}{
		{"unqualified", "daily", "", "daily"},
		{"qualified", "analytics.daily", "analytics", "daily"},
		{"another schema", "reports.daily", "reports", "daily"},
		{"a quoted name holding a dot", `"daily.v2"`, "", "daily.v2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := builder.SchemaScoped(objectidentity.KindMatView, test.declared)

			c.Assert(got.Key(), qt.Equals, builder.SchemaScopedParts(objectidentity.KindMatView, test.schema, test.object).Key())
		})
	}
	c := qt.New(t)
	c.Assert(builder.SchemaScoped(objectidentity.KindMatView, "daily").Key(), qt.Equals, builder.SchemaScoped(objectidentity.KindMatView, "analytics.daily").Key())
}
