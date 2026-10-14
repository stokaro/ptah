package chsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/builtintest"
)

func TestSQLRowPolicyMatchesGoAndYAML(t *testing.T) {
	c := qt.New(t)
	runtime := builtintest.Runtime()
	sql := policySQL(c, createPolicy)
	yaml := must.Must(yamlschema.Parse(runtime.YAML(), []byte("row_policies:\n  tenant:\n    table: app.orders\n    using: tenant = 1\n    as: RESTRICTIVE\n    to: ALL EXCEPT admin\n")))
	goModel := must.Must(goschema.ParseSource(runtime.Annotations(), "policy.go", `package models
//ptah:schema:rowpolicy name="tenant" table="app.orders" using="tenant = 1" as="RESTRICTIVE" to="ALL EXCEPT admin"
type Policy struct{}
`))
	expected := policyObject(c, sql)
	want := must.Must(expected.Value.(*chschema.DesiredRowPolicy).Observed())
	for _, model := range []schemamodel.Database{*yaml, goModel} {
		object := policyObject(c, model)
		c.Assert(object.Ref, qt.DeepEquals, expected.Ref)
		c.Assert(must.Must(object.Value.(*chschema.DesiredRowPolicy).Observed()), qt.DeepEquals, want)
	}
}
