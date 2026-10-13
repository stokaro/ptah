package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/internal/builtintest"
)

// The strict attribute reaches the model, so a Go schema can declare the
// STRICT a SQL schema declares with the clause (stokaro/ptah#3562).
func TestParseFunctionAnnotation_Strict(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		wantStrict bool
	}{
		{name: "strict", annotation: `strict="true"`, wantStrict: true},
		{name: "stated false", annotation: `strict="false"`},
		{name: "not stated", annotation: ``},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := "package p\n" +
				`//ptah:schema:function name="twice" params="a integer" returns="integer" language="sql" ` +
				test.annotation + ` body="SELECT a * 2"` + "\ntype TestStruct struct {}\n"

			database, err := goschema.ParseSource(builtintest.Annotations(), "routine.go", source)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Functions, qt.HasLen, 1)
			properties, _, propertyErr := pgroutine.PropertiesOf(database.Functions[0].Facets)
			c.Assert(propertyErr, qt.IsNil)

			c.Assert(properties.Strict, qt.Equals, test.wantStrict)
		})
	}
}
