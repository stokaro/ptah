package columnsequence_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/columnsequence"
)

// TestDeclared_HappyPath names the sequence each column that creates one
// creates, under the name PostgreSQL gives it.
func TestDeclared_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		field schemamodel.Field
		want  string
	}{
		{name: "bigserial", field: schemamodel.Field{Name: "id", Type: "bigserial"}, want: "items_id_seq"},
		{name: "SERIAL", field: schemamodel.Field{Name: "id", Type: "SERIAL"}, want: "items_id_seq"},
		{name: "smallserial", field: schemamodel.Field{Name: "n", Type: "smallserial"}, want: "items_n_seq"},
		{name: "serial8", field: schemamodel.Field{Name: "id", Type: "serial8"}, want: "items_id_seq"},
		{name: "AUTO_INCREMENT", field: schemamodel.Field{Name: "id", Type: "AUTO_INCREMENT"}, want: "items_id_seq"},
		{
			name:  "identity",
			field: schemamodel.Field{Name: "code", Type: "bigint", IdentityGeneration: "BY_DEFAULT"},
			want:  "items_code_seq",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := columnsequence.Declared("items", test.field)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestDeclared_FailurePath answers no sequence for a column that creates
// none, including one that draws from a sequence declared on its own.
func TestDeclared_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		field schemamodel.Field
	}{
		{name: "plain integer", field: schemamodel.Field{Name: "id", Type: "bigint"}},
		{name: "auto_inc flag alone", field: schemamodel.Field{Name: "id", Type: "integer", AutoInc: true}},
		{
			name:  "nextval default",
			field: schemamodel.Field{Name: "id", Type: "bigint", DefaultExpr: "nextval('shared_seq')"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := columnsequence.Declared("items", test.field)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}
