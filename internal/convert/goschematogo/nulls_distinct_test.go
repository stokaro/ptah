package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// nullsSchema is a table with a unique index and a unique constraint, each
// treating NULLs as nullsDistinct says.
func nullsSchema(nullsDistinct *bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Account", Name: "accounts", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Account", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Account", FieldName: "Email", Name: "email", Type: "TEXT", Nullable: true},
			{StructName: "Account", FieldName: "Phone", Name: "phone", Type: "TEXT", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "Account", Name: "accounts_email", Fields: []string{"email"}, Unique: true,
			NullsDistinct: nullsDistinct}},
		Constraints: []schemamodel.Constraint{{StructName: "Account", Name: "accounts_phone", Type: "UNIQUE", Table: "accounts",
			Columns: []string{"phone"}, NullsDistinct: nullsDistinct}},
	}
}

// TestRender_NullsDistinct pins that an index and a constraint keep how they
// treat NULLs through the Go source. A NULLS NOT DISTINCT read from a database
// and written without it reads back as the server's default, and comparing
// the source with that database plans a rebuild that drops the clause.
func TestRender_NullsDistinct(t *testing.T) {
	tests := []struct {
		name          string
		nullsDistinct *bool
		wantIndex     string
		wantKey       string
	}{
		{name: "NULLS NOT DISTINCT", nullsDistinct: new(false),
			wantIndex: `//ptah:schema:index name="accounts_email" fields="email" unique="true" nulls_distinct="false"`,
			wantKey:   `//ptah:schema:constraint name="accounts_phone" type="UNIQUE" table="accounts" columns="phone" nulls_distinct="false"`},
		{name: "NULLS DISTINCT, stated", nullsDistinct: new(true),
			wantIndex: `//ptah:schema:index name="accounts_email" fields="email" unique="true" nulls_distinct="true"`,
			wantKey:   `//ptah:schema:constraint name="accounts_phone" type="UNIQUE" table="accounts" columns="phone" nulls_distinct="true"`},
		{name: "not stated",
			wantIndex: `//ptah:schema:index name="accounts_email" fields="email" unique="true"` + "\n",
			wantKey:   `//ptah:schema:constraint name="accounts_phone" type="UNIQUE" table="accounts" columns="phone"` + "\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := nullsSchema(test.nullsDistinct)

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})
			c.Assert(err, qt.IsNil)
			reparsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", files[0].Data)

			c.Assert(err, qt.IsNil)
			c.Assert(string(files[0].Data), qt.Contains, test.wantIndex)
			c.Assert(string(files[0].Data), qt.Contains, test.wantKey)
			c.Assert(reparsed.Indexes[0].NullsDistinct, qt.DeepEquals, test.nullsDistinct)
			c.Assert(reparsed.Constraints[0].NullsDistinct, qt.DeepEquals, test.nullsDistinct)
		})
	}
}
