package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

func tables(names ...[2]string) *schemamodel.Database {
	database := &schemamodel.Database{}
	for _, name := range names {
		database.Tables = append(database.Tables, schemamodel.Table{Schema: name[0], Name: name[1]})
	}
	return database
}

func TestValidateTableSpellings_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		database      *schemamodel.Database
		defaultSchema string
	}{
		{name: "one spelling", database: tables([2]string{"", "accounts"}, [2]string{"", "invitations"}), defaultSchema: "public"},
		{name: "qualified only", database: tables([2]string{"public", "accounts"}), defaultSchema: "public"},
		{
			// A bare name lands in the default schema, which is not app, so the
			// two are different tables.
			name:          "the same name in another schema",
			database:      tables([2]string{"", "accounts"}, [2]string{"app", "accounts"}),
			defaultSchema: "public",
		},
		{
			// A server whose search path puts app first: the bare table is
			// app.accounts, and public.accounts is another table.
			name:          "a default schema the server chose",
			database:      tables([2]string{"", "accounts"}, [2]string{"public", "accounts"}),
			defaultSchema: "app",
		},
		{
			name:          "a target with no default schema",
			database:      tables([2]string{"", "accounts"}, [2]string{"public", "accounts"}),
			defaultSchema: "",
		},
		{name: "no schema", database: nil, defaultSchema: "public"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateTableSpellings(test.database, test.defaultSchema)

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateTableSpellings_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		database *schemamodel.Database
	}{
		{name: "bare first", database: tables([2]string{"", "accounts"}, [2]string{"public", "accounts"})},
		{name: "qualified first", database: tables([2]string{"public", "accounts"}, [2]string{"", "accounts"})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateTableSpellings(test.database, "public")

			c.Assert(err, qt.ErrorMatches, `table "accounts" is declared twice, once without a schema and once as "public.accounts"; `+
				`a table without a schema is created in the default schema "public", so both name one table -- `+
				`declare it once, or spell it the same way in every source`)
		})
	}
}
