package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// A body is written between dollar quotes it does not contain. The server ends
// the literal at the first quote it meets, so a body holding `$$` written
// between `$$` is cut there and the rest is read as SQL (stokaro/ptah#3691).
func TestPostgres_CreateFunction_QuotesTheBodyWithATagItDoesNotHold(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		quote string
	}{
		{name: "a body without dollars", body: "SELECT 'x'", quote: "$$"},
		{name: "a body with one dollar", body: "SELECT $1", quote: "$$"},
		{name: "a body holding the plain quote", body: "SELECT $$x$$", quote: "$ptah$"},
		{name: "a body holding both", body: "SELECT $$x$$, $ptah$y$ptah$", quote: "$ptah1$"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			function := ast.NewCreateFunction("f").SetReturns("text").SetLanguage("sql").SetBody(test.body)

			got := renderPostgres(c, function)

			c.Assert(got, qt.Equals, "CREATE OR REPLACE FUNCTION \"f\"() RETURNS text AS "+test.quote+"\n"+
				test.body+"\n"+test.quote+"\nLANGUAGE sql;\n")
		})
	}
}

// The trigger function Ptah writes for a trigger's body follows the same rule.
func TestPostgres_Trigger_QuotesTheFunctionBodyWithATagItDoesNotHold(t *testing.T) {
	c := qt.New(t)
	trigger := &ast.CreateTriggerNode{
		Name: "t_stamp", Table: "t", Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW",
		Body: "NEW.note := $$x$$; RETURN NEW;",
	}

	got := renderPostgres(c, trigger)

	c.Assert(got, qt.Contains, "RETURNS trigger AS $ptah$\nBEGIN\nNEW.note := $$x$$; RETURN NEW;\nEND;\n$ptah$ LANGUAGE plpgsql;")
}

// The block that fills NULL rows with the column's default holds the default,
// which is the author's text and may hold `$$`.
func TestPostgres_SetNotNull_QuotesTheFillWithATagTheDefaultDoesNotHold(t *testing.T) {
	c := qt.New(t)
	column := ast.NewColumn("c", "TEXT").SetNotNull().SetDefault("$$")

	got := renderPostgres(c, modifyColumn(column, ast.ColumnProperties{Nullability: true}, true))

	c.Assert(got, qt.Equals, "-- ALTER statements: --\n"+
		"DO $ptah$\nBEGIN\n"+
		"    IF EXISTS (SELECT 1 FROM \"t\" WHERE \"c\" IS NULL LIMIT 1) THEN\n"+
		"        UPDATE \"t\" SET \"c\" = '$$' WHERE \"c\" IS NULL;\n"+
		"    END IF;\nEND\n$ptah$;\n"+
		"ALTER TABLE \"t\" ALTER COLUMN \"c\" SET NOT NULL;\n\n")
}
