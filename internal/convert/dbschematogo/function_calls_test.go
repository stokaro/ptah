package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/deporder"
)

// claimsFunctions are two functions read back in name order, where the first
// calls the second: auth.uid() reads auth.verified_claims().
var claimsFunctions = []catalog.Function{
	{
		Schema:   "auth",
		Name:     "uid",
		Returns:  "uuid",
		Language: "sql",
		Body:     "SELECT nullif(auth.verified_claims() ->> 'sub', '')::uuid",
	},
	{
		Schema:   "auth",
		Name:     "verified_claims",
		Returns:  "jsonb",
		Language: "sql",
		Body:     "SELECT coalesce(nullif(current_setting('request.jwt.claims', true), ''), '{}')::jsonb",
	},
}

// TestConvert_OrdersReadFunctionsByWhatTheirBodiesCall converts the two
// functions and orders them for creation as a plan does. PostgreSQL resolves a
// LANGUAGE sql body when the function is created, so the callee comes first
// whatever order the reader listed them in.
func TestConvert_OrdersReadFunctionsByWhatTheirBodiesCall(t *testing.T) {
	c := qt.New(t)

	database := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{Functions: claimsFunctions}, "postgres", must.Must(builtin.New())))

	c.Assert(database.FunctionDependencies["auth.uid"], qt.DeepEquals, []string{"auth.verified_claims"})
	c.Assert(database.FunctionDependencies["auth.verified_claims"], qt.HasLen, 0)
	ordered := deporder.FunctionsForCreate(database, database.Functions)
	c.Assert(ordered, qt.HasLen, 2)
	c.Assert(ordered[0].Name, qt.Equals, "auth.verified_claims")
	c.Assert(ordered[1].Name, qt.Equals, "auth.uid")
}
