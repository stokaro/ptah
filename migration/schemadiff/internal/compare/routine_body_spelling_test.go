package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// declaredItemCount is a function whose body CockroachDB v26.3.2 stores as
// storedItemCount: the relation qualified with the database it was created in
// (stokaro/ptah#4058).
var declaredItemCount = schemamodel.Function{
	Name: "item_count", Returns: "int8", Language: "sql", Volatility: "STABLE",
	Body: "SELECT count(*) FROM public.items",
}

const storedItemCount = "SELECT count(*) FROM f1.public.items;"

// routineWithBody compares declaredItemCount against a catalog function of the
// same name holding body, with the answers a server gave.
func routineWithBody(body string, spellings map[string]config.RoutineArguments) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	compare.FunctionsWithSemantics(
		&schemamodel.Database{Functions: []schemamodel.Function{declaredItemCount}},
		&catalog.Database{Functions: []catalog.Function{{
			Schema: "public", Name: "item_count", Returns: "int8", Body: body,
			Language: "sql", Security: "INVOKER", Volatility: "STABLE",
		}}},
		diff, platform.CockroachDB, identifier.ForDialect(platform.CockroachDB), spellings,
	)
	return diff
}

// bodySpelling is the answer a server gives for declaredItemCount.
func bodySpelling(answer config.RoutineArguments) map[string]config.RoutineArguments {
	return map[string]config.RoutineArguments{exprkey.RoutineArguments(declaredItemCount): answer}
}

// Where the server stored the declared body, its stored form is what the
// catalog's body is compared with. Compared as text, the declaration never
// equals what CockroachDB stored, and the function was replaced on every plan.
func TestFunctionsWithSemantics_ComparesTheBodyTheServerStored(t *testing.T) {
	c := qt.New(t)

	diff := routineWithBody(storedItemCount, bodySpelling(config.RoutineArguments{
		Result: "int8", Resolved: true, Body: storedItemCount, BodyResolved: true,
	}))

	c.Assert(diff.FunctionsModified, qt.HasLen, 0)
	c.Assert(diff.FunctionsAdded, qt.HasLen, 0)
	c.Assert(diff.FunctionsRemoved, qt.HasLen, 0)
}

// The controls. A body the server stored differently from the catalog's is a
// change, and the plan writes the declared body. Without a body answer -- one
// the server gave for the signature alone, as PostgreSQL's probe does, or none
// -- the body is compared as text.
func TestFunctionsWithSemantics_TheBodyTheServerStoredStillSeesAChange(t *testing.T) {
	tests := []struct {
		name      string
		catalog   string
		spellings map[string]config.RoutineArguments
	}{
		{
			name:    "a stored body against another body",
			catalog: "SELECT count(*) + 1 FROM f1.public.items;",
			spellings: bodySpelling(config.RoutineArguments{
				Result: "int8", Resolved: true, Body: storedItemCount, BodyResolved: true,
			}),
		},
		{
			name:      "a signature answered without the body",
			catalog:   storedItemCount,
			spellings: bodySpelling(config.RoutineArguments{Result: "int8", Resolved: true}),
		},
		{
			name:    "no server",
			catalog: storedItemCount,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := routineWithBody(test.catalog, test.spellings)

			c.Assert(diff.FunctionsModified, qt.HasLen, 1)
			c.Assert(diff.FunctionsModified[0].Changes["body"], qt.Not(qt.Equals), "")
			c.Assert(diff.FunctionsModified[0].Desired.Body, qt.Equals, declaredItemCount.Body)
		})
	}
}
