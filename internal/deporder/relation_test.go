package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/deporder"
)

// relationReference is one question to [deporder.ReferencesRelation], asked
// in a session whose default schema and search path are given; empty is not
// known.
type relationReference struct {
	name          string
	body          string
	relation      string
	defaultSchema string
	searchPath    []string
}

// semantics is the session a row describes.
func (r relationReference) semantics() identifier.Semantics {
	return identifier.Semantics{DefaultSchema: r.defaultSchema, SearchPath: r.searchPath}
}

// TestReferencesRelation_Reads pins the spellings that name a relation. A
// reference the predicate misses is a reader the PostgreSQL planner neither
// rebuilds nor refuses: DROP ... CASCADE takes it, and a row-level security
// policy that reads it is gone with no error (stokaro/ptah#5004).
func TestReferencesRelation_Reads(t *testing.T) {
	tests := []relationReference{
		{name: "a bare reference", body: "SELECT total FROM order_totals", relation: "order_totals"},
		{
			name: "an unquoted qualified reference", body: "SELECT total FROM reporting.order_totals",
			relation: "reporting.order_totals",
		},
		{
			name: "a quoted qualified reference", body: `SELECT total FROM "reporting"."order_totals"`,
			relation: "reporting.order_totals",
		},
		{
			name: "a quoted schema", body: `SELECT total FROM "reporting".order_totals`,
			relation: "reporting.order_totals",
		},
		{
			name: "a quoted name", body: `SELECT total FROM reporting."order_totals"`,
			relation: "reporting.order_totals",
		},
		{
			name: "mixed case in the body", body: "SELECT total FROM Reporting.ORDER_TOTALS",
			relation: "reporting.order_totals",
		},
		{
			name: "mixed case in the relation", body: "SELECT total FROM reporting.order_totals",
			relation: "Reporting.Order_Totals",
		},
		{
			// PostgreSQL keeps the case of a quoted part, but a relation name
			// does not record whether its declaration quoted it, so the
			// predicate errs toward reading it.
			name: "a quoted part in another case", body: `SELECT total FROM "Reporting"."Order_Totals"`,
			relation: "reporting.order_totals",
		},
		{
			name: "a quoted relation name", body: `SELECT total FROM "reporting"."order_totals"`,
			relation: `"reporting"."order_totals"`,
		},
		{
			// With the default schema known, a bare order_totals is not
			// reporting's, so only the dotted name can read it.
			name: "spaces around the dot", body: "SELECT total FROM reporting . order_totals",
			relation: "reporting.order_totals", defaultSchema: "public", searchPath: []string{"public"},
		},
		{
			name: "a line break around the dot", body: "SELECT total FROM reporting\n\t.order_totals",
			relation: "reporting.order_totals", defaultSchema: "public", searchPath: []string{"public"},
		},
		{
			name: "a database in front", body: "SELECT total FROM shop.reporting.order_totals",
			relation: "reporting.order_totals",
		},
		{
			name: "a dot inside a quoted name", body: `SELECT id FROM "tenant.data"`,
			relation: `"tenant.data"`,
		},
		{
			name: "a doubled quote inside a quoted name", body: `SELECT id FROM reporting."say ""hi"""`,
			relation: `reporting."say ""hi"""`,
		},
		{
			name: "a backtick-quoted reference", body: "SELECT id FROM `reporting`.`order_totals`",
			relation: "reporting.order_totals",
		},
		{
			name: "a name after a closed literal", body: "SELECT 'x.y' AS label FROM reporting.order_totals",
			relation: "reporting.order_totals",
		},
		{
			name: "a name after a closed comment", body: "SELECT 1 /* reporting.x */ FROM reporting.order_totals",
			relation: "reporting.order_totals",
		},
		{
			name: "a bare reference to a qualified relation, schema unknown", body: "SELECT total FROM order_totals",
			relation: "reporting.order_totals",
		},
		{
			name: "a bare reference to a relation in the default schema", body: "SELECT id FROM items",
			relation: "public.items", defaultSchema: "public",
		},
		{
			name: "a qualified reference to a bare relation, schema unknown", body: "SELECT id FROM reporting.items",
			relation: "items",
		},
		{
			name: "a reference in the default schema to a bare relation", body: "SELECT id FROM public.items",
			relation: "items", defaultSchema: "public",
		},
		{
			name: "the default schema in another case", body: `SELECT id FROM "Public".items`,
			relation: "items", defaultSchema: "public",
		},
		{
			name: "a bare relation in a non-public default schema", body: "SELECT id FROM app.items",
			relation: "items", defaultSchema: "app",
		},
		{
			// PostgreSQL prints a relation the path reaches without its
			// schema, so under reporting, public a policy reading
			// public.order_totals reads back as `order_totals`.
			name: "a bare reference to a relation further along the search path", body: "SELECT 1 FROM order_totals",
			relation: "public.order_totals", defaultSchema: "reporting", searchPath: []string{"reporting", "public"},
		},
		{
			name: "a bare reference to a relation outside the default schema, path not known", body: "SELECT id FROM item_skus",
			relation: "reporting.item_skus", defaultSchema: "public",
		},
		{
			// `İ` lower-cases to two runes, which strings.EqualFold does not
			// match against the one it was.
			name: "a quoted name that lower-cases to more runes", body: `SELECT id FROM "İtems"`,
			relation: `"İtems"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(deporder.ReferencesRelation(test.body, test.relation, test.semantics()), qt.IsTrue,
				qt.Commentf("body: %s", test.body))
		})
	}
}

// TestReferencesRelation_DoesNotRead pins the spellings that name another
// relation, or nothing. A false reference costs a rebuild of an object nothing
// changed, which loses what the plan does not declare, or a refused plan: read
// as public.item_skus, a policy reading reporting.item_skus stops a column type
// change under public.item_skus (stokaro/ptah#5005).
func TestReferencesRelation_DoesNotRead(t *testing.T) {
	tests := []relationReference{
		{
			name: "the same name in another schema", body: "SELECT id FROM reporting.item_skus",
			relation: "public.item_skus",
		},
		{
			name: "the same name quoted in another schema", body: `SELECT id FROM "reporting"."item_skus"`,
			relation: "public.item_skus",
		},
		{
			name: "a bare relation and a reference into another schema", body: "SELECT id FROM reporting.item_skus",
			relation: "item_skus", defaultSchema: "public",
		},
		{
			name: "a bare reference and a relation outside the default schema", body: "SELECT id FROM item_skus",
			relation: "reporting.item_skus", defaultSchema: "public", searchPath: []string{"public"},
		},
		{
			name: "a bare reference and a relation off the search path", body: "SELECT 1 FROM order_totals",
			relation: "analytics.order_totals", defaultSchema: "reporting", searchPath: []string{"reporting", "public"},
		},
		{
			// An object declared without a schema is created in the default
			// schema alone, wherever else the path reaches.
			name: "a bare relation and a reference into a later schema of the path", body: "SELECT 1 FROM public.order_totals",
			relation: "order_totals", defaultSchema: "reporting", searchPath: []string{"reporting", "public"},
		},
		{
			// The expression PostgreSQL prints for a policy reading
			// reporting.item_skus, with public on the search path.
			name: "a column qualified by the relation's name", body: "(id IN ( SELECT item_skus.id FROM reporting.item_skus))",
			relation: "public.item_skus", defaultSchema: "public",
		},
		{
			name: "the star of a relation of that name", body: "SELECT item_skus.* FROM reporting.item_skus",
			relation: "item_skus", defaultSchema: "public",
		},
		{
			name: "a dotted name inside a quoted identifier", body: `SELECT id FROM "reporting.order_totals"`,
			relation: "reporting.order_totals",
		},
		{
			name: "a longer name", body: "SELECT total FROM reporting.order_totals_v2",
			relation: "reporting.order_totals",
		},
		{
			name: "a name ending in the relation's", body: "SELECT total FROM reporting.all_order_totals",
			relation: "reporting.order_totals",
		},
		{
			name: "a string literal", body: "SELECT 'reporting.order_totals' AS label FROM orders",
			relation: "reporting.order_totals",
		},
		{
			name: "quoted names inside a string literal", body: `SELECT '"reporting"."order_totals"' AS label FROM orders`,
			relation: "reporting.order_totals",
		},
		{
			name: "an escape string literal", body: `SELECT E'\'reporting.order_totals' AS label FROM orders`,
			relation: "reporting.order_totals",
		},
		{
			name: "a line comment", body: "SELECT id FROM orders -- reads \"reporting\".order_totals\n",
			relation: "reporting.order_totals",
		},
		{
			name: "a block comment", body: "SELECT id /* reporting.order_totals */ FROM orders",
			relation: "reporting.order_totals",
		},
		{
			name: "a dollar-quoted string", body: "SELECT fn($q$ SELECT 1 FROM reporting.order_totals $q$) FROM orders",
			relation: "reporting.order_totals",
		},
		{
			name: "a digit-led word", body: "SELECT 1order_totals FROM orders",
			relation: "order_totals",
		},
		{name: "an empty body", body: "", relation: "order_totals"},
		{name: "a relation tableref does not accept", body: "SELECT 1 FROM a.b.c", relation: "a.b.c"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(deporder.ReferencesRelation(test.body, test.relation, test.semantics()), qt.IsFalse,
				qt.Commentf("body: %s", test.body))
		})
	}
}

// TestReferencesIdentifier_KnowsNoSchema pins what [deporder.ReferencesIdentifier]
// means for its callers that have no comparison to ask: an unqualified name
// may be in any schema, on either side. Read as public, the first row would
// miss a reader of reporting.items that a drop of items, in a reporting
// default, takes with it.
func TestReferencesIdentifier_KnowsNoSchema(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		relation string
	}{
		{name: "a qualified reference to a bare relation", body: "SELECT id FROM reporting.items", relation: "items"},
		{name: "a bare reference to a qualified relation", body: "SELECT id FROM items", relation: "reporting.items"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(deporder.ReferencesIdentifier(test.body, test.relation), qt.IsTrue)
		})
	}
}
