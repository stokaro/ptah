package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/deporder"
)

func viewNames(objects []deporder.ViewLike) []string {
	names := make([]string, 0, len(objects))
	for _, object := range objects {
		names = append(names, object.Name)
	}
	return names
}

// TestViewLikesForCreateForDialect_ReadsEachQuotingOfAQualifiedReference pins
// that a view body naming analytics.zbase orders after it however the name is
// quoted: wholly, part by part, or not at all. Created first, the reader fails
// on execution with `relation "analytics.zbase" does not exist`
// (stokaro/ptah#1664).
func TestViewLikesForCreateForDialect_ReadsEachQuotingOfAQualifiedReference(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "a quoted qualified reference", body: `SELECT id FROM "analytics"."zbase"`},
		{name: "an unquoted qualified reference", body: "SELECT id FROM analytics.zbase"},
		{name: "a quoted name in an unquoted schema", body: `SELECT id FROM analytics."zbase"`},
		{name: "an unquoted name in a quoted schema", body: `SELECT id FROM "analytics".zbase`},
		{name: "a mixed-case reference", body: "SELECT id FROM Analytics.ZBase"},
		{name: "spaces around the dot", body: "SELECT id FROM analytics . zbase"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// Declared dependent-first, and named so alphabetical order is the
			// wrong order, so only the dependency edge can reorder them.
			objects := []deporder.ViewLike{
				{Name: "analytics.asummary", Body: test.body},
				{Name: "analytics.zbase", Body: "SELECT id FROM analytics.events"},
			}

			ordered := deporder.ViewLikesForCreateForDialect(objects, "postgres")

			c.Assert(viewNames(ordered), qt.DeepEquals, []string{"analytics.zbase", "analytics.asummary"})
		})
	}
}

// TestViewLikesForCreateForDialect_ReadsTheDefaultSchema pins the default
// schema in the creation order: an unqualified name is in it. A declaration
// without a schema is read by a body that names it in public, and an
// unqualified reference reads public.zbase where another schema has a zbase
// too, which leaves the bare name ambiguous.
func TestViewLikesForCreateForDialect_ReadsTheDefaultSchema(t *testing.T) {
	tests := []struct {
		name    string
		objects []deporder.ViewLike
		want    []string
	}{
		{
			name: "a qualified reference to an object declared without a schema",
			objects: []deporder.ViewLike{
				{Name: "asummary", Body: "SELECT id FROM public.zbase"},
				{Name: "zbase", Body: "SELECT id FROM events"},
			},
			want: []string{"zbase", "asummary"},
		},
		{
			name: "an unqualified reference where two schemas have the name",
			objects: []deporder.ViewLike{
				{Name: "asummary", Body: "SELECT id FROM zbase"},
				{Name: "public.zbase", Body: "SELECT id FROM events"},
				{Name: "analytics.zbase", Body: "SELECT id FROM analytics.events"},
			},
			want: []string{"public.zbase", "asummary", "analytics.zbase"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			ordered := deporder.ViewLikesForCreateForDialect(test.objects, "postgres")

			c.Assert(viewNames(ordered), qt.DeepEquals, test.want)
		})
	}
}

// TestViewLikesForCreateForDialect_ReadsBracketQuotedNames pins SQL Server's
// quoting where two schemas hold the name: a part in brackets is a quoted
// part, so `[sales].[zbase]` reads sales.zbase alone, and `dbo.[zbase]` reads
// dbo.zbase alone. Read as two bare words, `[sales].[zbase]` also reads
// dbo.zbase in the default schema, and the reader waits for it.
func TestViewLikesForCreateForDialect_ReadsBracketQuotedNames(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "a bracketed reference into another schema", body: "SELECT id FROM [sales].[zbase]",
			want: []string{"sales.zbase", "dbo.asummary", "dbo.zbase"},
		},
		{
			name: "a partly bracketed reference", body: "SELECT id FROM dbo.[zbase]",
			want: []string{"sales.zbase", "dbo.zbase", "dbo.asummary"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects := []deporder.ViewLike{
				{Name: "dbo.asummary", Body: test.body},
				{Name: "sales.zbase", Body: "SELECT 1 AS id"},
				{Name: "dbo.zbase", Body: "SELECT 1 AS id"},
			}

			ordered := deporder.ViewLikesForCreateForDialect(objects, platform.SQLServer)

			c.Assert(viewNames(ordered), qt.DeepEquals, test.want)
		})
	}
}

// TestViewLikesForCreateForDialect_KeepsOtherSchemasApart is the control for
// both tests above: a reference into another schema, or an unqualified one,
// reads no object of the same name in a schema it does not name, so the
// declarations keep their order.
func TestViewLikesForCreateForDialect_KeepsOtherSchemasApart(t *testing.T) {
	tests := []struct {
		name    string
		objects []deporder.ViewLike
	}{
		{
			name: "a reference into another schema",
			objects: []deporder.ViewLike{
				{Name: "asummary", Body: "SELECT id FROM analytics.zbase"},
				{Name: "zbase", Body: "SELECT id FROM events"},
			},
		},
		{
			name: "a quoted reference into another schema",
			objects: []deporder.ViewLike{
				{Name: "public.asummary", Body: `SELECT id FROM "analytics"."zbase"`},
				{Name: "public.zbase", Body: "SELECT id FROM events"},
			},
		},
		{
			name: "an unqualified reference to an object outside the default schema",
			objects: []deporder.ViewLike{
				{Name: "asummary", Body: "SELECT id FROM zbase"},
				{Name: "analytics.zbase", Body: "SELECT id FROM analytics.events"},
				{Name: "reporting.zbase", Body: "SELECT id FROM reporting.events"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			ordered := deporder.ViewLikesForCreateForDialect(test.objects, "postgres")

			c.Assert(viewNames(ordered), qt.DeepEquals, viewNames(test.objects))
		})
	}
}

// TestViewLikesForCreateKeepsIndependentViewsInCallerOrder is the control: the
// sort must reorder only what a reference requires.
//
// A sort that always moved the second declaration ahead would satisfy every row
// above without deriving anything.
func TestViewLikesForCreateKeepsIndependentViewsInCallerOrder(t *testing.T) {
	c := qt.New(t)
	objects := []deporder.ViewLike{
		{Name: "analytics.asummary", Body: "SELECT id FROM analytics.events"},
		{Name: "analytics.zbase", Body: "SELECT id FROM analytics.events"},
	}

	ordered := deporder.ViewLikesForCreateForDialect(objects, "postgres")

	c.Assert(viewNames(ordered), qt.DeepEquals,
		[]string{"analytics.asummary", "analytics.zbase"})
}
