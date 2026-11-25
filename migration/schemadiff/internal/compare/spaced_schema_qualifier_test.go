package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestViewDefinitionsWithDialect_AQualifierOfAnotherSchemaIsAChange compares a
// view whose stored body reads a.src from a schema no bare spelling names: " a",
// or "A" where a quoted name keeps its case. The bare qualifier names the
// schema a, not the view's own schema, so it is not the one a catalog adds and
// the comparison keeps it: the body differs from one that reads the src of its
// own schema.
func TestViewDefinitionsWithDialect_AQualifierOfAnotherSchemaIsAChange(t *testing.T) {
	tests := []struct {
		name, schema string
	}{
		{name: "a schema that begins with a space", schema: " a"},
		{name: "an upper-case schema", schema: "A"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ViewDefinitionsWithDialect(
				schemamodel.View{Name: "v", Body: "SELECT id FROM src"},
				catalog.View{Name: "v", Schema: test.schema, Body: "SELECT id FROM a.src"},
				platform.Postgres,
			)

			c.Assert(diff.Changes["body"], qt.Equals, "SELECT id FROM a.src -> SELECT id FROM src")
		})
	}
}

// TestViewDefinitionsWithDialect_AQualifierOfItsOwnSchemaIsNoChange is the
// control: the qualifier a catalog adds for the view's own schema is still
// removed where a bare spelling names that schema, so the test above cannot
// pass on a comparison that strips nothing. MySQL does not keep the case of a
// quoted name, so there a bare qualifier names a schema of any case.
func TestViewDefinitionsWithDialect_AQualifierOfItsOwnSchemaIsNoChange(t *testing.T) {
	tests := []struct {
		name, dialect, schema, live string
	}{
		{name: "a lower-case schema", dialect: platform.Postgres, schema: "a", live: "SELECT id FROM a.src"},
		{name: "a MySQL schema in another case", dialect: platform.MySQL, schema: "App", live: "SELECT id FROM App.src"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ViewDefinitionsWithDialect(
				schemamodel.View{Name: "v", Body: "SELECT id FROM src"},
				catalog.View{Name: "v", Schema: test.schema, Body: test.live},
				test.dialect,
			)

			c.Assert(diff.Changes["body"], qt.Equals, "")
		})
	}
}

// TestViewDefinitionsWithDialect_AQuotedQualifierNoBareSpellingReachesIsAChange
// compares views whose stored body reads a relation through a quoted schema
// that no bare spelling names. Read without its quotes, " a".src becomes
// a.src and "A".src becomes a.src, so a view reading another schema's table
// compares equal to the declaration and keeps serving the wrong rows.
func TestViewDefinitionsWithDialect_AQuotedQualifierNoBareSpellingReachesIsAChange(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		schema   string
		live     string
		want     string
	}{
		{
			name:     "a view of a reading the src of \" a\"",
			declared: "SELECT id FROM src",
			schema:   "a",
			live:     `SELECT id FROM " a".src`,
			want:     `SELECT id FROM " a".src -> SELECT id FROM src`,
		},
		{
			name:     "a view of public reading the src of \" a\", declared on a.src",
			declared: "SELECT id FROM a.src",
			schema:   "public",
			live:     `SELECT id FROM " a".src`,
			want:     `SELECT id FROM " a".src -> SELECT id FROM a.src`,
		},
		{
			name:     "a view of public reading the src of \"A\", declared on a.src",
			declared: "SELECT id FROM a.src",
			schema:   "public",
			live:     `SELECT id FROM "A".src`,
			want:     `SELECT id FROM "A".src -> SELECT id FROM a.src`,
		},
		{
			name:     "a view of a reading the src of \"A\"",
			declared: "SELECT id FROM src",
			schema:   "a",
			live:     `SELECT id FROM "A".src`,
			want:     `SELECT id FROM "A".src -> SELECT id FROM src`,
		},
		{
			name:     "a quoted column with a space",
			declared: "SELECT id FROM src",
			schema:   "public",
			live:     `SELECT " id" FROM src`,
			want:     `SELECT " id" FROM src -> SELECT id FROM src`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ViewDefinitionsWithDialect(
				schemamodel.View{Name: "v", Body: test.declared},
				catalog.View{Name: "v", Schema: test.schema, Body: test.live},
				platform.Postgres,
			)

			c.Assert(diff.Changes["body"], qt.Equals, test.want)
		})
	}
}

// TestViewDefinitionsWithDialect_AQuotedNameABareSpellingReachesIsNoChange is
// the control: a quoted name that a bare spelling reaches still compares
// equal to that spelling, and the quoted qualifier of a view's own schema is
// still the one a catalog adds, so the test above cannot pass on a comparison
// that keeps every quote.
func TestViewDefinitionsWithDialect_AQuotedNameABareSpellingReachesIsNoChange(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		declared string
		schema   string
		live     string
	}{
		{name: "a lower-case quoted schema", dialect: platform.Postgres, declared: "SELECT id FROM a.src", schema: "public", live: `SELECT id FROM "a".src`},
		{name: "a lower-case quoted column", dialect: platform.Postgres, declared: "SELECT id FROM src", schema: "public", live: `SELECT "id" FROM src`},
		{name: "the quoted qualifier of its own spaced schema", dialect: platform.Postgres, declared: "SELECT id FROM src", schema: " a", live: `SELECT id FROM " a".src`},
		{name: "the quoted qualifier of its own upper-case schema", dialect: platform.Postgres, declared: "SELECT id FROM src", schema: "A", live: `SELECT id FROM "A".src`},
		{name: "an upper-case quoted name on both sides", dialect: platform.Postgres, declared: `SELECT "Id" FROM "Src"`, schema: "public", live: `SELECT "Id" FROM "Src"`},
		// MySQL reads a backtick-quoted column of any case as the column, so
		// the case of a quoted name is not kept there.
		{name: "a MySQL quoted column in another case", dialect: platform.MySQL, declared: "SELECT id FROM src", schema: "", live: "SELECT `Id` FROM src"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ViewDefinitionsWithDialect(
				schemamodel.View{Name: "v", Body: test.declared},
				catalog.View{Name: "v", Schema: test.schema, Body: test.live},
				test.dialect,
			)

			c.Assert(diff.Changes["body"], qt.Equals, "")
		})
	}
}
