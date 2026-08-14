package objectlookup_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
)

// TestView_HappyPath pins the two spellings a diff and a schema legitimately use
// for the same view. The down direction plans against a schema converted back
// from an introspected database, which qualifies every name it read; the diff
// records the name the Go schema spells, which is normally bare.
func TestView_HappyPath(t *testing.T) {

	tests := []struct {
		name      string
		views     []goschema.View
		lookup    string
		semantics identifier.Semantics
		wantBody  string
	}{
		{
			name:      "exact name",
			views:     []goschema.View{{Name: "active_users", Body: "exact"}},
			lookup:    "active_users",
			semantics: identifier.ForDialect("postgres"),
			wantBody:  "exact",
		},
		{
			name:      "diff spells it bare, schema qualifies it",
			views:     []goschema.View{{Name: "reporting.active_users", Body: "qualified"}},
			lookup:    "active_users",
			semantics: identifier.ForDialect("postgres"),
			wantBody:  "qualified",
		},
		{
			name:      "diff qualifies it, schema spells it bare",
			views:     []goschema.View{{Name: "active_users", Body: "bare"}},
			lookup:    "reporting.active_users",
			semantics: identifier.ForDialect("postgres"),
			wantBody:  "bare",
		},
		{
			name: "an exact match wins over an unqualified one",
			views: []goschema.View{
				{Name: "reporting.active_users", Body: "qualified"},
				{Name: "active_users", Body: "bare"},
			},
			lookup:    "active_users",
			semantics: identifier.ForDialect("postgres"),
			wantBody:  "bare",
		},
		{
			// The default schema is what settles it. Under a structural rule
			// this is two candidates and no answer; under PostgreSQL's rules a
			// bare name IS the one in public.
			name: "a bare name resolves to the default schema, not to the other one",
			views: []goschema.View{
				{Name: "public.active_users", Body: "public"},
				{Name: "archive.active_users", Body: "archive"},
			},
			lookup:    "active_users",
			semantics: identifier.ForDialect("postgres"),
			wantBody:  "public",
		},
		{
			// Symmetric to the row above: the qualified spelling is the diff's
			// and the bare one is the declaration's.
			name: "a default-schema name resolves to the bare declaration",
			views: []goschema.View{
				{Name: "active_users", Body: "bare"},
				{Name: "archive.active_users", Body: "archive"},
			},
			lookup:    "public.active_users",
			semantics: identifier.ForDialect("postgres"),
			wantBody:  "bare",
		},
		{
			name:      "SQL Server resolves a bare name against dbo",
			views:     []goschema.View{{Name: "dbo.active_users", Body: "dbo"}},
			lookup:    "active_users",
			semantics: identifier.ForDialect("sqlserver"),
			wantBody:  "dbo",
		},
		{
			name:      "SQLite folds ASCII case",
			views:     []goschema.View{{Name: "Active_Users", Body: "folded"}},
			lookup:    "active_users",
			semantics: identifier.ForDialect("sqlite"),
			wantBody:  "folded",
		},
		{
			name:      "PostgreSQL does not fold case",
			views:     []goschema.View{{Name: "Active_Users", Body: "unfolded"}, {Name: "active_users", Body: "exact"}},
			lookup:    "active_users",
			semantics: identifier.ForDialect("postgres"),
			wantBody:  "exact",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := objectlookup.View(test.views, test.lookup, test.semantics)
			c.Assert(got, qt.IsNotNil)
			c.Assert(got.Body, qt.Equals, test.wantBody)
		})
	}
}

// TestView_FailurePath pins what the resolver refuses to guess at. Two views of
// the same name in different schemas name no one object, and rendering a
// statement for either of them would be a coin toss on which one the migration
// destroys.
func TestView_FailurePath(t *testing.T) {

	tests := []struct {
		name      string
		views     []goschema.View
		lookup    string
		semantics identifier.Semantics
	}{
		{
			name:      "no candidate at all",
			views:     []goschema.View{{Name: "other_view"}},
			lookup:    "active_users",
			semantics: identifier.ForDialect("postgres"),
		},
		{
			name: "the same name in two non-default schemas",
			views: []goschema.View{
				{Name: "reporting.active_users", Body: "reporting"},
				{Name: "archive.active_users", Body: "archive"},
			},
			lookup:    "active_users",
			semantics: identifier.ForDialect("postgres"),
		},
		{
			name: "PostgreSQL keeps two case-distinct names apart",
			views: []goschema.View{
				{Name: "reporting.Active_Users", Body: "upper"},
				{Name: "reporting.active_users", Body: "lower"},
			},
			lookup:    "ACTIVE_USERS",
			semantics: identifier.ForDialect("postgres"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(objectlookup.View(test.views, test.lookup, test.semantics), qt.IsNil)
		})
	}
}

func TestMaterializedView_HappyPath(t *testing.T) {
	c := qt.New(t)

	views := []goschema.MaterializedView{{Name: "reporting.user_stats", Body: "qualified"}}
	got := objectlookup.MaterializedView(views, "user_stats", identifier.ForDialect("postgres"))
	c.Assert(got, qt.IsNotNil)
	c.Assert(got.Body, qt.Equals, "qualified")
}

func TestMaterializedView_FailurePath(t *testing.T) {
	c := qt.New(t)

	views := []goschema.MaterializedView{
		{Name: "reporting.user_stats"},
		{Name: "archive.user_stats"},
	}
	c.Assert(objectlookup.MaterializedView(views, "user_stats", identifier.ForDialect("postgres")), qt.IsNil)
}

// TestTrigger_HappyPath covers the half of a trigger's identity that carries a
// schema: the table it hangs on.
func TestTrigger_HappyPath(t *testing.T) {

	tests := []struct {
		name      string
		triggers  []goschema.Trigger
		table     string
		trigger   string
		semantics identifier.Semantics
		wantBody  string
	}{
		{
			name:      "the table half is qualified in the schema only",
			triggers:  []goschema.Trigger{{Name: "touch", Table: "reporting.users", Body: "qualified"}},
			table:     "users",
			trigger:   "touch",
			semantics: identifier.ForDialect("postgres"),
			wantBody:  "qualified",
		},
		{
			name:      "the table half resolves through the default schema",
			triggers:  []goschema.Trigger{{Name: "touch", Table: "public.users", Body: "public"}},
			table:     "users",
			trigger:   "touch",
			semantics: identifier.ForDialect("postgres"),
			wantBody:  "public",
		},
		{
			name:      "SQLite folds the trigger name too",
			triggers:  []goschema.Trigger{{Name: "Touch", Table: "users", Body: "folded"}},
			table:     "users",
			trigger:   "touch",
			semantics: identifier.ForDialect("sqlite"),
			wantBody:  "folded",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := objectlookup.Trigger(test.triggers, test.table, test.trigger, test.semantics)
			c.Assert(got, qt.IsNotNil)
			c.Assert(got.Body, qt.Equals, test.wantBody)
		})
	}
}

func TestTrigger_FailurePath(t *testing.T) {

	tests := []struct {
		name      string
		triggers  []goschema.Trigger
		table     string
		trigger   string
		semantics identifier.Semantics
	}{
		{
			name:      "the trigger name still has to match",
			triggers:  []goschema.Trigger{{Name: "touch", Table: "reporting.users"}},
			table:     "users",
			trigger:   "other",
			semantics: identifier.ForDialect("postgres"),
		},
		{
			name: "the same trigger name on the same table in two schemas",
			triggers: []goschema.Trigger{
				{Name: "touch", Table: "reporting.users"},
				{Name: "touch", Table: "archive.users"},
			},
			table:     "users",
			trigger:   "touch",
			semantics: identifier.ForDialect("postgres"),
		},
		{
			name:      "PostgreSQL does not fold the trigger name",
			triggers:  []goschema.Trigger{{Name: "Touch", Table: "users"}},
			table:     "users",
			trigger:   "touch",
			semantics: identifier.ForDialect("postgres"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				objectlookup.Trigger(test.triggers, test.table, test.trigger, test.semantics),
				qt.IsNil,
			)
		})
	}
}

// TestContains pins the identity-aware replacement for the
// `slices.Contains(diff.Tables…, ref)` shape.
func TestContains(t *testing.T) {

	tests := []struct {
		name      string
		names     []string
		lookup    string
		semantics identifier.Semantics
		want      bool
	}{
		{
			name:      "exact",
			names:     []string{"orders"},
			lookup:    "orders",
			semantics: identifier.ForDialect("postgres"),
			want:      true,
		},
		{
			name:      "the collection qualifies, the reference does not",
			names:     []string{"public.orders"},
			lookup:    "orders",
			semantics: identifier.ForDialect("postgres"),
			want:      true,
		},
		{
			name:      "the reference qualifies, the collection does not",
			names:     []string{"orders"},
			lookup:    "public.orders",
			semantics: identifier.ForDialect("postgres"),
			want:      true,
		},
		{
			name:      "SQL Server resolves dbo",
			names:     []string{"dbo.orders"},
			lookup:    "orders",
			semantics: identifier.ForDialect("sqlserver"),
			want:      true,
		},
		{
			name:      "SQLite folds case",
			names:     []string{"main.Orders"},
			lookup:    "orders",
			semantics: identifier.ForDialect("sqlite"),
			want:      true,
		},
		{
			name:      "a different object is not contained",
			names:     []string{"public.orders"},
			lookup:    "customers",
			semantics: identifier.ForDialect("postgres"),
			want:      false,
		},
		{
			name:      "PostgreSQL keeps case-distinct names apart",
			names:     []string{"public.Orders"},
			lookup:    "orders",
			semantics: identifier.ForDialect("postgres"),
			want:      false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(objectlookup.Contains(test.names, test.lookup, test.semantics), qt.Equals, test.want)
		})
	}
}

// TestQualified covers the convenience entry point every schema object with a
// QualifiedName goes through: tables, domains, composites, ranges, sequences and
// enums.
func TestQualified(t *testing.T) {

	tests := []struct {
		name       string
		tables     []goschema.Table
		lookup     string
		semantics  identifier.Semantics
		wantStruct string
	}{
		{
			name:       "the diff qualifies, the declaration does not",
			tables:     []goschema.Table{{StructName: "User", Name: "users"}},
			lookup:     "public.users",
			semantics:  identifier.ForDialect("postgres"),
			wantStruct: "User",
		},
		{
			name:       "the declaration qualifies, the diff does not",
			tables:     []goschema.Table{{StructName: "User", Name: "users", Schema: "public"}},
			lookup:     "users",
			semantics:  identifier.ForDialect("postgres"),
			wantStruct: "User",
		},
		{
			name:       "SQL Server resolves dbo",
			tables:     []goschema.Table{{StructName: "Order", Name: "orders", Schema: "dbo"}},
			lookup:     "orders",
			semantics:  identifier.ForDialect("sqlserver"),
			wantStruct: "Order",
		},
		{
			// A non-default schema is not the default one, and the two must stay
			// distinct or a fix that resolves everything would pass.
			name: "a non-default schema is preferred over the default one when named",
			tables: []goschema.Table{
				{StructName: "PublicUser", Name: "users", Schema: "public"},
				{StructName: "AppUser", Name: "users", Schema: "app"},
			},
			lookup:     "app.users",
			semantics:  identifier.ForDialect("postgres"),
			wantStruct: "AppUser",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := objectlookup.Qualified(test.tables, test.lookup, test.semantics)
			c.Assert(got, qt.IsNotNil)
			c.Assert(got.StructName, qt.Equals, test.wantStruct)
		})
	}
}
