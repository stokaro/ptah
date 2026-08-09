package objectlookup_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
)

// TestUnqualifiedTierOnlySuppliesAnUnstatedSchema pins the gate on tier 3.
//
// Comparing unqualified names alone resolves a name across two DIFFERENT
// schemas, and that is the answer no migration wants: the planner renders the
// statement against the name the DIFF carries and the definition it found under
// another schema, so the DDL applies cleanly to a relation the desired schema
// never declared. Measured through the exported planner entry point on
// PostgreSQL 17.10, a schema holding only `reporting.users` and a diff naming
// `app.users` produced `ALTER TABLE "app"."users" ADD COLUMN "note" TEXT NOT
// NULL`, and information_schema.columns afterwards showed the column on
// `app.users`.
//
// Tier 3 therefore answers only where a schema is missing, never where two are
// stated and disagree. Every `wantNil: true` row below resolves without the
// gate, and every `wantNil: false` row is the capability the gate must keep.
func TestUnqualifiedTierOnlySuppliesAnUnstatedSchema(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		tables    []goschema.Table
		lookup    string
		semantics identifier.Semantics
		wantNil   bool
		wantName  string
	}{
		{
			// The measured shape. Both sides name a schema and the two disagree.
			name:      "PostgreSQL declines two stated schemas that differ",
			tables:    []goschema.Table{{StructName: "User", Name: "users", Schema: "reporting"}},
			lookup:    "app.users",
			semantics: identifier.ForDialect("postgres"),
			wantNil:   true,
		},
		{
			// The same shape with the sides swapped: the declaration is the one
			// naming the schema the diff does not.
			name:      "PostgreSQL declines the reverse direction too",
			tables:    []goschema.Table{{StructName: "User", Name: "users", Schema: "app"}},
			lookup:    "reporting.users",
			semantics: identifier.ForDialect("postgres"),
			wantNil:   true,
		},
		{
			// MySQL and MariaDB have no static default schema, so this is the
			// only tier that can join a reader's `mydb.v` to a bare declaration.
			// The gate must not take that away.
			name:      "MySQL still resolves a database-qualified name to a bare declaration",
			tables:    []goschema.Table{{StructName: "View", Name: "v"}},
			lookup:    "mydb.v",
			semantics: identifier.ForDialect("mysql"),
			wantNil:   false,
			wantName:  "v",
		},
		{
			// Two databases are two objects on MySQL exactly as two schemas are
			// on PostgreSQL, and neither side left anything unstated.
			name:      "MySQL declines two stated databases that differ",
			tables:    []goschema.Table{{StructName: "View", Name: "v", Schema: "db2"}},
			lookup:    "db1.v",
			semantics: identifier.ForDialect("mysql"),
			wantNil:   true,
		},
		{
			// SQLite carries a default schema, so `main` is reachable through
			// tier 2; a named non-default schema is not reachable at all.
			name:      "SQLite declines a named schema that is not the diff's",
			tables:    []goschema.Table{{StructName: "Note", Name: "notes", Schema: "attached"}},
			lookup:    "main.notes",
			semantics: identifier.ForDialect("sqlite"),
			wantNil:   true,
		},
		{
			// The capability tier 3 was introduced for (stokaro/ptah#1287): a
			// PostgreSQL reader qualifies every object outside the schema it
			// read, and the declaration left the schema to the search path.
			name:      "PostgreSQL still supplies a schema the declaration left unstated",
			tables:    []goschema.Table{{StructName: "User", Name: "users"}},
			lookup:    "app.users",
			semantics: identifier.ForDialect("postgres"),
			wantNil:   false,
			wantName:  "users",
		},
		{
			name:      "PostgreSQL still supplies a schema the diff left unstated",
			tables:    []goschema.Table{{StructName: "User", Name: "users", Schema: "app"}},
			lookup:    "users",
			semantics: identifier.ForDialect("postgres"),
			wantNil:   false,
			wantName:  "users",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got := objectlookup.Qualified(test.tables, test.lookup, test.semantics)
			c.Assert(got == nil, qt.Equals, test.wantNil, qt.Commentf("got: %+v", got))
			c.Assert(namedTable(got), qt.Equals, test.wantName)
		})
	}
}

// namedTable reports the table's own name, or the empty string for no table, so
// a row states its expectation without branching in the test body.
func namedTable(table *goschema.Table) string {
	if table == nil {
		return ""
	}
	return table.Name
}

// TestSame pins the two-reference form the collection-less sites use.
//
// It answers the same three tiers [objectlookup.Find] applies, so a site holding
// one name on each side -- a constraint's owning table against the table a
// TableDiff names -- asks the identity question rather than a string question.
func TestSame(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		reference string
		candidate string
		semantics identifier.Semantics
		want      bool
	}{
		{
			name:      "identical spellings",
			reference: "app.orders",
			candidate: "app.orders",
			semantics: identifier.ForDialect("mysql"),
			want:      true,
		},
		{
			name:      "the reference qualifies and the candidate does not",
			reference: "app.orders",
			candidate: "orders",
			semantics: identifier.ForDialect("mysql"),
			want:      true,
		},
		{
			name:      "the candidate qualifies and the reference does not",
			reference: "orders",
			candidate: "app.orders",
			semantics: identifier.ForDialect("mysql"),
			want:      true,
		},
		{
			name:      "PostgreSQL resolves a bare name against public",
			reference: "public.orders",
			candidate: "orders",
			semantics: identifier.ForDialect("postgres"),
			want:      true,
		},
		{
			name:      "SQLite folds ASCII case",
			reference: "Orders",
			candidate: "orders",
			semantics: identifier.ForDialect("sqlite"),
			want:      true,
		},
		{
			// The control: two stated schemas that differ are two objects, and
			// this is exactly the answer the planner acts on.
			name:      "two stated schemas that differ are not the same object",
			reference: "app.orders",
			candidate: "reporting.orders",
			semantics: identifier.ForDialect("mysql"),
			want:      false,
		},
		{
			name:      "different object names are not the same object",
			reference: "app.orders",
			candidate: "app.shipments",
			semantics: identifier.ForDialect("mysql"),
			want:      false,
		},
		{
			name:      "PostgreSQL keeps case-distinct names apart",
			reference: "public.Orders",
			candidate: "public.orders",
			semantics: identifier.ForDialect("postgres"),
			want:      false,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(objectlookup.Same(test.reference, test.candidate, test.semantics), qt.Equals, test.want)
		})
	}
}
