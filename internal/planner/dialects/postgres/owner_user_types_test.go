package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// typeScenario is one user-type change as the type owner's comparison
// reports it, and the script the plan renders for it.
type typeScenario struct {
	name  string
	owner []schemaext.ChangeRecord
	want  string
}

// typeRecord is an owner change of the type of kind named schema.name.
func typeRecord(kind schemaext.Kind, schema, name string, value schemaext.ChangeValue) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: pgtype.Ref(kind, schema, name), Value: value}
}

// renderedTypePlan plans diff with the bundled runtime, which selects the type
// owner, and renders it for PostgreSQL.
func renderedTypePlan(c *qt.C, diff *difftypes.SchemaDiff) string {
	c.Helper()
	nodes, err := postgres.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// TestPlanner_PlansOwnerTypes pins the script the PostgreSQL planner writes
// for each type owner change, byte for byte. The owner plans in the type
// windows the planner keeps for it, so a difference is a statement, an order
// or a note the owner writes differently.
func TestPlanner_PlansOwnerTypes(t *testing.T) {
	attributes := []pgtype.Attribute{{Name: "street", Type: "text"}, {Name: "zip", Type: "int"}}
	scenarios := []typeScenario{
		{
			name: "creations",
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "app", "email", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text", NotNull: true,
					Default: "it's", Check: "VALUE <> ''", Comment: "mail"}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{After: &pgtype.DesiredComposite{Attributes: attributes}}),
			},
			want: "CREATE SCHEMA IF NOT EXISTS \"app\";\nCREATE DOMAIN \"app\".\"email\" AS text NOT NULL DEFAULT 'it''s' CHECK (VALUE <> '');\nCOMMENT ON DOMAIN \"app\".\"email\" IS 'mail';\nCREATE TYPE \"addr\" AS (\"street\" text, \"zip\" int);\n",
		},
		{
			name: "creations ordered by what they name",
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "", "d", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "addr"}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{After: &pgtype.DesiredComposite{Attributes: attributes}}),
			},
			want: "CREATE TYPE \"addr\" AS (\"street\" text, \"zip\" int);\nCREATE DOMAIN \"d\" AS addr;\n",
		},
		{
			name: "removals",
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "app", "email", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{Attributes: attributes}}),
			},
			want: "-- WARNING: Make sure no columns use this domain!\nDROP DOMAIN IF EXISTS \"app\".\"email\" CASCADE;\n-- WARNING: Make sure no columns use this composite type!\nDROP TYPE IF EXISTS \"addr\" CASCADE;\n",
		},
		{
			name: "a domain changed in place",
			owner: []schemaext.ChangeRecord{typeRecord(pgtype.DomainKind, "app", "email", &pgtype.DomainChange{
				Before: &pgtype.ObservedDomain{BaseType: "text", Check: "(a) AND (b)", Checks: []pgtype.DomainCheck{
					{Name: "email_check", Expression: "a"}, {Name: "email_check1", Expression: "b"}}},
				After:   &pgtype.DesiredDomain{BaseType: "text", NotNull: true, Default: "it's", Check: "VALUE <> ''", Comment: "mail"},
				Differs: []string{pgtype.DiffCheck, pgtype.DiffDefault, pgtype.DiffNotNull}})},
			want: "ALTER DOMAIN \"app\".\"email\" DROP CONSTRAINT \"email_check\";\nALTER DOMAIN \"app\".\"email\" DROP CONSTRAINT \"email_check1\";\nALTER DOMAIN \"app\".\"email\" ADD CONSTRAINT \"ptah_check\" CHECK (VALUE <> '');\nALTER DOMAIN \"app\".\"email\" SET DEFAULT 'it''s';\nALTER DOMAIN \"app\".\"email\" SET NOT NULL;\n",
		},
		{
			name: "a composite type changed in place",
			owner: []schemaext.ChangeRecord{typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{
				Before:  &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "text"}, {Name: "city", Type: "text"}}},
				After:   &pgtype.DesiredComposite{Attributes: attributes},
				Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{{Name: "zip", Type: "int"}}, Removed: []string{"city"}})},
			want: "ALTER TYPE \"addr\" DROP ATTRIBUTE \"city\";\nALTER TYPE \"addr\" ADD ATTRIBUTE \"zip\" int;\n",
		},
		{
			name: "rebuilds",
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "app", "email", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "varchar"},
					After:   &pgtype.DesiredDomain{BaseType: "text", NotNull: true, Default: "it's", Check: "VALUE <> ''", Comment: "mail"},
					Differs: []string{pgtype.DiffBaseType}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{
					Before: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "zip", Type: "int"}, {Name: "street", Type: "text"}}},
					After:  &pgtype.DesiredComposite{Attributes: attributes}, Differs: []string{pgtype.DiffAttributes}}),
			},
			want: "-- Recreate modified domain; drop is non-CASCADE and fails if the domain is in use\nDROP DOMAIN IF EXISTS \"app\".\"email\";\n-- Recreate modified composite type; drop is non-CASCADE and fails if the type is in use\nDROP TYPE IF EXISTS \"addr\";\nCREATE DOMAIN \"app\".\"email\" AS text NOT NULL DEFAULT 'it''s' CHECK (VALUE <> '');\nCOMMENT ON DOMAIN \"app\".\"email\" IS 'mail';\nCREATE TYPE \"addr\" AS (\"street\" text, \"zip\" int);\n",
		},
		{
			name: "a composite type with a delta over a rebuilt domain",
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "", "d", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"},
					After: &pgtype.DesiredDomain{BaseType: "int"}, Differs: []string{pgtype.DiffBaseType}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{
					Before:  &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "d"}}},
					After:   &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "d"}, {Name: "zip", Type: "int"}}},
					Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{{Name: "zip", Type: "int"}}}),
			},
			want: "-- Recreate modified composite type; drop is non-CASCADE and fails if the type is in use\nDROP TYPE IF EXISTS \"addr\";\n-- Recreate modified domain; drop is non-CASCADE and fails if the domain is in use\nDROP DOMAIN IF EXISTS \"d\";\nCREATE DOMAIN \"d\" AS int;\nCREATE TYPE \"addr\" AS (\"street\" d, \"zip\" int);\n",
		},
		{
			name: "comments of surviving types",
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "app", "email", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text", Comment: "old"},
					After: &pgtype.DesiredDomain{BaseType: "text", Comment: "mail"}, Differs: []string{pgtype.DiffComment}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{Attributes: attributes, Comment: "old"},
					After: &pgtype.DesiredComposite{Attributes: attributes, Comment: "addresses"}, Differs: []string{pgtype.DiffComment}}),
			},
			want: "COMMENT ON TYPE \"addr\" IS 'addresses';\nCOMMENT ON DOMAIN \"app\".\"email\" IS 'mail';\n",
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			c := qt.New(t)

			owned := renderedTypePlan(c, &difftypes.SchemaDiff{FeatureChanges: scenario.owner})

			c.Assert(owned, qt.Equals, scenario.want)
		})
	}
}
