package postgres_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// typeScenario is one user-type change written twice: as the common
// comparison reports it, and as the type owner's comparison reports it.
type typeScenario struct {
	name   string
	common *difftypes.SchemaDiff
	owner  []schemaext.ChangeRecord
	// named is a statement fragment of the common script and the owner's
	// spelling of it. The owner names the CHECK an in-place change adds, so
	// the rollback can drop that constraint; the common path lets the server
	// name it.
	named [2]string
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

// TestPlanner_PlansOwnerTypesAsTheCommonPathDoes is the differential the flip
// of PostgreSQL user types to their owner is measured by: each scenario is
// planned once through the common user-type diff and once through the type
// owner's changes, and the two scripts must agree byte for byte, apart from
// the CHECK name a scenario's named states. The owner plans in the type
// windows the common steps leave empty, so a difference is a statement, an
// order or a note the owner writes differently.
func TestPlanner_PlansOwnerTypesAsTheCommonPathDoes(t *testing.T) {
	email := schemamodel.Domain{Name: "email", Schema: "app", BaseType: "text", NotNull: true, Default: "it's", Check: "VALUE <> ''",
		Comment: "mail"}
	addr := schemamodel.CompositeType{Name: "addr", Fields: []schemamodel.CompositeField{{Name: "street", Type: "text"}, {Name: "zip", Type: "int"}}}
	attributes := []pgtype.Attribute{{Name: "street", Type: "text"}, {Name: "zip", Type: "int"}}
	scenarios := []typeScenario{
		{
			name:   "creations",
			common: &difftypes.SchemaDiff{DomainsAdded: difftypes.DomainChanges{email}, CompositeTypesAdded: difftypes.CompositeTypeChanges{addr}},
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "app", "email", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text", NotNull: true,
					Default: "it's", Check: "VALUE <> ''", Comment: "mail"}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{After: &pgtype.DesiredComposite{Attributes: attributes}}),
			},
		},
		{
			name: "creations ordered by what they name",
			common: &difftypes.SchemaDiff{
				DomainsAdded:        difftypes.DomainChanges{{Name: "d", BaseType: "addr"}},
				CompositeTypesAdded: difftypes.CompositeTypeChanges{addr},
			},
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "", "d", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "addr"}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{After: &pgtype.DesiredComposite{Attributes: attributes}}),
			},
		},
		{
			name: "removals",
			common: &difftypes.SchemaDiff{
				DomainsRemoved:        difftypes.DomainChanges{{Name: "email", Schema: "app", BaseType: "text"}},
				CompositeTypesRemoved: difftypes.CompositeTypeChanges{addr},
			},
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "app", "email", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{Attributes: attributes}}),
			},
		},
		{
			name: "a domain changed in place",
			common: &difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{{
				DomainName: "app.email", CurrentBaseType: "text", CurrentCheckConstraints: []string{"email_check", "email_check1"},
				Changes: map[string]string{"check": "a -> b", "default": "a -> b", "not_null": "false -> true"}, Desired: email,
			}}},
			owner: []schemaext.ChangeRecord{typeRecord(pgtype.DomainKind, "app", "email", &pgtype.DomainChange{
				Before: &pgtype.ObservedDomain{BaseType: "text", Check: "(a) AND (b)", Checks: []pgtype.DomainCheck{
					{Name: "email_check", Expression: "a"}, {Name: "email_check1", Expression: "b"}}},
				After:   &pgtype.DesiredDomain{BaseType: "text", NotNull: true, Default: "it's", Check: "VALUE <> ''", Comment: "mail"},
				Differs: []string{pgtype.DiffCheck, pgtype.DiffDefault, pgtype.DiffNotNull}})},
			named: [2]string{`ADD CHECK`, `ADD CONSTRAINT "ptah_check" CHECK`},
		},
		{
			name: "a composite type changed in place",
			common: &difftypes.SchemaDiff{CompositeTypesModified: []difftypes.CompositeTypeDiff{{
				TypeName: "addr", Changes: map[string]string{"fields": "a -> b"}, CurrentFieldTypes: []string{"text", "text"},
				AttributesAdded: []difftypes.CompositeAttribute{{Name: "zip", Type: "int"}}, AttributesRemoved: []string{"city"}, Desired: addr,
			}}},
			owner: []schemaext.ChangeRecord{typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{
				Before:  &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "text"}, {Name: "city", Type: "text"}}},
				After:   &pgtype.DesiredComposite{Attributes: attributes},
				Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{{Name: "zip", Type: "int"}}, Removed: []string{"city"}})},
		},
		{
			name: "rebuilds",
			common: &difftypes.SchemaDiff{
				DomainsModified: []difftypes.DomainDiff{{DomainName: "app.email", CurrentBaseType: "varchar",
					Changes: map[string]string{"type": "varchar -> text"}, Desired: email}},
				CompositeTypesModified: []difftypes.CompositeTypeDiff{{TypeName: "addr", Changes: map[string]string{"fields": "a -> b"},
					CurrentFieldTypes: []string{"int", "text"}, Desired: addr}},
			},
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "app", "email", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "varchar"},
					After:   &pgtype.DesiredDomain{BaseType: "text", NotNull: true, Default: "it's", Check: "VALUE <> ''", Comment: "mail"},
					Differs: []string{pgtype.DiffBaseType}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{
					Before: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "zip", Type: "int"}, {Name: "street", Type: "text"}}},
					After:  &pgtype.DesiredComposite{Attributes: attributes}, Differs: []string{pgtype.DiffAttributes}}),
			},
		},
		{
			name: "a composite type with a delta over a rebuilt domain",
			common: &difftypes.SchemaDiff{
				DomainsModified: []difftypes.DomainDiff{{DomainName: "d", CurrentBaseType: "text", Changes: map[string]string{"type": "text -> int"},
					Desired: schemamodel.Domain{Name: "d", BaseType: "int"}}},
				CompositeTypesModified: []difftypes.CompositeTypeDiff{{TypeName: "addr", Changes: map[string]string{"fields": "a -> b"},
					CurrentFieldTypes: []string{"d"}, AttributesAdded: []difftypes.CompositeAttribute{{Name: "zip", Type: "int"}},
					Desired: schemamodel.CompositeType{Name: "addr", Fields: []schemamodel.CompositeField{{Name: "street", Type: "d"}, {Name: "zip", Type: "int"}}}}},
			},
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "", "d", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"},
					After: &pgtype.DesiredDomain{BaseType: "int"}, Differs: []string{pgtype.DiffBaseType}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{
					Before:  &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "d"}}},
					After:   &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "d"}, {Name: "zip", Type: "int"}}},
					Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{{Name: "zip", Type: "int"}}}),
			},
		},
		{
			name: "comments of surviving types",
			common: &difftypes.SchemaDiff{ObjectCommentsChanged: []difftypes.ObjectCommentChange{
				{Kind: difftypes.CommentedCompositeType, Name: "addr", Current: "old", Desired: "addresses"},
				{Kind: difftypes.CommentedDomain, Name: "app.email", Current: "old", Desired: "mail"},
			}},
			owner: []schemaext.ChangeRecord{
				typeRecord(pgtype.DomainKind, "app", "email", &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text", Comment: "old"},
					After: &pgtype.DesiredDomain{BaseType: "text", Comment: "mail"}, Differs: []string{pgtype.DiffComment}}),
				typeRecord(pgtype.CompositeKind, "", "addr", &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{Attributes: attributes, Comment: "old"},
					After: &pgtype.DesiredComposite{Attributes: attributes, Comment: "addresses"}, Differs: []string{pgtype.DiffComment}}),
			},
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			c := qt.New(t)

			common := renderedTypePlan(c, scenario.common)
			owned := renderedTypePlan(c, &difftypes.SchemaDiff{FeatureChanges: scenario.owner})

			c.Assert(owned, qt.Equals, strings.Replace(common, scenario.named[0], scenario.named[1], 1))
			c.Assert(owned, qt.Not(qt.Equals), "")
		})
	}
}
