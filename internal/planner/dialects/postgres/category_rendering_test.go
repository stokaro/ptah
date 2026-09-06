package postgres_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// categoryFixture is one schema diff category together with the desired schema
// the planner needs to emit SQL for it.
type categoryFixture struct {
	field   string
	diff    *difftypes.SchemaDiff
	desired *schemamodel.Database
}

// refusedFixture is one category the planner answers with an error, together
// with the reason it does.
type refusedFixture struct {
	why  string
	diff *difftypes.SchemaDiff
}

var supplementalDiffCategories = map[string]string{
	"DeclaredTables":                  "every table the declaration holds, carried so a foreign key can be resolved to the table it references -- usually one this diff does not touch. It is an INPUT to rendering rather than a change: on its own it creates no operation, and a fixture would assert that a list of tables plans nothing (stokaro/ptah#2315)",
	"DeclaredForeignKeys":             "every foreign key the schema the plan runs against holds, carried so the MySQL family can drop a column's keys before MODIFY and put them back. PostgreSQL changes a column type in place and touches no key doing it, so this planner reads the field nowhere; a fixture here would assert that a list of foreign keys plans nothing (stokaro/ptah#2315)",
	"DeclaredFunctions":               "the declaration order and call graph of the declared functions, carried so the additions can be ordered caller-after-callee. The bodies travel with the additions and it renders nothing of its own; TestPlanner_GenerateMigrationAST_OrdersFunctionsByDependencies drives it through FunctionsAdded, which is the only way it can be exercised (stokaro/ptah#2315)",
	"DeclaredTableDependencies":       "the table dependency graph, carried so the removals can be ordered child-before-parent. A creation carries its own edges and a removal is only a name, which is why this one is schema-wide; it renders nothing on its own, and a fixture would assert that a graph plans nothing (stokaro/ptah#2315)",
	"DeclaredUserTypes":               "the declaration's type vocabulary, which a created column's type is resolved THROUGH rather than rendered FROM. It creates no operation by itself: TestPlanner_CreatesAColumnTypedByADeclaredDomain drives it as part of a table creation, which is the only way it can be exercised (stokaro/ptah#2315)",
	"DeclaredSchemas":                 "every schema the declaration holds, carried so a CREATE SCHEMA the plan emits can carry the comment, character set and collation the author wrote for it. The plan reaches a schema through an object's qualifier, so the name arrives and nothing else does; on its own the list plans nothing, and TestGenerateSchemaDiffSQLStatements_ACreatedSchemaCarriesItsComment drives it through a table that names a schema, which is the only way it can be exercised (stokaro/ptah#2618)",
	"DeclaredViewLikes":               "every declared view and materialized view, which a cascading DROP is resolved AGAINST rather than rendered from. The recreate it feeds belongs to the drop that cascaded, and several fixtures below carry it for exactly that reason; on its own it plans nothing (stokaro/ptah#2315)",
	"DeclaredConstraintHosts":         "the declaration of every table a constraint change names, carried for a target that has to rebuild the table to change a constraint on it. PostgreSQL adds and drops constraints in place and never rebuilds, so this planner reads the field nowhere; a fixture here would assert that a list of table declarations plans nothing (stokaro/ptah#2315)",
	"ForeignKeysRemovedWithTables":    "supplements matching ConstraintsRemoved entries with column identities for MySQL/MariaDB drop ordering; it creates no operation by itself and PostgreSQL deliberately ignores it",
	"FunctionsRemovedWithSignatures":  "the same removals FunctionsRemoved names, with the argument list that makes each one addressable; the planner reads this list and falls back to the bare names, so it creates no operation of its own and a fixture would exercise the same DROP twice (stokaro/ptah#2296)",
	"ProceduresRemovedWithSignatures": "ProceduresRemoved with signatures, supplemental for the same reason",
}

// refusedDiffCategories are the categories the planner answers with an error
// rather than with SQL. Each carries a fixture, and
// TestEveryRefusedDiffCategoryIsRefused drives it: an exemption that only
// removed a category from the walk would let a category stop being refused
// without anything noticing, which is the failure this whole file exists to
// prevent one level down.
var refusedDiffCategories = map[string]refusedFixture{
	"HypertablesRemoved": {
		why: "TimescaleDB has no statement that turns a hypertable back into an ordinary table -- measured on 2.29.2, drop_hypertable does not exist -- so the planner refuses instead of emitting nothing and calling the two sides equal",
		diff: &difftypes.SchemaDiff{HypertablesRemoved: difftypes.HypertableChanges{{
			Table: "readings", Column: "ts",
		}}},
	},
	"HypertablesModified": {
		why: "TimescaleDB has no statement that repartitions an existing hypertable either, and the refusal is what keeps a permanent divergence from reading as no change",
		diff: &difftypes.SchemaDiff{HypertablesModified: []difftypes.HypertableDiff{{
			Table: "readings", OldColumn: "ts", NewColumn: "created_at",
		}}},
	},
	"RLSPolicyIdentityConflicts": {
		why: "two declared policies that resolve to one identity cannot be planned: the comparison already reduced them to one entry, so applying it would apply whichever the map kept (stokaro/ptah#2440)",
		diff: &difftypes.SchemaDiff{RLSPolicyIdentityConflicts: []difftypes.RLSPolicyConflict{{
			First:  schemamodel.RLSPolicy{Name: "tenant", Table: "orders", PolicyFor: "ALL", UsingExpression: "a = 1"},
			Second: schemamodel.RLSPolicy{Name: "tenant", Table: "public.orders", PolicyFor: "ALL", UsingExpression: "b = 2"},
		}}},
	},
}

// TestEveryRefusedDiffCategoryIsRefused is the control for the exemption above.
//
// Without it the map is a list of categories nothing checks, and a planner that
// quietly started rendering one -- or silently ignoring it -- would pass. Each
// row asserts the error, not merely that no SQL came out: a category that
// produced neither would satisfy "did not render" while doing exactly the thing
// the refusal exists to prevent.
func TestEveryRefusedDiffCategoryIsRefused(t *testing.T) {
	for field, fixture := range refusedDiffCategories {
		t.Run(field, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := postgres.New().GenerateMigrationAST(fixture.diff)

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s: %s", field, fixture.why))
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestEveryDiffCategoryRendersSQL walks the change categories of SchemaDiff and
// asserts the PostgreSQL planner emits at least one node for each.
//
// The walk is reflection over the struct rather than a list written here, which
// is the point: stokaro/ptah#1284 was two categories the comparator filled and
// no planner path read, and a list maintained by hand is what let them sit
// unnoticed. A category added to the diff model without a fixture below fails
// this test, and a fixture whose category the planner drops fails it too.
func TestEveryDiffCategoryRendersSQL(t *testing.T) {
	c := qt.New(t)

	fixtures := diffCategoryFixtures()
	c.Assert(uncoveredDiffCategories(fixtures), qt.HasLen, 0)

	for _, fixture := range fixtures {
		t.Run(fixture.field, func(t *testing.T) {
			c := qt.New(t)
			// Each fixture states a diff and the declaration it came from,
			// so the carries a comparison would have filled are filled here
			// rather than in every literal (stokaro/ptah#2315).
			nodes, err := postgres.New().GenerateMigrationAST(withDeclaredObjects(fixture.diff, fixture.desired))

			c.Assert(err, qt.IsNil)
			c.Assert(len(nodes) > 0, qt.IsTrue, qt.Commentf("the planner rendered nothing for %s", fixture.field))
		})
	}
}

// uncoveredDiffCategories returns the SchemaDiff change categories that no
// fixture exercises.
func uncoveredDiffCategories(fixtures []categoryFixture) []string {
	covered := make(map[string]bool, len(fixtures))
	for _, fixture := range fixtures {
		covered[fixture.field] = true
	}

	structType := reflect.TypeFor[difftypes.SchemaDiff]()
	var uncovered []string
	for field := range structType.Fields() {
		// Every exported field but the semantics snapshot, which is a pointer
		// to the rules the diff was produced under rather than a difference.
		//
		// This walked SLICE fields alone until stokaro/ptah#2315, so a category
		// spelled as a struct or a map was skipped without being exempt --
		// which is a gate reporting on a set it chose not to look at. Widening
		// it named exactly the three carries below and nothing else.
		if !field.IsExported() || field.Type.Kind() == reflect.Pointer {
			continue
		}
		if covered[field.Name] {
			continue
		}
		if _, supplemental := supplementalDiffCategories[field.Name]; supplemental {
			continue
		}
		if _, refused := refusedDiffCategories[field.Name]; refused {
			continue
		}
		uncovered = append(uncovered, field.Name)
	}
	return uncovered
}

func TestSupplementalDiffCategoriesAreDocumented(t *testing.T) {
	c := qt.New(t)

	c.Assert(supplementalDiffCategories["ForeignKeysRemovedWithTables"], qt.Not(qt.Equals), "")
}

func TestRefusedDiffCategoriesAreDocumented(t *testing.T) {
	c := qt.New(t)
	c.Assert(refusedDiffCategories["ExtensionsModified"], qt.Not(qt.Equals), "")
}

func diffCategoryFixtures() []categoryFixture {
	oneTable := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "t", StructName: "T"}},
		Fields: []schemamodel.Field{{Name: "c", StructName: "T", Type: "TEXT"}},
	}
	increment := int64(2)

	return []categoryFixture{
		{"TablesAdded", &difftypes.SchemaDiff{TablesAdded: difftypes.TableCreationsFor(oneTable, "t")}, oneTable},
		{"TablesRemoved", &difftypes.SchemaDiff{TablesRemoved: []string{"t"}}, &schemamodel.Database{}},
		{
			"TablesModified",
			&difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{TableName: "t", ColumnsAdded: difftypes.ColumnChanges{{Name: "c", StructName: "T", Type: "TEXT"}}}},
				// The declared tables the planner checks the change against: a
				// modification naming a table the declaration does not hold writes
				// no DDL, so a fixture that carried none would assert that this
				// category plans nothing.
				DeclaredTables: oneTable.Tables,
			},
			oneTable,
		},
		{
			"EnumsAdded",
			&difftypes.SchemaDiff{EnumsAdded: difftypes.EnumChanges{{Name: "e", Values: []string{"a", "b"}}}},
			&schemamodel.Database{Enums: []schemamodel.Enum{{Name: "e", Values: []string{"a"}}}},
		},
		{"EnumsRemoved", &difftypes.SchemaDiff{EnumsRemoved: difftypes.EnumChanges{{Name: "e"}}}, &schemamodel.Database{}},
		{
			"EnumsModified",
			&difftypes.SchemaDiff{EnumsModified: []difftypes.EnumDiff{{EnumName: "e", ValuesAdded: []string{"b"}}}},
			&schemamodel.Database{Enums: []schemamodel.Enum{{Name: "e", Values: []string{"a", "b"}}}},
		},
		{
			"IndexesAdded",
			&difftypes.SchemaDiff{IndexesAdded: difftypes.IndexChanges{{
				Index:     schemamodel.Index{Name: "ix", StructName: "T", Fields: []string{"c"}},
				TableName: "t",
			}}},
			&schemamodel.Database{
				Tables:  []schemamodel.Table{{Name: "t", StructName: "T"}},
				Indexes: []schemamodel.Index{{Name: "ix", StructName: "T", Fields: []string{"c"}}},
			},
		},
		{
			"IndexesRemoved",
			&difftypes.SchemaDiff{IndexesRemoved: []difftypes.IndexRef{{Name: "ix", TableName: "t"}}},
			&schemamodel.Database{},
		},
		{
			// A subset of IndexesRemoved, so the fixture carries both lists:
			// the marker changes the spelling of a drop the removal list asks
			// for, and on its own it asks for nothing.
			"ConstraintBackedIndexRemovals",
			&difftypes.SchemaDiff{
				IndexesRemoved:                []difftypes.IndexRef{{Name: "uq", TableName: "t"}},
				ConstraintBackedIndexRemovals: []difftypes.IndexRef{{Name: "uq", TableName: "t"}},
			},
			&schemamodel.Database{},
		},
		{
			"ExtensionsAdded",
			&difftypes.SchemaDiff{ExtensionsAdded: difftypes.ExtensionChanges{{Name: "pg_trgm"}}},
			&schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "pg_trgm"}}},
		},
		{"ExtensionsRemoved", &difftypes.SchemaDiff{ExtensionsRemoved: difftypes.ExtensionChanges{{Name: "pg_trgm"}}}, &schemamodel.Database{}},
		{
			"FunctionsAdded",
			&difftypes.SchemaDiff{FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "f"}}}},
			&schemamodel.Database{Functions: []schemamodel.Function{{Name: "f", Returns: "int", Body: "SELECT 1"}}},
		},
		{"FunctionsRemoved", &difftypes.SchemaDiff{FunctionsRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "f"}}}}, &schemamodel.Database{}},
		{"ProceduresRemoved", &difftypes.SchemaDiff{ProceduresRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "p"}}}}, &schemamodel.Database{}},
		{
			"FunctionsModified",
			&difftypes.SchemaDiff{FunctionsModified: []difftypes.FunctionDiff{{
				FunctionName: "f",
				Changes:      map[string]string{"body": "x -> y"},
				Desired:      schemamodel.Function{Name: "f", Returns: "int", Body: "SELECT 2"},
			}}},
			&schemamodel.Database{},
		},
		{
			"SequencesAdded",
			&difftypes.SchemaDiff{SequencesAdded: difftypes.SequenceChanges{{Name: "s"}}},
			&schemamodel.Database{Sequences: []schemamodel.Sequence{{Name: "s"}}},
		},
		{"SequencesRemoved", &difftypes.SchemaDiff{SequencesRemoved: difftypes.SequenceChanges{{Name: "s"}}}, &schemamodel.Database{}},
		{
			"SequencesModified",
			&difftypes.SchemaDiff{SequencesModified: []difftypes.SequenceDiff{{
				SequenceName: "s",
				Changes:      map[string]string{"increment": "1 -> 2"},
				Desired:      schemamodel.Sequence{Name: "s", Increment: &increment},
			}}},
			&schemamodel.Database{},
		},
		{
			"DomainsAdded",
			&difftypes.SchemaDiff{DomainsAdded: difftypes.DomainChanges{{Name: "d"}}},
			&schemamodel.Database{Domains: []schemamodel.Domain{{Name: "d", BaseType: "TEXT"}}},
		},
		{"DomainsRemoved", &difftypes.SchemaDiff{DomainsRemoved: difftypes.DomainChanges{{Name: "d"}}}, &schemamodel.Database{}},
		{
			"DomainsModified",
			&difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{{DomainName: "d", Changes: map[string]string{"base_type": "TEXT -> VARCHAR"}}}},
			&schemamodel.Database{Domains: []schemamodel.Domain{{Name: "d", BaseType: "VARCHAR"}}},
		},
		{
			"CompositeTypesAdded",
			&difftypes.SchemaDiff{CompositeTypesAdded: difftypes.CompositeTypeChanges{{Name: "ct"}}},
			&schemamodel.Database{CompositeTypes: []schemamodel.CompositeType{
				{Name: "ct", Fields: []schemamodel.CompositeField{{Name: "a", Type: "TEXT"}}},
			}},
		},
		{"CompositeTypesRemoved", &difftypes.SchemaDiff{CompositeTypesRemoved: difftypes.CompositeTypeChanges{{Name: "ct"}}}, &schemamodel.Database{}},
		{
			"CompositeTypesModified",
			&difftypes.SchemaDiff{CompositeTypesModified: []difftypes.CompositeTypeDiff{{TypeName: "ct", Changes: map[string]string{"fields": "a -> b"}}}},
			&schemamodel.Database{CompositeTypes: []schemamodel.CompositeType{
				{Name: "ct", Fields: []schemamodel.CompositeField{{Name: "b", Type: "TEXT"}}},
			}},
		},
		{
			"RangesAdded",
			&difftypes.SchemaDiff{RangesAdded: difftypes.RangeChanges{{Name: "r"}}},
			&schemamodel.Database{Ranges: []schemamodel.Range{{Name: "r", Subtype: "int4"}}},
		},
		{"RangesRemoved", &difftypes.SchemaDiff{RangesRemoved: difftypes.RangeChanges{{Name: "r"}}}, &schemamodel.Database{}},
		{
			"RangesModified",
			&difftypes.SchemaDiff{RangesModified: []difftypes.RangeDiff{{
				RangeName:      "r",
				Changes:        map[string]string{"subtype": "timestamptz -> int8"},
				CurrentSubtype: "timestamptz",
			}}},
			&schemamodel.Database{Ranges: []schemamodel.Range{{Name: "r", Subtype: "int8"}}},
		},
		{
			"ViewsAdded",
			&difftypes.SchemaDiff{
				ViewsAdded: difftypes.ViewChanges{{Name: "v"}},
				// The declared view-like objects a comparison fills: rendering a view
				// back reads its body from here, so a fixture carrying none would
				// assert that this category plans nothing.
				DeclaredViewLikes: difftypes.ViewLikeVocabulary{
					Views: []schemamodel.View{{Name: "v", Body: "SELECT 1"}},
				},
			},
			&schemamodel.Database{Views: []schemamodel.View{{Name: "v", Body: "SELECT 1"}}},
		},
		{"ViewsRemoved", &difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "v"}}}, &schemamodel.Database{}},
		{
			"ViewsModified",
			&difftypes.SchemaDiff{
				ViewsModified: []difftypes.ViewDiff{{
					ViewName: "v",
					Desired:  schemamodel.View{Name: "v", Body: "SELECT 1"},
					Changes:  map[string]string{"body": "a -> b"},
				}},
				// A modification that drops and recreates resolves the recreate
				// through the declared set, so the row carries it.
				DeclaredViewLikes: difftypes.ViewLikeVocabulary{
					Views: []schemamodel.View{{Name: "v", Body: "SELECT 1"}},
				},
			},
			&schemamodel.Database{Views: []schemamodel.View{{Name: "v", Body: "SELECT 2"}}},
		},
		{
			"SynonymsAdded",
			&difftypes.SchemaDiff{SynonymsAdded: difftypes.SynonymChanges{{Name: "s"}}},
			&schemamodel.Database{Synonyms: []schemamodel.Synonym{{Name: "s", Target: "dbo.t"}}},
		},
		{"SynonymsRemoved", &difftypes.SchemaDiff{SynonymsRemoved: difftypes.SynonymChanges{{Name: "s"}}}, &schemamodel.Database{}},
		{
			"HypertablesAdded",
			&difftypes.SchemaDiff{HypertablesAdded: difftypes.HypertableChanges{{Table: "conditions"}}},
			&schemamodel.Database{Hypertables: []schemamodel.Hypertable{
				{Table: "conditions", Column: "time"},
			}},
		},
		{
			"ContinuousAggregatesAdded",
			&difftypes.SchemaDiff{ContinuousAggregatesAdded: difftypes.ContinuousAggregateChanges{{Name: "hourly"}}},
			&schemamodel.Database{ContinuousAggregates: []schemamodel.ContinuousAggregate{
				{Name: "hourly", Body: "SELECT 1"},
			}},
		},
		{
			"ContinuousAggregatesRemoved",
			&difftypes.SchemaDiff{ContinuousAggregatesRemoved: difftypes.ContinuousAggregateChanges{{Name: "hourly"}}},
			&schemamodel.Database{},
		},
		{
			"ContinuousAggregatesModified",
			&difftypes.SchemaDiff{ContinuousAggregatesModified: []difftypes.ContinuousAggregateDiff{
				{
					Name: "hourly", OldBody: "SELECT 1", NewBody: "SELECT 2",
					Desired: schemamodel.ContinuousAggregate{Name: "hourly", Body: "SELECT 2"},
				},
			}},
			&schemamodel.Database{},
		},
		{
			"SynonymsModified",
			&difftypes.SchemaDiff{SynonymsModified: []difftypes.SynonymDiff{{
				SynonymName: "s", OldTarget: "dbo.old", NewTarget: "dbo.new",
				Desired: schemamodel.Synonym{Name: "s", Target: "dbo.new"},
			}}},
			&schemamodel.Database{},
		},
		{
			"ExtendedPropertiesAdded",
			&difftypes.SchemaDiff{ExtendedPropertiesAdded: []difftypes.ExtendedPropertyRef{
				{Name: "ptah_flag", Schema: "app", Table: "docs", Value: "enabled"},
			}},
			&schemamodel.Database{},
		},
		{
			"ExtendedPropertiesRemoved",
			&difftypes.SchemaDiff{ExtendedPropertiesRemoved: []difftypes.ExtendedPropertyRef{
				{Name: "ptah_flag", Schema: "app", Table: "docs", Value: "enabled"},
			}},
			&schemamodel.Database{},
		},
		{
			"ExtendedPropertiesModified",
			&difftypes.SchemaDiff{ExtendedPropertiesModified: []difftypes.ExtendedPropertyDiff{{
				ExtendedPropertyRef: difftypes.ExtendedPropertyRef{
					Name: "ptah_flag", Schema: "app", Table: "docs", Value: "disabled",
				},
				OldValue: "enabled",
			}}},
			&schemamodel.Database{},
		},
		{
			"MaterializedViewsAdded",
			&difftypes.SchemaDiff{
				MaterializedViewsAdded: difftypes.MaterializedViewChanges{{Name: "mv"}},
				DeclaredViewLikes: difftypes.ViewLikeVocabulary{
					MaterializedViews: []schemamodel.MaterializedView{{Name: "mv", Body: "SELECT 1"}},
				},
			},
			&schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "mv", Body: "SELECT 1"}}},
		},
		{"MaterializedViewsRemoved", &difftypes.SchemaDiff{MaterializedViewsRemoved: difftypes.MaterializedViewChanges{{Name: "mv"}}}, &schemamodel.Database{}},
		{
			"MaterializedViewsModified",
			&difftypes.SchemaDiff{MaterializedViewsModified: []difftypes.MaterializedViewDiff{{
				ViewName: "mv",
				Changes:  map[string]string{"body": "a -> b"},
				Desired:  schemamodel.MaterializedView{Name: "mv", Body: "SELECT 2"},
			}}},
			&schemamodel.Database{},
		},
		{
			"TriggersAdded",
			&difftypes.SchemaDiff{TriggersAdded: []difftypes.TriggerRef{{
				TriggerName: "tg", TableName: "t",
				Desired: schemamodel.Trigger{
					Name: "tg", Table: "t", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW", Body: "RETURN NEW;",
				},
			}}},
			&schemamodel.Database{},
		},
		{
			"TriggersRemoved",
			&difftypes.SchemaDiff{TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "tg", TableName: "t"}}},
			&schemamodel.Database{},
		},
		{
			"TriggersModified",
			&difftypes.SchemaDiff{TriggersModified: []difftypes.TriggerDiff{{
				TriggerName: "tg", TableName: "t",
				Changes: map[string]string{"timing": "BEFORE -> AFTER"},
				Desired: schemamodel.Trigger{
					Name: "tg", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "RETURN NEW;",
				},
			}}},
			&schemamodel.Database{},
		},
		{
			// This row was exempt as "refused before emission until ALTER
			// EXTENSION SET SCHEMA planning is supported" until
			// TestEveryRefusedDiffCategoryIsRefused asked whether it still was.
			// stokaro/ptah#1718 replaced that blanket refusal with real
			// planning, and the exemption outlived it -- so the category had
			// been out of this walk ever since, which is the state this file
			// exists to prevent.
			"ExtensionsModified",
			&difftypes.SchemaDiff{ExtensionsModified: []difftypes.ExtensionDiff{{
				Name: "pg_trgm", FromSchema: "public", ToSchema: "extensions", Relocatable: true,
			}}},
			&schemamodel.Database{},
		},
		{
			"RLSPoliciesAdded",
			&difftypes.SchemaDiff{RLSPoliciesAdded: []difftypes.RLSPolicyRef{{
				PolicyName: "pol", TableName: "t",
				Desired: schemamodel.RLSPolicy{Name: "pol", Table: "t", PolicyFor: "ALL", ToRoles: "app"},
			}}},
			&schemamodel.Database{},
		},
		{
			"RLSPoliciesRemoved",
			&difftypes.SchemaDiff{RLSPoliciesRemoved: []difftypes.RLSPolicyRef{{PolicyName: "pol", TableName: "t"}}},
			&schemamodel.Database{},
		},
		{
			"RLSPoliciesModified",
			&difftypes.SchemaDiff{RLSPoliciesModified: []difftypes.RLSPolicyDiff{{
				PolicyName: "pol", TableName: "t",
				Changes: map[string]string{"using": "a -> b"},
				Desired: schemamodel.RLSPolicy{Name: "pol", Table: "t", PolicyFor: "ALL", ToRoles: "app"},
			}}},
			&schemamodel.Database{},
		},
		{"RLSEnabledTablesAdded", &difftypes.SchemaDiff{RLSEnabledTablesAdded: difftypes.RLSEnabledTableChanges{{Table: "t"}}}, &schemamodel.Database{}},
		{"RLSEnabledTablesRemoved", &difftypes.SchemaDiff{RLSEnabledTablesRemoved: difftypes.RLSEnabledTableChanges{{Table: "t"}}}, &schemamodel.Database{}},
		{
			"RolesAdded",
			&difftypes.SchemaDiff{RolesAdded: difftypes.RoleChanges{{Name: "app"}}},
			&schemamodel.Database{Roles: []schemamodel.Role{{Name: "app"}}},
		},
		{"RolesRemoved", &difftypes.SchemaDiff{RolesRemoved: difftypes.RoleChanges{{Name: "app"}}}, &schemamodel.Database{}},
		{
			"RolesModified",
			&difftypes.SchemaDiff{RolesModified: []difftypes.RoleDiff{{
				RoleName: "app",
				Changes:  map[string]string{"login": "false -> true"},
				Desired:  schemamodel.Role{Name: "app", Login: true},
			}}},
			&schemamodel.Database{},
		},
		{
			"GrantsAdded",
			&difftypes.SchemaDiff{GrantsAdded: []difftypes.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"}}},
			&schemamodel.Database{},
		},
		{
			"GrantsRemoved",
			&difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"}}},
			&schemamodel.Database{},
		},
		{
			"GrantOptionsAdded",
			&difftypes.SchemaDiff{GrantOptionsAdded: []difftypes.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", WithOption: true}}},
			&schemamodel.Database{},
		},
		{
			"GrantOptionsRevoked",
			&difftypes.SchemaDiff{GrantOptionsRevoked: []difftypes.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", WithOption: true}}},
			&schemamodel.Database{},
		},
		{
			"ConstraintsAdded",
			&difftypes.SchemaDiff{
				// The record a comparison carries. A name with no definition is
				// refused, so a row carrying only the name would assert that this
				// category cannot be planned (stokaro/ptah#2315).
				ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
					Name: "uq", TableName: "t", Type: "UNIQUE", Columns: []string{"c"},
				}},
			},
			&schemamodel.Database{
				Tables:      []schemamodel.Table{{Name: "t", StructName: "T"}},
				Constraints: []schemamodel.Constraint{{Name: "uq", StructName: "T", Type: "UNIQUE", Columns: []string{"c"}}},
			},
		},
		{
			"ConstraintsAdded",
			&difftypes.SchemaDiff{ConstraintsAdded: []difftypes.ConstraintAdditionInfo{
				{Name: "uq", TableName: "t", Type: "UNIQUE", Columns: []string{"c"}},
			}},
			&schemamodel.Database{},
		},
		{"ConstraintsRemoved", &difftypes.SchemaDiff{ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "uq"}}}, &schemamodel.Database{}},
		{
			"ConstraintsRemoved",
			&difftypes.SchemaDiff{ConstraintsRemoved: []difftypes.ConstraintRemovalInfo{
				{Name: "uq", TableName: "t", Type: "UNIQUE"},
			}},
			&schemamodel.Database{},
		},
	}
}
