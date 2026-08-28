package postgres_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
	"ForeignKeysRemovedWithTables":    "supplements matching ConstraintsRemovedWithTables entries with column identities for MySQL/MariaDB drop ordering; it creates no operation by itself and PostgreSQL deliberately ignores it",
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

			nodes, err := postgres.New().GenerateMigrationAST(fixture.diff, &schemamodel.Database{})

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
			nodes, err := postgres.New().GenerateMigrationAST(fixture.diff, fixture.desired)

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
		if !field.IsExported() || field.Type.Kind() != reflect.Slice {
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
		{"TablesAdded", &difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{Name: "t"}}}, oneTable},
		{"TablesRemoved", &difftypes.SchemaDiff{TablesRemoved: []string{"t"}}, &schemamodel.Database{}},
		{
			"TablesModified",
			&difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{TableName: "t", ColumnsAdded: difftypes.ColumnChanges{{Name: "c", StructName: "T", Type: "TEXT"}}}}},
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
			&difftypes.SchemaDiff{IndexesAdded: []difftypes.IndexRef{{Name: "ix", TableName: "t"}}},
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
			&difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "v"}}},
			&schemamodel.Database{Views: []schemamodel.View{{Name: "v", Body: "SELECT 1"}}},
		},
		{"ViewsRemoved", &difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "v"}}}, &schemamodel.Database{}},
		{
			"ViewsModified",
			&difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{{
				ViewName: "v",
				Desired:  schemamodel.View{Name: "v", Body: "SELECT 1"},
				Changes:  map[string]string{"body": "a -> b"},
			}}},
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
			&difftypes.SchemaDiff{MaterializedViewsAdded: difftypes.MaterializedViewChanges{{Name: "mv"}}},
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
			&difftypes.SchemaDiff{ConstraintsAdded: []string{"uq"}},
			&schemamodel.Database{
				Tables:      []schemamodel.Table{{Name: "t", StructName: "T"}},
				Constraints: []schemamodel.Constraint{{Name: "uq", StructName: "T", Type: "UNIQUE", Columns: []string{"c"}}},
			},
		},
		{
			"ConstraintsAddedWithTables",
			&difftypes.SchemaDiff{ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
				{Name: "uq", TableName: "t", Type: "UNIQUE", Columns: []string{"c"}},
			}},
			&schemamodel.Database{},
		},
		{"ConstraintsRemoved", &difftypes.SchemaDiff{ConstraintsRemoved: []string{"uq"}}, &schemamodel.Database{}},
		{
			"ConstraintsRemovedWithTables",
			&difftypes.SchemaDiff{ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
				{Name: "uq", TableName: "t", Type: "UNIQUE"},
			}},
			&schemamodel.Database{},
		},
	}
}
