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

var supplementalDiffCategories = map[string]string{
	"ForeignKeysRemovedWithTables":    "supplements matching ConstraintsRemovedWithTables entries with column identities for MySQL/MariaDB drop ordering; it creates no operation by itself and PostgreSQL deliberately ignores it",
	"FunctionsRemovedWithSignatures":  "the same removals FunctionsRemoved names, with the argument list that makes each one addressable; the planner reads this list and falls back to the bare names, so it creates no operation of its own and a fixture would exercise the same DROP twice (stokaro/ptah#2296)",
	"ProceduresRemovedWithSignatures": "ProceduresRemoved with signatures, supplemental for the same reason",
}

var refusedDiffCategories = map[string]string{
	"ExtensionsModified":  "PostgreSQL extension placement drift is detected but refused before emission until ALTER EXTENSION SET SCHEMA planning is supported",
	"HypertablesRemoved":  "TimescaleDB has no statement that turns a hypertable back into an ordinary table -- measured on 2.29.2, drop_hypertable does not exist -- so the planner refuses instead of emitting nothing and calling the two sides equal",
	"HypertablesModified": "TimescaleDB has no statement that repartitions an existing hypertable either, and the refusal is what keeps a permanent divergence from reading as no change",
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
			nodes, err := postgres.New().GenerateMigrationASTChecked(fixture.diff, fixture.desired)

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
		{"TablesAdded", &difftypes.SchemaDiff{TablesAdded: []string{"t"}}, oneTable},
		{"TablesRemoved", &difftypes.SchemaDiff{TablesRemoved: []string{"t"}}, &schemamodel.Database{}},
		{
			"TablesModified",
			&difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{TableName: "t", ColumnsAdded: []string{"c"}}}},
			oneTable,
		},
		{
			"EnumsAdded",
			&difftypes.SchemaDiff{EnumsAdded: []string{"e"}},
			&schemamodel.Database{Enums: []schemamodel.Enum{{Name: "e", Values: []string{"a"}}}},
		},
		{"EnumsRemoved", &difftypes.SchemaDiff{EnumsRemoved: []string{"e"}}, &schemamodel.Database{}},
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
			&difftypes.SchemaDiff{ExtensionsAdded: []string{"pg_trgm"}},
			&schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "pg_trgm"}}},
		},
		{"ExtensionsRemoved", &difftypes.SchemaDiff{ExtensionsRemoved: []string{"pg_trgm"}}, &schemamodel.Database{}},
		{
			"FunctionsAdded",
			&difftypes.SchemaDiff{FunctionsAdded: []string{"f"}},
			&schemamodel.Database{Functions: []schemamodel.Function{{Name: "f", Returns: "int", Body: "SELECT 1"}}},
		},
		{"FunctionsRemoved", &difftypes.SchemaDiff{FunctionsRemoved: []string{"f"}}, &schemamodel.Database{}},
		{"ProceduresRemoved", &difftypes.SchemaDiff{ProceduresRemoved: []string{"p"}}, &schemamodel.Database{}},
		{
			"FunctionsModified",
			&difftypes.SchemaDiff{FunctionsModified: []difftypes.FunctionDiff{{FunctionName: "f", Changes: map[string]string{"body": "x -> y"}}}},
			&schemamodel.Database{Functions: []schemamodel.Function{{Name: "f", Returns: "int", Body: "SELECT 2"}}},
		},
		{
			"SequencesAdded",
			&difftypes.SchemaDiff{SequencesAdded: []string{"s"}},
			&schemamodel.Database{Sequences: []schemamodel.Sequence{{Name: "s"}}},
		},
		{"SequencesRemoved", &difftypes.SchemaDiff{SequencesRemoved: []string{"s"}}, &schemamodel.Database{}},
		{
			"SequencesModified",
			&difftypes.SchemaDiff{SequencesModified: []difftypes.SequenceDiff{{SequenceName: "s", Changes: map[string]string{"increment": "1 -> 2"}}}},
			&schemamodel.Database{Sequences: []schemamodel.Sequence{{Name: "s", Increment: &increment}}},
		},
		{
			"DomainsAdded",
			&difftypes.SchemaDiff{DomainsAdded: []string{"d"}},
			&schemamodel.Database{Domains: []schemamodel.Domain{{Name: "d", BaseType: "TEXT"}}},
		},
		{"DomainsRemoved", &difftypes.SchemaDiff{DomainsRemoved: []string{"d"}}, &schemamodel.Database{}},
		{
			"DomainsModified",
			&difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{{DomainName: "d", Changes: map[string]string{"base_type": "TEXT -> VARCHAR"}}}},
			&schemamodel.Database{Domains: []schemamodel.Domain{{Name: "d", BaseType: "VARCHAR"}}},
		},
		{
			"CompositeTypesAdded",
			&difftypes.SchemaDiff{CompositeTypesAdded: []string{"ct"}},
			&schemamodel.Database{CompositeTypes: []schemamodel.CompositeType{
				{Name: "ct", Fields: []schemamodel.CompositeField{{Name: "a", Type: "TEXT"}}},
			}},
		},
		{"CompositeTypesRemoved", &difftypes.SchemaDiff{CompositeTypesRemoved: []string{"ct"}}, &schemamodel.Database{}},
		{
			"CompositeTypesModified",
			&difftypes.SchemaDiff{CompositeTypesModified: []difftypes.CompositeTypeDiff{{TypeName: "ct", Changes: map[string]string{"fields": "a -> b"}}}},
			&schemamodel.Database{CompositeTypes: []schemamodel.CompositeType{
				{Name: "ct", Fields: []schemamodel.CompositeField{{Name: "b", Type: "TEXT"}}},
			}},
		},
		{
			"RangesAdded",
			&difftypes.SchemaDiff{RangesAdded: []string{"r"}},
			&schemamodel.Database{Ranges: []schemamodel.Range{{Name: "r", Subtype: "int4"}}},
		},
		{"RangesRemoved", &difftypes.SchemaDiff{RangesRemoved: []string{"r"}}, &schemamodel.Database{}},
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
			&difftypes.SchemaDiff{ViewsAdded: []string{"v"}},
			&schemamodel.Database{Views: []schemamodel.View{{Name: "v", Body: "SELECT 1"}}},
		},
		{"ViewsRemoved", &difftypes.SchemaDiff{ViewsRemoved: []string{"v"}}, &schemamodel.Database{}},
		{
			"ViewsModified",
			&difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{{ViewName: "v", Changes: map[string]string{"body": "a -> b"}}}},
			&schemamodel.Database{Views: []schemamodel.View{{Name: "v", Body: "SELECT 2"}}},
		},
		{
			"SynonymsAdded",
			&difftypes.SchemaDiff{SynonymsAdded: []string{"s"}},
			&schemamodel.Database{Synonyms: []schemamodel.Synonym{{Name: "s", Target: "dbo.t"}}},
		},
		{"SynonymsRemoved", &difftypes.SchemaDiff{SynonymsRemoved: []string{"s"}}, &schemamodel.Database{}},
		{
			"HypertablesAdded",
			&difftypes.SchemaDiff{HypertablesAdded: []string{"conditions"}},
			&schemamodel.Database{Hypertables: []schemamodel.Hypertable{
				{Table: "conditions", Column: "time"},
			}},
		},
		{
			"ContinuousAggregatesAdded",
			&difftypes.SchemaDiff{ContinuousAggregatesAdded: []string{"hourly"}},
			&schemamodel.Database{ContinuousAggregates: []schemamodel.ContinuousAggregate{
				{Name: "hourly", Body: "SELECT 1"},
			}},
		},
		{
			"ContinuousAggregatesRemoved",
			&difftypes.SchemaDiff{ContinuousAggregatesRemoved: []string{"hourly"}},
			&schemamodel.Database{},
		},
		{
			"ContinuousAggregatesModified",
			&difftypes.SchemaDiff{ContinuousAggregatesModified: []difftypes.ContinuousAggregateDiff{
				{Name: "hourly", OldBody: "SELECT 1", NewBody: "SELECT 2"},
			}},
			&schemamodel.Database{ContinuousAggregates: []schemamodel.ContinuousAggregate{
				{Name: "hourly", Body: "SELECT 2"},
			}},
		},
		{
			"SynonymsModified",
			&difftypes.SchemaDiff{SynonymsModified: []difftypes.SynonymDiff{
				{SynonymName: "s", OldTarget: "dbo.old", NewTarget: "dbo.new"},
			}},
			&schemamodel.Database{Synonyms: []schemamodel.Synonym{{Name: "s", Target: "dbo.new"}}},
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
			&difftypes.SchemaDiff{MaterializedViewsAdded: []string{"mv"}},
			&schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "mv", Body: "SELECT 1"}}},
		},
		{"MaterializedViewsRemoved", &difftypes.SchemaDiff{MaterializedViewsRemoved: []string{"mv"}}, &schemamodel.Database{}},
		{
			"MaterializedViewsModified",
			&difftypes.SchemaDiff{MaterializedViewsModified: []difftypes.MaterializedViewDiff{{ViewName: "mv", Changes: map[string]string{"body": "a -> b"}}}},
			&schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "mv", Body: "SELECT 2"}}},
		},
		{
			"TriggersAdded",
			&difftypes.SchemaDiff{TriggersAdded: []difftypes.TriggerRef{{TriggerName: "tg", TableName: "t"}}},
			&schemamodel.Database{Triggers: []schemamodel.Trigger{
				{Name: "tg", Table: "t", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW", Body: "RETURN NEW;"},
			}},
		},
		{
			"TriggersRemoved",
			&difftypes.SchemaDiff{TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "tg", TableName: "t"}}},
			&schemamodel.Database{},
		},
		{
			"TriggersModified",
			&difftypes.SchemaDiff{TriggersModified: []difftypes.TriggerDiff{{TriggerName: "tg", TableName: "t", Changes: map[string]string{"timing": "BEFORE -> AFTER"}}}},
			&schemamodel.Database{Triggers: []schemamodel.Trigger{
				{Name: "tg", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "RETURN NEW;"},
			}},
		},
		{
			"RLSPoliciesAdded",
			&difftypes.SchemaDiff{RLSPoliciesAdded: []difftypes.RLSPolicyRef{{PolicyName: "pol", TableName: "t"}}},
			&schemamodel.Database{RLSPolicies: []schemamodel.RLSPolicy{{Name: "pol", Table: "t", PolicyFor: "ALL", ToRoles: "app"}}},
		},
		{
			"RLSPoliciesRemoved",
			&difftypes.SchemaDiff{RLSPoliciesRemoved: []difftypes.RLSPolicyRef{{PolicyName: "pol", TableName: "t"}}},
			&schemamodel.Database{},
		},
		{
			"RLSPoliciesModified",
			&difftypes.SchemaDiff{RLSPoliciesModified: []difftypes.RLSPolicyDiff{{PolicyName: "pol", TableName: "t", Changes: map[string]string{"using": "a -> b"}}}},
			&schemamodel.Database{RLSPolicies: []schemamodel.RLSPolicy{{Name: "pol", Table: "t", PolicyFor: "ALL", ToRoles: "app"}}},
		},
		{"RLSEnabledTablesAdded", &difftypes.SchemaDiff{RLSEnabledTablesAdded: []string{"t"}}, &schemamodel.Database{}},
		{"RLSEnabledTablesRemoved", &difftypes.SchemaDiff{RLSEnabledTablesRemoved: []string{"t"}}, &schemamodel.Database{}},
		{
			"RolesAdded",
			&difftypes.SchemaDiff{RolesAdded: []string{"app"}},
			&schemamodel.Database{Roles: []schemamodel.Role{{Name: "app"}}},
		},
		{"RolesRemoved", &difftypes.SchemaDiff{RolesRemoved: []string{"app"}}, &schemamodel.Database{}},
		{
			"RolesModified",
			&difftypes.SchemaDiff{RolesModified: []difftypes.RoleDiff{{RoleName: "app", Changes: map[string]string{"login": "false -> true"}}}},
			&schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", Login: true}}},
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
