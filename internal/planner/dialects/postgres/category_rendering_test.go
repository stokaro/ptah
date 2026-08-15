package postgres_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// categoryFixture is one schema diff category together with the desired schema
// the planner needs to emit SQL for it.
type categoryFixture struct {
	field     string
	diff      *types.SchemaDiff
	generated *goschema.Database
}

var supplementalDiffCategories = map[string]string{
	"ForeignKeysRemovedWithTables": "supplements matching ConstraintsRemovedWithTables entries with column identities for MySQL/MariaDB drop ordering; it creates no operation by itself and PostgreSQL deliberately ignores it",
}

var refusedDiffCategories = map[string]string{
	"ExtensionsModified": "PostgreSQL extension placement drift is detected but refused before emission until ALTER EXTENSION SET SCHEMA planning is supported",
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
			nodes, err := postgres.New().GenerateMigrationASTChecked(fixture.diff, fixture.generated)

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

	structType := reflect.TypeFor[types.SchemaDiff]()
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
	oneTable := &goschema.Database{
		Tables: []goschema.Table{{Name: "t", StructName: "T"}},
		Fields: []goschema.Field{{Name: "c", StructName: "T", Type: "TEXT"}},
	}
	increment := int64(2)

	return []categoryFixture{
		{"TablesAdded", &types.SchemaDiff{TablesAdded: []string{"t"}}, oneTable},
		{"TablesRemoved", &types.SchemaDiff{TablesRemoved: []string{"t"}}, &goschema.Database{}},
		{
			"TablesModified",
			&types.SchemaDiff{TablesModified: []types.TableDiff{{TableName: "t", ColumnsAdded: []string{"c"}}}},
			oneTable,
		},
		{
			"EnumsAdded",
			&types.SchemaDiff{EnumsAdded: []string{"e"}},
			&goschema.Database{Enums: []goschema.Enum{{Name: "e", Values: []string{"a"}}}},
		},
		{"EnumsRemoved", &types.SchemaDiff{EnumsRemoved: []string{"e"}}, &goschema.Database{}},
		{
			"EnumsModified",
			&types.SchemaDiff{EnumsModified: []types.EnumDiff{{EnumName: "e", ValuesAdded: []string{"b"}}}},
			&goschema.Database{Enums: []goschema.Enum{{Name: "e", Values: []string{"a", "b"}}}},
		},
		{
			"IndexesAdded",
			&types.SchemaDiff{IndexesAdded: []types.IndexRef{{Name: "ix", TableName: "t"}}},
			&goschema.Database{
				Tables:  []goschema.Table{{Name: "t", StructName: "T"}},
				Indexes: []goschema.Index{{Name: "ix", StructName: "T", Fields: []string{"c"}}},
			},
		},
		{
			"IndexesRemoved",
			&types.SchemaDiff{IndexesRemoved: []types.IndexRef{{Name: "ix", TableName: "t"}}},
			&goschema.Database{},
		},
		{
			// A subset of IndexesRemoved, so the fixture carries both lists:
			// the marker changes the spelling of a drop the removal list asks
			// for, and on its own it asks for nothing.
			"ConstraintBackedIndexRemovals",
			&types.SchemaDiff{
				IndexesRemoved:                []types.IndexRef{{Name: "uq", TableName: "t"}},
				ConstraintBackedIndexRemovals: []types.IndexRef{{Name: "uq", TableName: "t"}},
			},
			&goschema.Database{},
		},
		{
			"ExtensionsAdded",
			&types.SchemaDiff{ExtensionsAdded: []string{"pg_trgm"}},
			&goschema.Database{Extensions: []goschema.Extension{{Name: "pg_trgm"}}},
		},
		{"ExtensionsRemoved", &types.SchemaDiff{ExtensionsRemoved: []string{"pg_trgm"}}, &goschema.Database{}},
		{
			"FunctionsAdded",
			&types.SchemaDiff{FunctionsAdded: []string{"f"}},
			&goschema.Database{Functions: []goschema.Function{{Name: "f", Returns: "int", Body: "SELECT 1"}}},
		},
		{"FunctionsRemoved", &types.SchemaDiff{FunctionsRemoved: []string{"f"}}, &goschema.Database{}},
		{
			"FunctionsModified",
			&types.SchemaDiff{FunctionsModified: []types.FunctionDiff{{FunctionName: "f", Changes: map[string]string{"body": "x -> y"}}}},
			&goschema.Database{Functions: []goschema.Function{{Name: "f", Returns: "int", Body: "SELECT 2"}}},
		},
		{
			"SequencesAdded",
			&types.SchemaDiff{SequencesAdded: []string{"s"}},
			&goschema.Database{Sequences: []goschema.Sequence{{Name: "s"}}},
		},
		{"SequencesRemoved", &types.SchemaDiff{SequencesRemoved: []string{"s"}}, &goschema.Database{}},
		{
			"SequencesModified",
			&types.SchemaDiff{SequencesModified: []types.SequenceDiff{{SequenceName: "s", Changes: map[string]string{"increment": "1 -> 2"}}}},
			&goschema.Database{Sequences: []goschema.Sequence{{Name: "s", Increment: &increment}}},
		},
		{
			"DomainsAdded",
			&types.SchemaDiff{DomainsAdded: []string{"d"}},
			&goschema.Database{Domains: []goschema.Domain{{Name: "d", BaseType: "TEXT"}}},
		},
		{"DomainsRemoved", &types.SchemaDiff{DomainsRemoved: []string{"d"}}, &goschema.Database{}},
		{
			"DomainsModified",
			&types.SchemaDiff{DomainsModified: []types.DomainDiff{{DomainName: "d", Changes: map[string]string{"base_type": "TEXT -> VARCHAR"}}}},
			&goschema.Database{Domains: []goschema.Domain{{Name: "d", BaseType: "VARCHAR"}}},
		},
		{
			"CompositeTypesAdded",
			&types.SchemaDiff{CompositeTypesAdded: []string{"ct"}},
			&goschema.Database{CompositeTypes: []goschema.CompositeType{
				{Name: "ct", Fields: []goschema.CompositeTypeField{{Name: "a", Type: "TEXT"}}},
			}},
		},
		{"CompositeTypesRemoved", &types.SchemaDiff{CompositeTypesRemoved: []string{"ct"}}, &goschema.Database{}},
		{
			"CompositeTypesModified",
			&types.SchemaDiff{CompositeTypesModified: []types.CompositeTypeDiff{{TypeName: "ct", Changes: map[string]string{"fields": "a -> b"}}}},
			&goschema.Database{CompositeTypes: []goschema.CompositeType{
				{Name: "ct", Fields: []goschema.CompositeTypeField{{Name: "b", Type: "TEXT"}}},
			}},
		},
		{
			"RangesAdded",
			&types.SchemaDiff{RangesAdded: []string{"r"}},
			&goschema.Database{Ranges: []goschema.Range{{Name: "r", Subtype: "int4"}}},
		},
		{"RangesRemoved", &types.SchemaDiff{RangesRemoved: []string{"r"}}, &goschema.Database{}},
		{
			"RangesModified",
			&types.SchemaDiff{RangesModified: []types.RangeDiff{{
				RangeName:      "r",
				Changes:        map[string]string{"subtype": "timestamptz -> int8"},
				CurrentSubtype: "timestamptz",
			}}},
			&goschema.Database{Ranges: []goschema.Range{{Name: "r", Subtype: "int8"}}},
		},
		{
			"ViewsAdded",
			&types.SchemaDiff{ViewsAdded: []string{"v"}},
			&goschema.Database{Views: []goschema.View{{Name: "v", Body: "SELECT 1"}}},
		},
		{"ViewsRemoved", &types.SchemaDiff{ViewsRemoved: []string{"v"}}, &goschema.Database{}},
		{
			"ViewsModified",
			&types.SchemaDiff{ViewsModified: []types.ViewDiff{{ViewName: "v", Changes: map[string]string{"body": "a -> b"}}}},
			&goschema.Database{Views: []goschema.View{{Name: "v", Body: "SELECT 2"}}},
		},
		{
			"MaterializedViewsAdded",
			&types.SchemaDiff{MaterializedViewsAdded: []string{"mv"}},
			&goschema.Database{MaterializedViews: []goschema.MaterializedView{{Name: "mv", Body: "SELECT 1"}}},
		},
		{"MaterializedViewsRemoved", &types.SchemaDiff{MaterializedViewsRemoved: []string{"mv"}}, &goschema.Database{}},
		{
			"MaterializedViewsModified",
			&types.SchemaDiff{MaterializedViewsModified: []types.MaterializedViewDiff{{ViewName: "mv", Changes: map[string]string{"body": "a -> b"}}}},
			&goschema.Database{MaterializedViews: []goschema.MaterializedView{{Name: "mv", Body: "SELECT 2"}}},
		},
		{
			"TriggersAdded",
			&types.SchemaDiff{TriggersAdded: []types.TriggerRef{{TriggerName: "tg", TableName: "t"}}},
			&goschema.Database{Triggers: []goschema.Trigger{
				{Name: "tg", Table: "t", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW", Body: "RETURN NEW;"},
			}},
		},
		{
			"TriggersRemoved",
			&types.SchemaDiff{TriggersRemoved: []types.TriggerRef{{TriggerName: "tg", TableName: "t"}}},
			&goschema.Database{},
		},
		{
			"TriggersModified",
			&types.SchemaDiff{TriggersModified: []types.TriggerDiff{{TriggerName: "tg", TableName: "t", Changes: map[string]string{"timing": "BEFORE -> AFTER"}}}},
			&goschema.Database{Triggers: []goschema.Trigger{
				{Name: "tg", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "RETURN NEW;"},
			}},
		},
		{
			"RLSPoliciesAdded",
			&types.SchemaDiff{RLSPoliciesAdded: []types.RLSPolicyRef{{PolicyName: "pol", TableName: "t"}}},
			&goschema.Database{RLSPolicies: []goschema.RLSPolicy{{Name: "pol", Table: "t", PolicyFor: "ALL", ToRoles: "app"}}},
		},
		{
			"RLSPoliciesRemoved",
			&types.SchemaDiff{RLSPoliciesRemoved: []types.RLSPolicyRef{{PolicyName: "pol", TableName: "t"}}},
			&goschema.Database{},
		},
		{
			"RLSPoliciesModified",
			&types.SchemaDiff{RLSPoliciesModified: []types.RLSPolicyDiff{{PolicyName: "pol", TableName: "t", Changes: map[string]string{"using": "a -> b"}}}},
			&goschema.Database{RLSPolicies: []goschema.RLSPolicy{{Name: "pol", Table: "t", PolicyFor: "ALL", ToRoles: "app"}}},
		},
		{"RLSEnabledTablesAdded", &types.SchemaDiff{RLSEnabledTablesAdded: []string{"t"}}, &goschema.Database{}},
		{"RLSEnabledTablesRemoved", &types.SchemaDiff{RLSEnabledTablesRemoved: []string{"t"}}, &goschema.Database{}},
		{
			"RolesAdded",
			&types.SchemaDiff{RolesAdded: []string{"app"}},
			&goschema.Database{Roles: []goschema.Role{{Name: "app"}}},
		},
		{"RolesRemoved", &types.SchemaDiff{RolesRemoved: []string{"app"}}, &goschema.Database{}},
		{
			"RolesModified",
			&types.SchemaDiff{RolesModified: []types.RoleDiff{{RoleName: "app", Changes: map[string]string{"login": "false -> true"}}}},
			&goschema.Database{Roles: []goschema.Role{{Name: "app", Login: true}}},
		},
		{
			"GrantsAdded",
			&types.SchemaDiff{GrantsAdded: []types.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"}}},
			&goschema.Database{},
		},
		{
			"GrantsRemoved",
			&types.SchemaDiff{GrantsRemoved: []types.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"}}},
			&goschema.Database{},
		},
		{
			"GrantOptionsAdded",
			&types.SchemaDiff{GrantOptionsAdded: []types.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", WithOption: true}}},
			&goschema.Database{},
		},
		{
			"GrantOptionsRevoked",
			&types.SchemaDiff{GrantOptionsRevoked: []types.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", WithOption: true}}},
			&goschema.Database{},
		},
		{
			"ConstraintsAdded",
			&types.SchemaDiff{ConstraintsAdded: []string{"uq"}},
			&goschema.Database{
				Tables:      []goschema.Table{{Name: "t", StructName: "T"}},
				Constraints: []goschema.Constraint{{Name: "uq", StructName: "T", Type: "UNIQUE", Columns: []string{"c"}}},
			},
		},
		{
			"ConstraintsAddedWithTables",
			&types.SchemaDiff{ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
				{Name: "uq", TableName: "t", Type: "UNIQUE", Columns: []string{"c"}},
			}},
			&goschema.Database{},
		},
		{"ConstraintsRemoved", &types.SchemaDiff{ConstraintsRemoved: []string{"uq"}}, &goschema.Database{}},
		{
			"ConstraintsRemovedWithTables",
			&types.SchemaDiff{ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
				{Name: "uq", TableName: "t", Type: "UNIQUE"},
			}},
			&goschema.Database{},
		},
	}
}
