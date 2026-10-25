package postgres_test

import (
	"context"
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/planner/dialects/postgres"
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
	"DeclaredTableDependencies":       "the table dependency graph, carried so removals can be ordered child-before-parent alongside tables that remain; it renders nothing on its own, and a fixture would assert that a graph plans nothing (stokaro/ptah#2315)",
	"DeclaredUserTypes":               "the declaration's type vocabulary, which a created column's type is resolved THROUGH rather than rendered FROM. It creates no operation by itself: TestPlanner_CreatesAColumnTypedByADeclaredDomain drives it as part of a table creation, which is the only way it can be exercised (stokaro/ptah#2315)",
	"DeclaredSchemas":                 "every schema the declaration holds, carried so a CREATE SCHEMA the plan emits can carry the comment, character set and collation the author wrote for it. The plan reaches a schema through an object's qualifier, so the name arrives and nothing else does; on its own the list plans nothing, and TestGenerateSchemaDiffSQLStatements_ACreatedSchemaCarriesItsComment drives it through a table that names a schema, which is the only way it can be exercised (stokaro/ptah#2618)",
	"DeclaredViewLikes":               "every declared view and materialized view, which a cascading DROP is resolved AGAINST rather than rendered from. The recreate it feeds belongs to the drop that cascaded, and several fixtures below carry it for exactly that reason; on its own it plans nothing (stokaro/ptah#2315)",
	"DeclaredConstraintHosts":         "the declaration of every table a constraint change names, carried for a target that has to rebuild the table to change a constraint on it. PostgreSQL adds and drops constraints in place and never rebuilds, so this planner reads the field nowhere; a fixture here would assert that a list of table declarations plans nothing (stokaro/ptah#2315)",
	"ObservedConstraintHosts":         "PostgreSQL changes constraints in place; observed rebuild operands do not add PostgreSQL operations",
	"SkippedFeatures":                 "the objects the comparison left out because the target does not serve their model; PostgreSQL serves every model a skip names, and the plan's comment for one is written by the planner host for every dialect, which TestOffFamilyTargetsSkipADeclaredExtension drives",
	"DeclaredIndexes":                 "every declared index with its relation resolved, which a view rebuild reads to put back the indexes of a materialized view it drops and creates again. It renders only beside the rebuild that reads it; TestViewRebuild_PutsBackWhatTheDropTook_HappyPath drives it (stokaro/ptah#4976)",
	"DeclaredGrants":                  "every privilege the declaration grants, which a view rebuild reads to grant again what the drop of a view, a materialized view or a routine took. It renders only beside the rebuild that reads it; TestViewRebuild_PutsBackWhatTheDropTook_HappyPath drives it (stokaro/ptah#4976)",
	"DeclaredTriggers":                "every declared trigger, which a view rebuild reads to put back the triggers of a view it drops and creates again. It renders only beside the rebuild that reads it; TestViewRebuild_PutsBackWhatTheDropTook_HappyPath drives it (stokaro/ptah#4976)",
	"RoutinesKept":                    "the declared routines the database holds unchanged, with their drop identities, which a view rebuild reads to drop and create again a routine that uses a rebuilt view's row type. It renders only beside that rebuild; TestViewRebuild_PutsBackWhatTheDropTook_HappyPath drives it (stokaro/ptah#4980)",
	"CurrentViewOptions":              "the options the read reported on each view and materialized view, such as security_invoker, which Ptah does not declare. A view rebuild reads it to refuse recreating a view that has any; it renders nothing (stokaro/ptah#4976)",
	"CurrentGrants":                   "every grant the read of the database reported, carried for a target that rebuilds a table and has to give the new one the grants the old one held. PostgreSQL changes its tables in place and never rebuilds one, so this planner reads the field nowhere",
	"CurrentNotDescribed":             "what the read of the database declined to describe, carried for a target that rebuilds a table and must not drop a setting the read left out. PostgreSQL changes its tables in place and never rebuilds one, so this planner reads the field nowhere; a fixture here would assert that a coverage set plans nothing",
	"CurrentDatabasePath":             "the absolute path of the database the read describes, carried for a target that names a Serial column's sequence, and some objects of a GRANT, by an absolute path. PostgreSQL addresses every object relative to the connection, so this planner reads the field nowhere; a fixture here would assert that a path plans nothing",
	"Features":                        "every named feature object on each side, carried for a planner whose rule about one object depends on others the change set does not name, such as the YDB planner keeping the tables an async replication owns. It is an INPUT to a planner rather than a change: the changes are FeatureChanges, and a fixture here would assert that a context plans nothing",
	"ForeignKeysRemovedWithTables":    "supplements matching ConstraintsRemoved entries with column identities for MySQL/MariaDB drop ordering; it creates no operation by itself and PostgreSQL deliberately ignores it",
	"FunctionsRemovedWithSignatures":  "the same removals FunctionsRemoved names, with the argument list that makes each one addressable; the planner reads this list and falls back to the bare names, so it creates no operation of its own and a fixture would exercise the same DROP twice (stokaro/ptah#2296)",
	"ProceduresRemovedWithSignatures": "ProceduresRemoved with signatures, supplemental for the same reason",
	"Matched":                         "how many objects of each kind the comparison paired, which a drift report counts each side's objects with. It is not a change and no planner reads it, so a fixture here would assert that a count plans nothing",
}

// refusedDiffCategories are the categories the planner answers with an error
// rather than with SQL. Each carries a fixture, and
// TestEveryRefusedDiffCategoryIsRefused drives it: an exemption that only
// removed a category from the walk would let a category stop being refused
// without anything noticing, which is the failure this whole file exists to
// prevent one level down.
var refusedDiffCategories = map[string]refusedFixture{
	"FeatureChanges": {
		why:  "a named feature change requires an owning planning handler; PostgreSQL refuses a YDB changefeed instead of dropping it",
		diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbschema.ChangefeedRef("", "events", "feed"), Value: &ydbdiff.Changefeed{After: &ydbschema.DesiredChangefeed{Spec: ydbschema.ChangefeedSpec{Name: "feed", Mode: "UPDATES", Format: "JSON"}}}}}},
	},
	"SchemasAdded": {
		why:  "the comparison records a created database only for a whole MySQL or MariaDB server (stokaro/ptah#3789), so a PostgreSQL plan reaches one only through a diff built by hand, and planning nothing would call the two sides equal",
		diff: &difftypes.SchemaDiff{SchemasAdded: []schemamodel.Schema{{Name: "app"}}},
	},
	"SchemasRemoved": {
		why:  "a dropped database is a whole-server MySQL-family change for the same reason",
		diff: &difftypes.SchemaDiff{SchemasRemoved: []string{"app"}},
	},
	"SchemasModified": {
		why:  "a database's character set and collation are a whole-server MySQL-family change for the same reason",
		diff: &difftypes.SchemaDiff{SchemasModified: []difftypes.SchemaChange{{Name: "app", Collate: "C"}}},
	},
	"RoleMembershipsAdded": {
		why:  "the comparison records a membership only on a target with role_membership, which is YDB's, so a PostgreSQL plan reaches one only through a diff built by hand, and planning nothing would leave the member without what it was declared to hold",
		diff: &difftypes.SchemaDiff{RoleMembershipsAdded: []difftypes.RoleMembershipRef{{Role: "readers", Member: "app"}}},
	},
	"RoleMembershipsRemoved": {
		why:  "the comparison records a membership only on a target with role_membership, which is YDB's; planning nothing would leave the member holding what it was declared not to hold",
		diff: &difftypes.SchemaDiff{RoleMembershipsRemoved: []difftypes.RoleMembershipRef{{Role: "readers", Member: "app"}}},
	},
	"IndexesRenamed": {
		why:  "the comparison pairs a removed index with an added one only on a target with index_rename, which is YDB's, so a PostgreSQL plan reaches a rename only through a diff built by hand, and planning nothing would leave the index under its old name",
		diff: &difftypes.SchemaDiff{IndexesRenamed: []difftypes.IndexRename{{TableName: "users", From: "a", To: "b"}}},
	},
	"RLSPolicyIdentityConflicts": {
		why: "row-level security on this family belongs to its owner, and two declared policies that resolve to one identity are refused by the source that read them (stokaro/ptah#2440)",
		diff: &difftypes.SchemaDiff{RLSPolicyIdentityConflicts: []difftypes.RLSPolicyConflict{{
			First:  schemamodel.RLSPolicy{Name: "tenant", Table: "orders", PolicyFor: "ALL", UsingExpression: "a = 1"},
			Second: schemamodel.RLSPolicy{Name: "tenant", Table: "public.orders", PolicyFor: "ALL", UsingExpression: "b = 2"},
		}}},
	},
	"RLSPoliciesAdded": {
		why:  "row-level security on this family belongs to its owner, whose sources never fill the shared lists, so a shared entry reaches the planner only in a diff built by hand",
		diff: &difftypes.SchemaDiff{RLSPoliciesAdded: []difftypes.RLSPolicyRef{{PolicyName: "pol", TableName: "t", Desired: schemamodel.RLSPolicy{Name: "pol", Table: "t"}}}},
	},
	"RLSPoliciesRemoved": {
		why:  "the PostgreSQL reader reports policies to their owner, for the same reason",
		diff: &difftypes.SchemaDiff{RLSPoliciesRemoved: []difftypes.RLSPolicyRef{{PolicyName: "pol", TableName: "t"}}},
	},
	"RLSPoliciesModified": {
		why:  "a changed shared policy is the owner's for the same reason",
		diff: &difftypes.SchemaDiff{RLSPoliciesModified: []difftypes.RLSPolicyDiff{{PolicyName: "pol", TableName: "t", Changes: map[string]string{"using": "a -> b"}}}},
	},
	"RLSEnabledTablesAdded": {
		why:  "a table's switches are a facet of the owner's on this family",
		diff: &difftypes.SchemaDiff{RLSEnabledTablesAdded: difftypes.RLSEnabledTableChanges{{Table: "t"}}},
	},
	"RLSEnabledTablesRemoved": {
		why:  "a table's switches are a facet of the owner's on this family",
		diff: &difftypes.SchemaDiff{RLSEnabledTablesRemoved: difftypes.RLSEnabledTableChanges{{Table: "t"}}},
	},
	"RLSForceChanged": {
		why:  "FORCE is one of the switches the owner holds",
		diff: &difftypes.SchemaDiff{RLSForceChanged: difftypes.RLSForceChanges{{Table: "t", Forced: true}}},
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

			nodes, err := postgres.New().GenerateMigrationAST(
				context.Background(), builtintest.Runtime(),
				fixture.diff,
			)

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
			nodes, err := postgres.New().GenerateMigrationAST(
				context.Background(), builtintest.Runtime(),
				withDeclaredObjects(fixture.diff, fixture.desired),
			)

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
		// The walk covers every field kind, not slices alone. A category
		// spelled as a struct or a map that a slice-only walk skips is not
		// exempt, and a gate reporting on a set it chose not to look at reports
		// nothing. The full walk names exactly the three carries below.
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
	for field, fixture := range refusedDiffCategories {
		c.Assert(fixture.why, qt.Not(qt.Equals), "", qt.Commentf("%s", field))
	}
}

func diffCategoryFixtures() []categoryFixture {
	oneTable := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "t", StructName: "T"}},
		Fields: []schemamodel.Field{{Name: "c", StructName: "T", Type: "TEXT"}},
	}
	increment := int64(2)

	return []categoryFixture{
		{"TablesAdded", &difftypes.SchemaDiff{TablesAdded: difftypes.TableCreationsFor(oneTable, identifier.ForDialect("postgres"), "t")}, oneTable},
		{"TablesRemoved", &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "t"}}}, &schemamodel.Database{}},
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
			// Planned so the renderer refuses it by capability; PostgreSQL has
			// no invisible index.
			"IndexVisibilityChanged",
			&difftypes.SchemaDiff{IndexVisibilityChanged: []difftypes.IndexVisibilityChange{{
				TableName: "t", Name: "t_idx", Invisible: true,
			}}},
			&schemamodel.Database{},
		},
		{
			"IndexCommentsChanged",
			&difftypes.SchemaDiff{IndexCommentsChanged: []difftypes.IndexCommentChange{{
				TableName: "t", Name: "t_idx", Current: "old", Desired: "new",
			}}},
			&schemamodel.Database{},
		},
		{
			"ConstraintCommentsChanged",
			&difftypes.SchemaDiff{ConstraintCommentsChanged: []difftypes.ConstraintCommentChange{{
				TableName: "t", Name: "t_pos", Current: "old", Desired: "new",
			}}},
			&schemamodel.Database{},
		},
		{
			"ConstraintsValidated",
			&difftypes.SchemaDiff{ConstraintsValidated: []difftypes.ConstraintValidation{{TableName: "t", Name: "t_pos"}}},
			&schemamodel.Database{},
		},
		{
			"ObjectCommentsChanged",
			&difftypes.SchemaDiff{ObjectCommentsChanged: []difftypes.ObjectCommentChange{{
				Kind: difftypes.CommentedView, Name: "v", Current: "old", Desired: "new",
			}}},
			&schemamodel.Database{},
		},
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
