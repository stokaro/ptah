package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// unhostableSchema declares one of every object kind a non-PostgreSQL target
// cannot host, plus a table so the plan carries a real statement too.
func unhostableSchema() *goschema.Database {
	start := int64(1000)
	return &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pg_trgm"}},
		Sequences:  []goschema.Sequence{{Name: "order_number_seq", AsType: "bigint", Start: &start}},
		Roles:      []goschema.Role{{Name: "app_role"}},
		Functions:  []goschema.Function{{Name: "bump", Returns: "integer", Language: "sql", Body: "SELECT 1;"}},
		Tables:     []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", Name: "n", Type: "INTEGER", Nullable: true, Check: "n > 0"},
		},
		Views:             []goschema.View{{StructName: "V", Name: "v1", Body: "SELECT id FROM t"}},
		MaterializedViews: []goschema.MaterializedView{{StructName: "MV", Name: "mv1", Body: "SELECT id FROM t"}},
		RLSEnabledTables:  []goschema.RLSEnabledTable{{StructName: "S", Table: "t"}},
		RLSPolicies: []goschema.RLSPolicy{{
			StructName: "S", Name: "p1", Table: "t", PolicyFor: "SELECT",
			ToRoles: "app_role", UsingExpression: "true",
		}},
		Grants: []goschema.Grant{{
			StructName: "G", Role: "app_role", Privileges: []string{"SELECT"}, OnTable: "t",
		}},
		Triggers: []goschema.Trigger{{
			StructName: "TR", Name: "trg1", Table: "t",
			Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "SELECT 1",
		}},
	}
}

// unhostableCreationDiff is the diff `schema apply` computes for
// unhostableSchema against an empty database: every declared object is an
// addition.
func unhostableCreationDiff() *types.SchemaDiff {
	return &types.SchemaDiff{
		ExtensionsAdded:        []string{"pg_trgm"},
		SequencesAdded:         []string{"order_number_seq"},
		RolesAdded:             []string{"app_role"},
		FunctionsAdded:         []string{"bump"},
		TablesAdded:            []string{"t"},
		ViewsAdded:             []string{"v1"},
		MaterializedViewsAdded: []string{"mv1"},
		RLSEnabledTablesAdded:  []string{"t"},
		RLSPoliciesAdded:       []types.RLSPolicyRef{{PolicyName: "p1", TableName: "t"}},
		GrantsAdded: []types.GrantRef{{
			Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t",
		}},
		TriggersAdded: []types.TriggerRef{{TriggerName: "trg1", TableName: "t"}},
	}
}

// diagnosticLines returns every line of the joined SQL that is a
// not-supported diagnostic comment, in order.
//
// The two surfaces cannot be compared statement for statement: `render` returns
// one statement per node while the plan is re-split from rendered SQL, which
// folds a leading comment into the statement that follows it. Comparing the
// diagnostic lines compares what the two surfaces actually say.
func diagnosticLines(statements []string) []string {
	var lines []string
	for _, statement := range statements {
		for line := range strings.SplitSeq(statement, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "--") && strings.Contains(trimmed, "not supported") {
				lines = append(lines, trimmed)
			}
		}
	}
	return lines
}

func planStatements(c *qt.C, diff *types.SchemaDiff, generated *goschema.Database, dialect string) []string {
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, dialect)
	c.Assert(err, qt.IsNil)
	return statements
}

func renderStatements(c *qt.C, generated *goschema.Database, dialect string) []string {
	statements, err := renderer.GetOrderedCreateStatements(generated, dialect)
	c.Assert(err, qt.IsNil)
	return statements
}

// TestPlan_ClickHouseNamesEveryObjectItCannotHost pins the plan path to the
// rule the render path was already held to: neither surface may drop a declared
// object in silence.
//
// stokaro/ptah#931 item 7 was closed on `schema render` only. The ClickHouse
// planner ignored every PostgreSQL-shaped diff category, so measured on live
// ClickHouse 24.8 `ptah schema apply --dry-run` on a schema declaring an
// extension, a sequence, a role, a function, a view, a materialized view and a
// trigger planned the single CREATE TABLE and said nothing about the other
// seven, while `ptah schema render --dialect clickhouse` on the same model
// named all of them.
func TestPlan_ClickHouseNamesEveryObjectItCannotHost(t *testing.T) {
	c := qt.New(t)

	planned := strings.Join(planStatements(c, unhostableCreationDiff(), unhostableSchema(), platform.ClickHouse), "\n")

	tests := []struct {
		name string
		want string
	}{
		{name: "extension", want: `-- CLICKHOUSE: CREATE EXTENSION "pg_trgm" is not supported`},
		{name: "sequence", want: `-- CLICKHOUSE: CREATE SEQUENCE "order_number_seq" is not supported`},
		{name: "role", want: `-- CLICKHOUSE: CREATE ROLE "app_role" is not supported`},
		{name: "function", want: `-- CLICKHOUSE: CREATE FUNCTION "bump" is not supported`},
		{name: "view", want: `-- CLICKHOUSE: CREATE VIEW "v1" is not supported`},
		{name: "materialized view", want: `-- CLICKHOUSE: CREATE MATERIALIZED VIEW "mv1" is not supported`},
		{name: "rls enable", want: `-- CLICKHOUSE: ALTER TABLE ENABLE ROW LEVEL SECURITY "t" is not supported`},
		{name: "rls policy", want: `-- CLICKHOUSE: CREATE POLICY "p1" is not supported`},
		{name: "grant", want: `-- CLICKHOUSE: GRANT "app_role" is not supported`},
		{name: "trigger", want: `-- CLICKHOUSE: CREATE TRIGGER "trg1" is not supported`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(planned, qt.Contains, test.want)
		})
	}
}

// TestPlan_ClickHouseRenderAndPlanGiveTheSameAnswer states the governing rule
// directly rather than through two independently pinned string lists: whatever
// `render` says about a ClickHouse target, the plan must say too, in the same
// order.
//
// A per-string test can be satisfied by a diagnostic that render does not emit,
// or miss a kind neither list happens to name. Equality of the two diagnostic
// sequences cannot.
func TestPlan_ClickHouseRenderAndPlanGiveTheSameAnswer(t *testing.T) {
	c := qt.New(t)

	rendered := diagnosticLines(renderStatements(c, unhostableSchema(), platform.ClickHouse))
	planned := diagnosticLines(planStatements(c, unhostableCreationDiff(), unhostableSchema(), platform.ClickHouse))

	c.Assert(len(rendered), qt.Equals, 10)
	c.Assert(planned, qt.DeepEquals, rendered)
}

// TestPlan_ClickHouseObjectsAloneAreNotReportedAsSynced covers the worst shape
// of the same defect: a schema whose only declared object is one ClickHouse
// cannot host produced NO statements at all, so `schema apply` printed
// "Schema is synced, no changes to be made." and exited 0 against an empty
// database. That is an affirmative false report, not under-generation.
func TestPlan_ClickHouseObjectsAloneAreNotReportedAsSynced(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Views: []goschema.View{{StructName: "V", Name: "v_only", Body: "SELECT 1"}},
	}
	diff := &types.SchemaDiff{ViewsAdded: []string{"v_only"}}

	statements := planStatements(c, diff, generated, platform.ClickHouse)

	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Contains, `-- CLICKHOUSE: CREATE VIEW "v_only" is not supported`)
}

// TestPlan_ClickHouseNamesRemovedObjectsToo pins the other direction: an object
// disappearing from the model is as invisible as one appearing in it, and the
// removal categories have their own code path.
func TestPlan_ClickHouseNamesRemovedObjectsToo(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		ExtensionsRemoved:        []string{"pg_trgm"},
		SequencesRemoved:         []string{"order_number_seq"},
		RolesRemoved:             []string{"app_role"},
		FunctionsRemoved:         []string{"bump"},
		ViewsRemoved:             []string{"v1"},
		MaterializedViewsRemoved: []string{"mv1"},
		RLSEnabledTablesRemoved:  []string{"t"},
		RLSPoliciesRemoved:       []types.RLSPolicyRef{{PolicyName: "p1", TableName: "t"}},
		GrantsRemoved: []types.GrantRef{{
			Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t",
		}},
		TriggersRemoved: []types.TriggerRef{{TriggerName: "trg1", TableName: "t"}},
	}

	planned := strings.Join(planStatements(c, diff, &goschema.Database{}, platform.ClickHouse), "\n")

	tests := []struct {
		name string
		want string
	}{
		{name: "extension", want: `-- CLICKHOUSE: DROP EXTENSION "pg_trgm" is not supported`},
		{name: "sequence", want: `-- CLICKHOUSE: DROP SEQUENCE "order_number_seq" is not supported`},
		{name: "role", want: `-- CLICKHOUSE: DROP ROLE "app_role" is not supported`},
		{name: "function", want: `-- CLICKHOUSE: DROP FUNCTION "bump" is not supported`},
		{name: "view", want: `-- CLICKHOUSE: DROP VIEW "v1" is not supported`},
		{name: "materialized view", want: `-- CLICKHOUSE: DROP MATERIALIZED VIEW "mv1" is not supported`},
		{name: "rls disable", want: `-- CLICKHOUSE: ALTER TABLE DISABLE ROW LEVEL SECURITY "t" is not supported`},
		{name: "rls policy", want: `-- CLICKHOUSE: DROP POLICY "p1" is not supported`},
		{name: "grant", want: `-- CLICKHOUSE: REVOKE "app_role" is not supported`},
		{name: "trigger", want: `-- CLICKHOUSE: DROP TRIGGER "trg1" is not supported`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(planned, qt.Contains, test.want)
		})
	}
}

// TestPlan_PostgreSQLStillPlansTheObjects is the non-interference control for
// every ClickHouse assertion above: the diagnostics must be about a target that
// cannot host these objects, not about the objects. A fix that made every
// planner emit comments would pass the tests above and fail this one.
func TestPlan_PostgreSQLStillPlansTheObjects(t *testing.T) {
	c := qt.New(t)

	planned := strings.Join(planStatements(c, unhostableCreationDiff(), unhostableSchema(), platform.Postgres), "\n")

	tests := []struct {
		name string
		want string
	}{
		{name: "extension", want: `CREATE EXTENSION`},
		{name: "sequence", want: `CREATE SEQUENCE`},
		{name: "role", want: `CREATE ROLE`},
		{name: "function", want: `CREATE OR REPLACE FUNCTION`},
		{name: "view", want: `CREATE VIEW`},
		{name: "materialized view", want: `CREATE MATERIALIZED VIEW`},
		{name: "trigger", want: `CREATE TRIGGER`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(planned, qt.Contains, test.want)
		})
	}

	c.Run("no diagnostics", func(c *qt.C) {
		c.Assert(diagnosticLines(planStatements(c, unhostableCreationDiff(), unhostableSchema(), platform.Postgres)),
			qt.HasLen, 0)
	})
}

// mysqlFamilySchema declares the two object kinds #931 items 5 and 8 moved on
// the render side: a standalone extension and a standalone sequence.
func mysqlFamilySchema() *goschema.Database {
	start := int64(1000)
	return &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pg_trgm"}},
		Sequences:  []goschema.Sequence{{Name: "order_number_seq", AsType: "bigint", Start: &start}},
		Tables:     []goschema.Table{{StructName: "T", Name: "t"}},
		Fields:     []goschema.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
	}
}

func mysqlFamilyCreationDiff() *types.SchemaDiff {
	return &types.SchemaDiff{
		ExtensionsAdded: []string{"pg_trgm"},
		SequencesAdded:  []string{"order_number_seq"},
		TablesAdded:     []string{"t"},
	}
}

// TestPlan_MySQLFamilyNamesTheExtensionAndSequenceItCannotHost is the same
// blocker as the ClickHouse one, on the other two dialects #931 touched.
//
// Items 5 and 8 moved render alone: `schema render --dialect mysql` names both
// objects, while `schema apply --dry-run` against live MySQL 9.7 and live
// MariaDB 10.11.18 planned the CREATE TABLE and said nothing about either.
func TestPlan_MySQLFamilyNamesTheExtensionAndSequenceItCannotHost(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		dialect       string
		wantExtension string
		wantSequence  string
	}{
		{
			dialect:       platform.MySQL,
			wantExtension: "-- Extension pg_trgm not supported in MySQL",
			wantSequence:  "-- CREATE SEQUENCE order_number_seq not supported in mysql",
		},
		{
			dialect:       platform.MariaDB,
			wantExtension: "-- Extension pg_trgm not supported in MariaDB",
			wantSequence:  "-- CREATE SEQUENCE order_number_seq not supported in mariadb",
		},
	}

	for _, test := range tests {
		c.Run(test.dialect, func(c *qt.C) {
			planned := strings.Join(planStatements(c, mysqlFamilyCreationDiff(), mysqlFamilySchema(), test.dialect), "\n")
			rendered := strings.Join(renderStatements(c, mysqlFamilySchema(), test.dialect), "\n")

			c.Assert(planned, qt.Contains, test.wantExtension)
			c.Assert(planned, qt.Contains, test.wantSequence)
			c.Assert(rendered, qt.Contains, test.wantExtension)
			c.Assert(rendered, qt.Contains, test.wantSequence)
		})
	}
}

// TestPlan_SQLServerKeepsTheSequenceOutOfThePlan is the inverse control for the
// MySQL-family fix. SQL Server has had sequences since 2012, and its renderer
// answers a sequence node with a flat "CREATE SEQUENCE is not supported" that
// would be a false statement about the engine, so the converter that feeds
// `render` deliberately withholds the node there. The plan path has to withhold
// it for the same reason: routing every kind to every planner would satisfy the
// test above and be wrong here.
func TestPlan_SQLServerKeepsTheSequenceOutOfThePlan(t *testing.T) {
	c := qt.New(t)

	planned := strings.Join(planStatements(c, mysqlFamilyCreationDiff(), mysqlFamilySchema(), platform.SQLServer), "\n")
	rendered := strings.Join(renderStatements(c, mysqlFamilySchema(), platform.SQLServer), "\n")

	c.Assert(planned, qt.Not(qt.Contains), "CREATE SEQUENCE")
	c.Assert(rendered, qt.Not(qt.Contains), "CREATE SEQUENCE")
	c.Assert(planned, qt.Contains, `-- SQLSERVER: extensions "pg_trgm" is not supported`)
	c.Assert(rendered, qt.Contains, `-- SQLSERVER: extensions "pg_trgm" is not supported`)
}
