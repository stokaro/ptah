package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
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
		// FOR ALL rather than FOR SELECT, which is what an annotation without
		// `for=` parses to. ClickHouse stores the two identically and answers
		// SELECT to both, so a declaration naming SELECT explicitly cannot
		// converge and this renderer names it instead of creating it
		// (stokaro/ptah#1736). The refusal has its own row in
		// clickhouse/rowpolicy_test.go; this fixture is about the created path.
		RLSPolicies: []goschema.RLSPolicy{{
			StructName: "S", Name: "p1", Table: "t", PolicyFor: "ALL",
			ToRoles: "app_role", UsingExpression: "true",
		}},
		// Qualified because a ClickHouse grant scope is a two-part pattern and
		// an offline render has no current database to attach a bare table to.
		// See internal/clickhouserbac (stokaro/ptah#1025).
		Grants: []goschema.Grant{{
			StructName: "G", Role: "app_role", Privileges: []string{"SELECT"}, OnTable: "app.t",
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
			Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t",
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
			// The marker is the renderer's own `-- <DIALECT>:` prefix rather
			// than the words "not supported". A declaration this target
			// declines for a reason that is not the engine lacking the feature
			// says so in its own words -- ClickHouse names the absent
			// table-level RLS switch rather than claiming it has no row-level
			// security -- and a phrase filter would drop exactly those lines
			// from the agreement this test enforces (stokaro/ptah#1736).
			if strings.HasPrefix(trimmed, "-- CLICKHOUSE:") {
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

// TestPlan_ClickHouseRendersViewsAndNamesUnsupportedObjects pins the plan path
// to the rule the render path is held to: executable views are planned, and no
// unsupported object disappears in silence.
//
// stokaro/ptah#931 item 7 was closed on `schema render` only. The ClickHouse
// planner ignored every PostgreSQL-shaped diff category, so measured on live
// ClickHouse 24.8 `ptah schema apply --dry-run` on a schema declaring an
// extension, a sequence, a role, a function, a view, a materialized view and a
// trigger planned the single CREATE TABLE and said nothing about the other
// seven, while `ptah schema render --dialect clickhouse` on the same model
// named all of them.
func TestPlan_ClickHouseRendersViewsAndNamesUnsupportedObjects(t *testing.T) {
	c := qt.New(t)

	planned := strings.Join(planStatements(c, unhostableCreationDiff(), unhostableSchema(), platform.ClickHouse), "\n")

	tests := []struct {
		name string
		want string
	}{
		{name: "extension", want: `-- CLICKHOUSE: CREATE EXTENSION "pg_trgm" is not supported`},
		{name: "sequence", want: `-- CLICKHOUSE: CREATE SEQUENCE "order_number_seq" is not supported`},
		{name: "role", want: "CREATE ROLE IF NOT EXISTS `app_role`"},
		{name: "function", want: `-- CLICKHOUSE: CREATE FUNCTION "bump" is not supported`},
		// Row policies are planned as real DDL now (stokaro/ptah#1736). The
		// enable half stays a diagnostic, because ClickHouse has no
		// table-level switch to render -- but the sentence names the absent
		// switch instead of claiming the engine lacks row-level security.
		{name: "rls enable", want: `-- CLICKHOUSE: table "t" needs no ENABLE ROW LEVEL SECURITY`},
		{name: "rls policy", want: "CREATE ROW POLICY IF NOT EXISTS `p1` ON `t` AS PERMISSIVE FOR SELECT USING true"},
		{name: "grant", want: "GRANT SELECT ON `app`.`t` TO `app_role`"},
		{name: "trigger", want: `-- CLICKHOUSE: CREATE TRIGGER "trg1" is not supported`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(planned, qt.Contains, test.want)
		})
	}
	c.Assert(planned, qt.Contains, "CREATE VIEW `v1` AS\nSELECT id FROM t")
	c.Assert(
		planned,
		qt.Contains,
		"CREATE MATERIALIZED VIEW `mv1` ENGINE = MergeTree ORDER BY tuple() AS\nSELECT id FROM t",
	)
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

	// Five: extensions, sequences, functions and triggers, which this target
	// does not host, plus the table-level ENABLE ROW LEVEL SECURITY, which has
	// no ClickHouse spelling even though the policy itself is real DDL now
	// (stokaro/ptah#1736). The DISABLE half belongs to the removal path and is
	// counted by TestPlan_ClickHouseNamesRemovedObjectsToo. Roles and grants
	// left this list when ClickHouse gained real RBAC (stokaro/ptah#1025). The
	// count is asserted so that a kind silently ceasing to be diagnosed is a
	// failure rather than a shorter slice nobody reads.
	c.Assert(rendered, qt.HasLen, 5)
	c.Assert(planned, qt.DeepEquals, rendered)
}

// TestPlan_ClickHouseViewAloneIsNotReportedAsSynced covers the smallest
// executable view plan against an empty database.
func TestPlan_ClickHouseViewAloneIsNotReportedAsSynced(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Views: []goschema.View{{StructName: "V", Name: "v_only", Body: "SELECT 1"}},
	}
	diff := &types.SchemaDiff{ViewsAdded: []string{"v_only"}}

	statements := planStatements(c, diff, generated, platform.ClickHouse)

	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Contains, "CREATE VIEW `v_only` AS\nSELECT 1")
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
			Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t",
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
		// Ptah does not drop ClickHouse roles: the comparator computes no role
		// removals at all, and the issue's non-goals forbid dropping a role
		// that may be shared outside the managed schema. The planner names the
		// situation instead of silently ignoring the diff category.
		{name: "role", want: `role "app_role" exists on the server and not in the schema`},
		{name: "function", want: `-- CLICKHOUSE: DROP FUNCTION "bump" is not supported`},
		{name: "rls disable", want: `-- CLICKHOUSE: table "t" has no row-level security switch to disable`},
		{name: "rls policy", want: "DROP ROW POLICY IF EXISTS `p1` ON `t`"},
		{name: "grant", want: "REVOKE SELECT ON `app`.`t` FROM `app_role`"},
		{name: "trigger", want: `-- CLICKHOUSE: DROP TRIGGER "trg1" is not supported`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(planned, qt.Contains, test.want)
		})
	}
	c.Assert(planned, qt.Contains, "DROP VIEW IF EXISTS `v1`")
	// ClickHouse drops a materialized view with DROP VIEW as well, so the
	// removal is planned rather than named.
	c.Assert(planned, qt.Contains, "DROP VIEW IF EXISTS `mv1`")
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(planned, qt.Contains, test.want)
		})
	}

	t.Run("no diagnostics", func(t *testing.T) {
		c := qt.New(t)
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
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			planned := strings.Join(planStatements(c, mysqlFamilyCreationDiff(), mysqlFamilySchema(), test.dialect), "\n")
			rendered := strings.Join(renderStatements(c, mysqlFamilySchema(), test.dialect), "\n")

			c.Assert(planned, qt.Contains, test.wantExtension)
			c.Assert(planned, qt.Contains, test.wantSequence)
			c.Assert(rendered, qt.Contains, test.wantExtension)
			c.Assert(rendered, qt.Contains, test.wantSequence)
		})
	}
}

func TestPlan_NonPostgreSQLTargetsDoNotLoseExtensionPlacementDrift(t *testing.T) {
	diff := &types.SchemaDiff{ExtensionsModified: []types.ExtensionDiff{{
		Name: "pgcrypto", FromSchema: "public", ToSchema: "extensions",
	}}}

	tests := []struct {
		dialect string
		want    string
	}{
		{dialect: platform.MySQL, want: "-- Extension pgcrypto not supported in MySQL"},
		{dialect: platform.MariaDB, want: "-- Extension pgcrypto not supported in MariaDB"},
		{dialect: platform.ClickHouse, want: `-- CLICKHOUSE: CREATE EXTENSION "pgcrypto" is not supported`},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			planned := strings.Join(planStatements(c, diff, &goschema.Database{}, test.dialect), "\n")
			c.Assert(planned, qt.Contains, test.want)
		})
	}
}

func TestPlan_ExtensionInstallationSchemaSupportedTargets(t *testing.T) {
	diff := &types.SchemaDiff{ExtensionsAdded: []string{"pgcrypto"}}
	generated := &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pgcrypto", Schema: "extensions"}},
	}

	for _, dialect := range []string{platform.Postgres, platform.YugabyteDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"CREATE SCHEMA IF NOT EXISTS \"extensions\"",
				"CREATE EXTENSION \"pgcrypto\" WITH SCHEMA \"extensions\"",
			})
		})
	}
}

func TestPlan_ExtensionInstallationSchemaUnsupportedTargetsFailBeforeAST(t *testing.T) {
	diff := &types.SchemaDiff{ExtensionsAdded: []string{"pgcrypto"}}
	generated := &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pgcrypto", Schema: "extensions"}},
	}

	for _, dialect := range []string{platform.CockroachDB, platform.Spanner} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(diff, generated, dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, dialect+` does not support PostgreSQL extension installation schema "extensions" for extension "pgcrypto"`)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestPlan_WhitespaceOnlyExtensionInstallationSchemaUnsupportedTargetsFailBeforeAST(t *testing.T) {
	diff := &types.SchemaDiff{ExtensionsAdded: []string{"pgcrypto"}}
	generated := &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pgcrypto", Schema: " "}},
	}

	for _, dialect := range []string{platform.CockroachDB, platform.Spanner} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(diff, generated, dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, dialect+` does not support PostgreSQL extension installation schema " " for extension "pgcrypto"`)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestPlan_SQLServerGeneratesTheSequenceAndStillNamesTheExtension is what the
// hold-out became.
//
// Both surfaces used to withhold the SQL Server sequence entirely, and the
// reason given was the renderer's answer: a flat "CREATE SEQUENCE is not
// supported", which is a false statement about an engine that has had sequences
// since 2012. Withholding traded that falsehood for a silent omission -- exit 0,
// no statement, no diagnostic, on a sequence the author declared. Naming Ptah's
// generator rather than the engine let both surfaces say something true
// (stokaro/ptah#929 item 5); building the path let them say the useful thing
// instead (stokaro/ptah#1626).
//
// The extension row is the control, and it is why this test still has two
// halves. A change that flipped every skip comment into a statement would
// satisfy the sequence assertion; the extension has no T-SQL form at all, so it
// must still arrive as a named skip on both surfaces.
func TestPlan_SQLServerGeneratesTheSequenceAndStillNamesTheExtension(t *testing.T) {
	c := qt.New(t)

	planned := strings.Join(planStatements(c, mysqlFamilyCreationDiff(), mysqlFamilySchema(), platform.SQLServer), "\n")
	rendered := strings.Join(renderStatements(c, mysqlFamilySchema(), platform.SQLServer), "\n")

	// Exactly one, not at least one. The two halves that used to answer for a
	// sequence -- the named skip and the real DDL -- are now one switch, and a
	// planner that forgot to turn the first off would emit the CREATE and the
	// skip comment for one object. With the capability on, the "skip" is
	// itself rendered as a bare CREATE SEQUENCE, so the duplicate is two
	// executable statements rather than a statement plus a comment, and only a
	// count can see it.
	c.Assert(countCreateSequence(executableSQL(planned)), qt.Equals, 1)
	c.Assert(countCreateSequence(executableSQL(rendered)), qt.Equals, 1)
	c.Assert(planned, qt.Not(qt.Contains), `-- SQLSERVER: CREATE SEQUENCE "order_number_seq" is not generated`)

	extension := `-- SQLSERVER: extensions "pg_trgm" is not generated for this target; skipped.`
	c.Assert(planned, qt.Contains, extension)
	c.Assert(rendered, qt.Contains, extension)
}

// executableSQL drops every SQL line comment, leaving only what a server would
// run. A named skip repeats the object's own DDL keywords inside a comment, so
// Contains over the raw text cannot separate an emitted statement from a
// diagnostic about one.
func executableSQL(sqlText string) string {
	kept := make([]string, 0, strings.Count(sqlText, "\n")+1)
	for line := range strings.SplitSeq(sqlText, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// databaseDeclaringRoles builds a desired schema whose only content is the
// named roles, in the order given.
func databaseDeclaringRoles(names ...string) *goschema.Database {
	roles := make([]goschema.Role, 0, len(names))
	for _, name := range names {
		roles = append(roles, goschema.Role{Name: name})
	}
	return &goschema.Database{Roles: roles}
}

// TestPlan_MySQLFamilyRoleRefusalNamesTheSameRoleAtEitherGate pins the sentence
// a MySQL-family plan produces for a schema carrying several roles.
//
// Planning passes two refusal gates. renderer.ValidateSchemaWithCapabilities
// reads the DESIRED schema before a dialect planner is chosen, so it sees roles
// in declaration order; the MySQL planner's own CREATE ROLE nodes are rendered
// after it, and the comparer sorts diff.RolesAdded, so that gate refuses on the
// alphabetically first role. Only whichever gate is reached first is ever seen,
// and the two named different roles for the same schema: the 016-roles fixture
// declares app_user, admin_user, readonly_user, and the live MySQL and MariaDB
// cross-database scenarios failed with `expected ... CREATE ROLE admin_user
// ..., got ... CREATE ROLE app_user ...` (stokaro/ptah#1479).
//
// A plan is the path the integration scenario takes, so this is the level the
// disagreement has to be pinned at; the second case removes the roles from the
// desired schema so the planner gate answers instead of validation.
func TestPlan_MySQLFamilyRoleRefusalNamesTheSameRoleAtEitherGate(t *testing.T) {
	// The order goschema parses the 016-roles fixture in.
	declarationOrder := []string{"app_user", "admin_user", "readonly_user"}
	// The order compare.Roles leaves diff.RolesAdded in.
	added := []string{"admin_user", "app_user", "readonly_user"}

	tests := []struct {
		name      string
		generated *goschema.Database
	}{
		{name: "validation gate", generated: databaseDeclaringRoles(declarationOrder...)},
		{name: "planner gate", generated: &goschema.Database{}},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				statements, err := planner.GenerateSchemaDiffSQLStatements(
					&types.SchemaDiff{RolesAdded: added}, test.generated, dialect)

				c.Assert(statements, qt.HasLen, 0)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err, qt.ErrorMatches,
					".*"+dialect+": CREATE ROLE admin_user: Ptah does not read or compare MySQL-family role state.*")
			})
		}
	}
}

// countCreateSequence counts the executable CREATE SEQUENCE statements in a
// rendered plan.
func countCreateSequence(sql string) int {
	count := 0
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.Contains(line, "CREATE SEQUENCE") {
			count++
		}
	}
	return count
}

// roleFamilySchema and roleFamilyCreationDiff exercise the PLAN path for roles
// and grants, which the renderer-only tests cannot reach.
//
// A mutant that made planRoles emit nothing survived every test in this
// repository until this fixture existed: the live round trip renders through
// GetOrderedCreateStatements, so the planner was never asked
// (stokaro/ptah#1698).
func roleFamilySchema() *goschema.Database {
	return &goschema.Database{
		Roles:  []goschema.Role{{StructName: "A", Name: "app_reader", Inherit: true}},
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		Grants: []goschema.Grant{{
			StructName: "A", Role: "app_reader", Privileges: []string{"SELECT"}, OnTable: "t",
		}},
	}
}

func roleFamilyCreationDiff() *types.SchemaDiff {
	return &types.SchemaDiff{
		RolesAdded:  []string{"app_reader"},
		TablesAdded: []string{"t"},
		GrantsAdded: []types.GrantRef{{
			Role: "app_reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t",
		}},
	}
}

// TestPlan_SQLServerPlansTheRoleAndGrantExactlyOnce pins the plan path for a
// target that manages roles.
//
// Exactly one, not at least one. The two halves that answer for a role -- the
// named skip and the real DDL -- are one switch, and a planner that forgot to
// turn the first off would emit both. With the capability on, the "skip" node
// renders as a bare CREATE ROLE too, so the duplicate is two executable
// statements rather than a statement plus a comment, and only a count sees it.
func TestPlan_SQLServerPlansTheRoleAndGrantExactlyOnce(t *testing.T) {
	c := qt.New(t)

	planned := strings.Join(
		planStatements(c, roleFamilyCreationDiff(), roleFamilySchema(), platform.SQLServer), "\n")

	executable := executableSQL(planned)
	c.Assert(countStatement(executable, "CREATE ROLE"), qt.Equals, 1)
	c.Assert(countStatement(executable, "GRANT SELECT"), qt.Equals, 1)
	c.Assert(executable, qt.Contains, "CREATE ROLE [app_reader]")
	c.Assert(planned, qt.Not(qt.Contains), `-- SQLSERVER: roles "app_reader" is not generated`)
}

// TestPlan_MySQLFamilyStillRefusesTheRoleItCannotManage is the control. A
// change that turned every named skip into a statement would satisfy the row
// above; the MySQL family reads no role state, and its refusal is the proof
// that the capability decided the difference rather than the dialect list.
func TestPlan_MySQLFamilyStillRefusesTheRoleItCannotManage(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			_, err := planner.GenerateSchemaDiffAST(
				roleFamilyCreationDiff(), roleFamilySchema(), dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, "app_reader")
		})
	}
}

// countStatement counts the executable statements naming a keyword.
func countStatement(sql, keyword string) int {
	count := 0
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.Contains(line, keyword) {
			count++
		}
	}
	return count
}
