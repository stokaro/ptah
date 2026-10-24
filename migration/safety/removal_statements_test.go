package safety_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff"
)

// The reasons the plan check gives the removals below, as a reader sees them.
const (
	dropSequenceReason         = "DROP SEQUENCE removes the sequence and its position; one created again can hand out a key a row already holds"
	dropViewReason             = "DROP VIEW removes a definition the plan cannot restore, and every query that reads the view fails"
	dropMaterializedViewReason = "DROP MATERIALIZED VIEW removes the view and the rows it holds"
	dropTriggerReason          = "DROP TRIGGER removes executable behavior from every write to its table"
	dropFunctionReason         = "DROP FUNCTION removes executable database behavior"
	revokeReason               = "REVOKE takes away access a role may rely on"
	revokeMembershipReason     = "removing a member from a role takes away the privileges it held through the role"
	replacedViewReason         = "DROP VIEW is followed by CREATE VIEW of the same view: the plan replaces the view rather than removing it"
	replacedTriggerReason      = "DROP TRIGGER is followed by CREATE TRIGGER of the same trigger: the plan replaces the trigger rather than removing it"
	cascadedViewReason         = "DROP VIEW with CASCADE also drops every object that depends on the view, and creating the view again does not bring them back"
	replacedViewInPlaceReason  = "CREATE OR REPLACE VIEW changes the view in place, and what every query that reads it gets"
	replacedTriggerInPlace     = "CREATE OR REPLACE TRIGGER changes the trigger in place, and what every write to its table does"
)

// verdict is what the plan check says about one statement.
type verdict struct {
	Severity safety.Severity
	Reason   string
}

// TestAssess_RatesEachRemoval pins the parsed path: the node a planner builds
// for each removal, before any renderer spells it.
func TestAssess_RatesEachRemoval(t *testing.T) {
	tests := []struct {
		name    string
		node    ast.Node
		subject string
		want    verdict
	}{
		{name: "DROP SEQUENCE", node: ast.NewDropSequence("order_seq"), subject: "order_seq",
			want: verdict{safety.Destructive, dropSequenceReason}},
		{name: "DROP VIEW", node: ast.NewDropView("active_users"), subject: "active_users",
			want: verdict{safety.Destructive, dropViewReason}},
		{name: "DROP MATERIALIZED VIEW", node: ast.NewDropMaterializedView("daily"), subject: "daily",
			want: verdict{safety.Destructive, dropMaterializedViewReason}},
		{name: "DROP TRIGGER", node: ast.NewDropTrigger("audit", "users"), subject: "audit",
			want: verdict{safety.Destructive, dropTriggerReason}},
		{name: "REVOKE", node: ast.NewRevokePrivilege("reader", "TABLE", "users", []string{"SELECT"}), subject: "users",
			want: verdict{safety.Warning, revokeReason}},
		{name: "a membership removed", node: ast.NewRevokeRoleMembership("readers", "app"), subject: "readers",
			want: verdict{safety.Warning, revokeMembershipReason}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			assessments := safety.Assess([]ast.Node{test.node})

			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(verdict{assessments[0].Severity, assessments[0].Reason}, qt.Equals, test.want)
			c.Assert(assessments[0].Subject, qt.Equals, test.subject)
		})
	}
}

// TestAssessRendered_RatesEachRemovalPerEngine pins the rendered path: each
// engine's spelling of the node, every statement it renders. On the
// PostgreSQL family a trigger takes the function Ptah generated for it with
// it, and that statement is the DROP FUNCTION it is.
func TestAssessRendered_RatesEachRemovalPerEngine(t *testing.T) {
	trigger := []verdict{{safety.Destructive, dropTriggerReason}}
	triggerWithFunction := []verdict{{safety.Destructive, dropTriggerReason}, {safety.Destructive, dropFunctionReason}}
	tests := []struct {
		dialect string
		node    ast.Node
		want    []verdict
	}{
		{dialect: "postgres", node: ast.NewDropSequence("order_seq").SetIfExists(), want: []verdict{{safety.Destructive, dropSequenceReason}}},
		{dialect: "mariadb", node: ast.NewDropSequence("order_seq").SetIfExists(), want: []verdict{{safety.Destructive, dropSequenceReason}}},
		{dialect: "sqlserver", node: ast.NewDropSequence("order_seq").SetIfExists(), want: []verdict{{safety.Destructive, dropSequenceReason}}},
		{dialect: "cockroachdb", node: ast.NewDropSequence("order_seq").SetIfExists(), want: []verdict{{safety.Destructive, dropSequenceReason}}},
		{dialect: "postgres", node: ast.NewDropView("active_users").SetIfExists(), want: []verdict{{safety.Destructive, dropViewReason}}},
		{dialect: "mysql", node: ast.NewDropView("active_users").SetIfExists(), want: []verdict{{safety.Destructive, dropViewReason}}},
		{dialect: "mariadb", node: ast.NewDropView("active_users").SetIfExists(), want: []verdict{{safety.Destructive, dropViewReason}}},
		{dialect: "sqlite", node: ast.NewDropView("active_users").SetIfExists(), want: []verdict{{safety.Destructive, dropViewReason}}},
		{dialect: "sqlserver", node: ast.NewDropView("active_users").SetIfExists(), want: []verdict{{safety.Destructive, dropViewReason}}},
		{dialect: "clickhouse", node: ast.NewDropView("active_users").SetIfExists(), want: []verdict{{safety.Destructive, dropViewReason}}},
		{dialect: "ydb", node: ast.NewDropView("active_users").SetIfExists(), want: []verdict{{safety.Destructive, dropViewReason}}},
		{dialect: "postgres", node: ast.NewDropMaterializedView("daily").SetIfExists(), want: []verdict{{safety.Destructive, dropMaterializedViewReason}}},
		{dialect: "cockroachdb", node: ast.NewDropMaterializedView("daily").SetIfExists(), want: []verdict{{safety.Destructive, dropMaterializedViewReason}}},
		// ClickHouse drops a materialized view with DROP VIEW, and the node
		// keeps its own verdict where the words are the other statement's.
		{dialect: "clickhouse", node: ast.NewDropMaterializedView("daily").SetIfExists(), want: []verdict{{safety.Destructive, dropViewReason}}},
		{dialect: "postgres", node: ast.NewDropTrigger("audit", "users").SetIfExists(), want: triggerWithFunction},
		{dialect: "cockroachdb", node: ast.NewDropTrigger("audit", "users").SetIfExists(), want: triggerWithFunction},
		{dialect: "mysql", node: ast.NewDropTrigger("audit", "users").SetIfExists(), want: trigger},
		{dialect: "mariadb", node: ast.NewDropTrigger("audit", "users").SetIfExists(), want: trigger},
		{dialect: "sqlite", node: ast.NewDropTrigger("audit", "users").SetIfExists(), want: trigger},
		{dialect: "sqlserver", node: ast.NewDropTrigger("audit", "users").SetIfExists(), want: trigger},
		{dialect: "postgres", node: ast.NewRevokePrivilege("reader", "TABLE", "users", []string{"SELECT"}), want: []verdict{{safety.Warning, revokeReason}}},
		{dialect: "mysql", node: ast.NewRevokePrivilege("reader", "TABLE", "users", []string{"SELECT"}), want: []verdict{{safety.Warning, revokeReason}}},
		{dialect: "mariadb", node: ast.NewRevokePrivilege("reader", "TABLE", "users", []string{"SELECT"}), want: []verdict{{safety.Warning, revokeReason}}},
		{dialect: "sqlserver", node: ast.NewRevokePrivilege("reader", "TABLE", "users", []string{"SELECT"}), want: []verdict{{safety.Warning, revokeReason}}},
		{dialect: "ydb", node: ast.NewRevokePrivilege("reader", "TABLE", "users", []string{"SELECT"}), want: []verdict{{safety.Warning, revokeReason}}},
		{dialect: "ydb", node: ast.NewRevokeRoleMembership("readers", "app"), want: []verdict{{safety.Warning, revokeMembershipReason}}},
	}
	runtime := must.Must(builtin.New())

	for _, test := range tests {
		t.Run(test.dialect+"/"+fmt.Sprintf("%T", test.node), func(t *testing.T) {
			c := qt.New(t)

			assessments, err := safety.AssessRendered(t.Context(), runtime, []ast.Node{test.node}, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(verdicts(assessments), qt.DeepEquals, test.want)
		})
	}
}

// TestAssessSQL_RatesEachRemovalPerEngine pins the SQL-text path: the words of
// a statement read from a migration file, as each engine spells it.
func TestAssessSQL_RatesEachRemovalPerEngine(t *testing.T) {
	tests := []struct {
		dialect   string
		statement string
		want      verdict
	}{
		{dialect: "postgres", statement: `DROP SEQUENCE IF EXISTS "order_seq"`, want: verdict{safety.Destructive, dropSequenceReason}},
		{dialect: "mariadb", statement: "DROP SEQUENCE IF EXISTS `order_seq`", want: verdict{safety.Destructive, dropSequenceReason}},
		{dialect: "sqlserver", statement: "DROP SEQUENCE [dbo].[order_seq]", want: verdict{safety.Destructive, dropSequenceReason}},
		{dialect: "postgres", statement: `drop view if exists "active_users" cascade`, want: verdict{safety.Destructive, dropViewReason}},
		{dialect: "mysql", statement: "DROP VIEW IF EXISTS `active_users`", want: verdict{safety.Destructive, dropViewReason}},
		{dialect: "sqlite", statement: `DROP VIEW IF EXISTS "active_users"`, want: verdict{safety.Destructive, dropViewReason}},
		{dialect: "clickhouse", statement: "DROP VIEW IF EXISTS app.active_users", want: verdict{safety.Destructive, dropViewReason}},
		{dialect: "ydb", statement: "DROP VIEW `active_users`", want: verdict{safety.Destructive, dropViewReason}},
		{dialect: "postgres", statement: `DROP MATERIALIZED VIEW IF EXISTS "daily" CASCADE`, want: verdict{safety.Destructive, dropMaterializedViewReason}},
		{dialect: "postgres", statement: `DROP TRIGGER IF EXISTS "audit" ON "users" CASCADE`, want: verdict{safety.Destructive, dropTriggerReason}},
		{dialect: "mysql", statement: "DROP TRIGGER IF EXISTS `audit`", want: verdict{safety.Destructive, dropTriggerReason}},
		{dialect: "sqlite", statement: `DROP TRIGGER IF EXISTS "audit"`, want: verdict{safety.Destructive, dropTriggerReason}},
		{dialect: "sqlserver", statement: "DROP TRIGGER [dbo].[audit]", want: verdict{safety.Destructive, dropTriggerReason}},
		{dialect: "postgres", statement: `REVOKE SELECT ON TABLE "users" FROM "reader"`, want: verdict{safety.Warning, revokeReason}},
		{dialect: "postgres", statement: `REVOKE GRANT OPTION FOR SELECT ON TABLE "users" FROM "reader"`, want: verdict{safety.Warning, revokeReason}},
		{dialect: "postgres", statement: `REVOKE "readers" FROM "app"`, want: verdict{safety.Warning, revokeReason}},
		{dialect: "postgres", statement: `ALTER GROUP "readers" DROP USER "app"`, want: verdict{safety.Warning, revokeMembershipReason}},
		{dialect: "mysql", statement: "REVOKE SELECT ON `app`.`users` FROM `reader`", want: verdict{safety.Warning, revokeReason}},
		{dialect: "ydb", statement: "REVOKE 'ydb.generic.read' ON `/local/users` FROM `reader`", want: verdict{safety.Warning, revokeReason}},
		{dialect: "ydb", statement: "ALTER GROUP `readers` DROP USER `app`", want: verdict{safety.Warning, revokeMembershipReason}},
	}
	runtime := must.Must(builtin.New())

	for _, test := range tests {
		t.Run(test.dialect+"/"+test.statement, func(t *testing.T) {
			c := qt.New(t)

			assessments, err := safety.AssessSQL(t.Context(), runtime, test.dialect, []string{test.statement})

			c.Assert(err, qt.IsNil)
			c.Assert(verdicts(assessments), qt.DeepEquals, []verdict{test.want})
		})
	}
}

// TestAssessSQL_RatesEachReplacementPerEngine pins how a drop is read beside
// the statements after it. A drop of a trigger or a view that a later statement
// creates again, with the same name and, for a trigger whose drop names its
// table, on the same table, is the change the drift report counts, unless it
// cascades. Every other drop is a removal: one nothing creates again, one only
// another drop follows, one a statement before it created, one of another
// object or another table, and a drop of a sequence or a materialized view,
// which loses its position or its rows whatever follows.
func TestAssessSQL_RatesEachReplacementPerEngine(t *testing.T) {
	safe := verdict{safety.Safe, "does not remove data or tighten constraints"}
	tests := []struct {
		name       string
		dialect    string
		statements []string
		want       []verdict
	}{
		{
			name: "a MySQL trigger dropped and created again", dialect: "mysql",
			statements: []string{
				"LOCK TABLES `users` WRITE",
				"DROP TRIGGER IF EXISTS `touch_users`",
				"CREATE TRIGGER `touch_users` BEFORE UPDATE ON `users` FOR EACH ROW SET NEW.email = LOWER(NEW.email)",
				"UNLOCK TABLES",
			},
			want: []verdict{safe, {safety.Warning, replacedTriggerReason}, safe, safe},
		},
		{
			name: "a MySQL trigger created with its definer", dialect: "mysql",
			statements: []string{
				"DROP TRIGGER `app`.`touch_users`",
				"CREATE DEFINER=`root`@`%` TRIGGER `app`.`touch_users` BEFORE UPDATE ON `users` FOR EACH ROW SET NEW.email = NEW.email",
			},
			want: []verdict{{safety.Warning, replacedTriggerReason}, safe},
		},
		{
			name: "a SQLite view dropped and created again", dialect: "sqlite",
			statements: []string{`DROP VIEW IF EXISTS "user_emails"`, `CREATE VIEW "user_emails" AS SELECT id, email FROM users`},
			want:       []verdict{{safety.Warning, replacedViewReason}, safe},
		},
		{
			name: "a PostgreSQL trigger dropped and created again on its table", dialect: "postgres",
			statements: []string{
				`DROP TRIGGER IF EXISTS "audit" ON "users"`,
				`CREATE TRIGGER "audit" AFTER INSERT ON "users" FOR EACH ROW EXECUTE FUNCTION audit()`,
			},
			want: []verdict{{safety.Warning, replacedTriggerReason}, safe},
		},
		{
			name: "a view dropped with CASCADE and created again", dialect: "postgres",
			statements: []string{`DROP VIEW IF EXISTS "user_emails" CASCADE`, `CREATE VIEW "user_emails" AS SELECT id, email FROM users`},
			want:       []verdict{{safety.Destructive, cascadedViewReason}, safe},
		},
		{
			name: "a view dropped and nothing created", dialect: "postgres",
			statements: []string{`DROP VIEW IF EXISTS "user_emails"`},
			want:       []verdict{{safety.Destructive, dropViewReason}},
		},
		{
			name: "a view dropped twice", dialect: "postgres",
			statements: []string{`DROP VIEW "user_emails"`, `DROP VIEW IF EXISTS "user_emails"`},
			want:       []verdict{{safety.Destructive, dropViewReason}, {safety.Destructive, dropViewReason}},
		},
		{
			name: "a view created before it is dropped", dialect: "postgres",
			statements: []string{`CREATE VIEW "user_emails" AS SELECT email FROM users`, `DROP VIEW "user_emails"`},
			want:       []verdict{safe, {safety.Destructive, dropViewReason}},
		},
		{
			name: "a renamed trigger", dialect: "mysql",
			statements: []string{
				"CREATE TRIGGER `touch_users_v2` BEFORE UPDATE ON `users` FOR EACH ROW SET NEW.email = NEW.email",
				"DROP TRIGGER IF EXISTS `touch_users`",
			},
			want: []verdict{safe, {safety.Destructive, dropTriggerReason}},
		},
		{
			name: "a trigger created again on another table", dialect: "postgres",
			statements: []string{
				`DROP TRIGGER "audit" ON "users"`,
				`CREATE TRIGGER "audit" AFTER INSERT ON "orders" FOR EACH ROW EXECUTE FUNCTION audit()`,
			},
			want: []verdict{{safety.Destructive, dropTriggerReason}, safe},
		},
		{
			name: "a view created again under a name spelled otherwise", dialect: "postgres",
			statements: []string{`DROP VIEW user_emails`, `CREATE VIEW "user_emails" AS SELECT email FROM users`},
			want:       []verdict{{safety.Destructive, dropViewReason}, safe},
		},
		{
			name: "a view replaced by a materialized view of its name", dialect: "postgres",
			statements: []string{`DROP VIEW "user_emails"`, `CREATE MATERIALIZED VIEW "user_emails" AS SELECT email FROM users`},
			want:       []verdict{{safety.Destructive, dropViewReason}, safe},
		},
		{
			name: "a materialized view dropped and created again", dialect: "postgres",
			statements: []string{`DROP MATERIALIZED VIEW "daily"`, `CREATE MATERIALIZED VIEW "daily" AS SELECT 1`},
			want:       []verdict{{safety.Destructive, dropMaterializedViewReason}, safe},
		},
		{
			name: "a sequence dropped and created again", dialect: "postgres",
			statements: []string{`DROP SEQUENCE "order_seq"`, `CREATE SEQUENCE "order_seq"`},
			want:       []verdict{{safety.Destructive, dropSequenceReason}, safe},
		},
		{
			name: "a MariaDB trigger replaced in place", dialect: "mariadb",
			statements: []string{"CREATE OR REPLACE TRIGGER `touch_users` BEFORE UPDATE ON `users` FOR EACH ROW SET NEW.email = NEW.email"},
			want:       []verdict{{safety.Warning, replacedTriggerInPlace}},
		},
		{
			name: "a view replaced in place", dialect: "postgres",
			statements: []string{`CREATE OR REPLACE VIEW "user_emails" AS SELECT email FROM users WHERE id > 0`},
			want:       []verdict{{safety.Warning, replacedViewInPlaceReason}},
		},
		{
			name: "a SQL Server view altered in place", dialect: "sqlserver",
			statements: []string{"CREATE OR ALTER VIEW [user_emails] AS SELECT email FROM users"},
			want:       []verdict{{safety.Warning, replacedViewInPlaceReason}},
		},
	}
	runtime := must.Must(builtin.New())

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			assessments, err := safety.AssessSQL(t.Context(), runtime, test.dialect, test.statements)

			c.Assert(err, qt.IsNil)
			c.Assert(verdicts(assessments), qt.DeepEquals, test.want)
		})
	}
}

// changeRow is a database that differs from the desired schema by one object:
// current is the desired schema with that object added or changed.
type changeRow struct {
	dialect  string
	category string
	// both adds to the desired schema and the database alike, and current to
	// the database alone.
	both, current func(*schemamodel.Database)
}

// removalRows are the removals a comparison of two documents produces and a
// planner plans, on an engine that has each.
var removalRows = []changeRow{
	{dialect: "postgres", category: "tables_removed", current: func(d *schemamodel.Database) {
		d.Tables = append(d.Tables, schemamodel.Table{StructName: "Audit", Name: "audit"})
		d.Fields = append(d.Fields, schemamodel.Field{StructName: "Audit", Name: "id", Type: "INTEGER", Primary: true})
	}},
	{dialect: "postgres", category: "columns_removed", current: func(d *schemamodel.Database) {
		d.Fields = append(d.Fields, schemamodel.Field{StructName: "User", Name: "nick", Type: "TEXT", Nullable: true})
	}},
	{dialect: "postgres", category: "indexes_removed", current: func(d *schemamodel.Database) {
		d.Indexes = []schemamodel.Index{{StructName: "User", Name: "idx_users_email", Fields: []string{"email"}}}
	}},
	{dialect: "postgres", category: "constraints_removed", current: func(d *schemamodel.Database) {
		d.Constraints = []schemamodel.Constraint{{StructName: "User", Table: "users", Name: "chk_email", Type: "CHECK", CheckExpression: "email <> ''"}}
	}},
	{dialect: "postgres", category: "enums_removed", current: func(d *schemamodel.Database) {
		d.Enums = []schemamodel.Enum{{Name: "mood", Values: []string{"happy", "sad"}}}
	}},
	{dialect: "postgres", category: "enum_values_removed",
		both: func(d *schemamodel.Database) {
			d.Enums = []schemamodel.Enum{{Name: "mood", Values: []string{"happy", "sad"}}}
		},
		current: func(d *schemamodel.Database) { d.Enums[0].Values = append(d.Enums[0].Values, "calm") }},
	{dialect: "postgres", category: "functions_removed", current: func(d *schemamodel.Database) {
		d.Functions = []schemamodel.Function{{Name: "add_one", Parameters: "i integer", Returns: "integer", Language: "sql", Body: "SELECT i + 1"}}
	}},
	{dialect: "postgres", category: "sequences_removed", current: func(d *schemamodel.Database) {
		d.Sequences = []schemamodel.Sequence{{Name: "order_seq"}}
	}},
	{dialect: "mariadb", category: "sequences_removed", current: func(d *schemamodel.Database) {
		d.Sequences = []schemamodel.Sequence{{Name: "order_seq"}}
	}},
	{dialect: "postgres", category: "views_removed", current: withView},
	{dialect: "mysql", category: "views_removed", current: withView},
	{dialect: "sqlite", category: "views_removed", current: withView},
	{dialect: "postgres", category: "materialized_views_removed", current: func(d *schemamodel.Database) {
		d.MaterializedViews = []schemamodel.MaterializedView{{Name: "user_count", Body: "SELECT count(*) AS n FROM users"}}
	}},
	{dialect: "postgres", category: "triggers_removed", current: withTrigger("RETURN NEW;")},
	{dialect: "mysql", category: "triggers_removed", current: withTrigger("SET NEW.email = NEW.email;")},
	{dialect: "postgres", category: "grants_removed", both: withReader, current: withGrant(false)},
	{dialect: "mysql", category: "grants_removed", both: withReader, current: withGrant(false)},
	{dialect: "postgres", category: "grant_options_removed",
		both:    func(d *schemamodel.Database) { withReader(d); withGrant(false)(d) },
		current: func(d *schemamodel.Database) { d.Grants[0].WithOption = true }},
	{dialect: "ydb", category: "role_memberships_removed",
		both:    func(d *schemamodel.Database) { d.Roles = []schemamodel.Role{{Name: "readers"}, {Name: "app"}} },
		current: func(d *schemamodel.Database) { d.Roles[1].MemberOf = []string{"readers"} }},
}

// modificationRows are the changes a plan makes by replacing a trigger or a
// view: in place where the engine has CREATE OR REPLACE, and as a drop and a
// create of the same object where it does not. Neither loses anything, so the
// plan check rates the drop as the drift report rates the change.
var modificationRows = []changeRow{
	{dialect: "mysql", category: "triggers_modified",
		both:    withTrigger("SET NEW.email = LOWER(NEW.email);"),
		current: func(d *schemamodel.Database) { d.Triggers[0].Body = "SET NEW.email = NEW.email;" }},
	{dialect: "mariadb", category: "triggers_modified",
		both:    withTrigger("SET NEW.email = LOWER(NEW.email);"),
		current: func(d *schemamodel.Database) { d.Triggers[0].Body = "SET NEW.email = NEW.email;" }},
	{dialect: "sqlite", category: "triggers_modified",
		both:    withTrigger("SELECT 2;"),
		current: func(d *schemamodel.Database) { d.Triggers[0].Body = "SELECT 1;" }},
	{dialect: "postgres", category: "triggers_modified",
		both:    withTrigger("NEW.email := lower(NEW.email); RETURN NEW;"),
		current: func(d *schemamodel.Database) { d.Triggers[0].Body = "RETURN NEW;" }},
	{dialect: "mysql", category: "views_modified", both: withViewBody("SELECT email FROM users WHERE id > 0"), current: withView},
	{dialect: "sqlite", category: "views_modified", both: withViewBody("SELECT email FROM users WHERE id > 0"), current: withView},
	{dialect: "postgres", category: "views_modified", both: withViewBody("SELECT email FROM users WHERE id > 0"), current: withView},
}

// unplannedRemovals are the removal categories no row above plans, each with
// the reason a document comparison cannot reach it.
var unplannedRemovals = map[string]string{
	"roles_removed":              "the comparison never removes a role the declaration does not name",
	"schemas_removed":            "only a comparison of a whole MySQL or MariaDB server removes a database",
	"rls_policies_removed":       "row-level security is planned by its feature owner on every engine that has it",
	"rls_enabled_tables_removed": "row-level security is planned by its feature owner on every engine that has it",
	"rls_force_removed":          "row-level security is planned by its feature owner on every engine that has it",
	"unique_protections_removed": "a qualifier of indexes_removed, which a row above plans",
	"table_constraints_removed":  "a table's own constraint lists, which only a diff assembled by hand fills",
}

// TestPlanCheck_RatesEachRemovalAsTheDriftReportDoes holds the two readings of
// one removal to each other: the drift report's category for it, and the
// highest verdict the plan check gives the statements a planner writes for it.
// A drift check at --severity destructive and migrations plan
// --check-destructive then fail and pass on the same removals.
func TestPlanCheck_RatesEachRemovalAsTheDriftReportDoes(t *testing.T) {
	runtime := must.Must(builtin.New())

	for _, row := range removalRows {
		t.Run(row.dialect+"/"+row.category, func(t *testing.T) {
			c := qt.New(t)

			findings, assessments := planChange(c, runtime, row)

			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Category, qt.Equals, row.category)
			c.Assert(len(assessments) > 0, qt.IsTrue)
			c.Assert(safety.HighestAssessment(assessments), qt.Equals, findings[0].Severity)
		})
	}
}

// TestPlanCheck_RatesEachReplacementAsTheDriftReportDoes is the same agreement
// for a trigger or a view the plan changes: a warning in both readings, on
// every engine, whether the plan replaces the object in place or drops it
// and creates it again.
func TestPlanCheck_RatesEachReplacementAsTheDriftReportDoes(t *testing.T) {
	runtime := must.Must(builtin.New())

	for _, row := range modificationRows {
		t.Run(row.dialect+"/"+row.category, func(t *testing.T) {
			c := qt.New(t)

			findings, assessments := planChange(c, runtime, row)

			c.Assert(findings, qt.DeepEquals, []safety.Finding{{Category: row.category, Count: 1, Severity: safety.Warning}})
			c.Assert(safety.HighestAssessment(assessments), qt.Equals, safety.Warning)
		})
	}
}

// TestPlanCheck_RatesAViewDroppedWithCascadeAsDestructive is the replacement
// that is not one. PostgreSQL refuses CREATE OR REPLACE VIEW when the view's
// column list changes, so the plan drops the view with CASCADE and creates it
// again, and CASCADE takes every object that depends on the view with it. The
// drift report still counts a changed view; the plan check rates the drop by
// what it removes.
func TestPlanCheck_RatesAViewDroppedWithCascadeAsDestructive(t *testing.T) {
	c := qt.New(t)
	row := changeRow{
		dialect: "postgres", category: "views_modified",
		both: withViewBody("SELECT id, email FROM users"), current: withView,
	}

	findings, assessments := planChange(c, must.Must(builtin.New()), row)

	c.Assert(findings, qt.DeepEquals, []safety.Finding{{Category: "views_modified", Count: 1, Severity: safety.Warning}})
	c.Assert(assessments, qt.Not(qt.HasLen), 0)
	c.Assert(verdict{assessments[0].Severity, assessments[0].Reason}, qt.Equals, verdict{safety.Destructive, cascadedViewReason})
	c.Assert(assessments[0].Statement, qt.Contains, "CASCADE")
	c.Assert(safety.HighestAssessment(assessments), qt.Equals, safety.Destructive)
}

// planChange compares the row's documents, plans the difference, and returns
// the drift report's findings and the plan check's verdicts on it.
func planChange(c *qt.C, runtime *engine.Runtime, row changeRow) ([]safety.Finding, []safety.StatementAssessment) {
	c.Helper()
	desired, current := changeDocuments(row)
	diff, err := schemadiff.CompareSchemas(c.Context(), desired, current, row.dialect, runtime)
	c.Assert(err, qt.IsNil)
	nodes, err := planner.GenerateSchemaDiffAST(c.Context(), runtime, diff, row.dialect)
	c.Assert(err, qt.IsNil)
	assessments, err := safety.AssessRendered(c.Context(), runtime, nodes, row.dialect)
	c.Assert(err, qt.IsNil)
	return safety.ClassifySchemaDiff(diff), assessments
}

// TestPlanCheck_EveryRemovalHasARow keeps the rows above complete: every
// removal category the drift report lists is planned by a row or named in
// unplannedRemovals with a reason, and nothing else is named there.
func TestPlanCheck_EveryRemovalHasARow(t *testing.T) {
	c := qt.New(t)

	var listed []string
	for _, category := range safety.FindingCategories() {
		listed = append(listed, category.Category)
	}
	listed = slices.DeleteFunc(listed, func(category string) bool { return !strings.HasSuffix(category, "_removed") })

	covered := make([]string, 0, len(removalRows)+len(unplannedRemovals))
	for _, row := range removalRows {
		covered = append(covered, row.category)
	}
	for category := range unplannedRemovals {
		covered = append(covered, category)
	}
	slices.Sort(covered)
	c.Assert(slices.Compact(covered), qt.DeepEquals, listed)
}

func changeDocuments(row changeRow) (desired, current *schemamodel.Database) {
	desired, current = removalBase(), removalBase()
	for _, document := range []*schemamodel.Database{desired, current} {
		if row.both != nil {
			row.both(document)
		}
	}
	row.current(current)
	schemamodel.Finalize(desired)
	schemamodel.Finalize(current)
	return desired, current
}

// removalBase is the table every row's documents hold.
func removalBase() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
		},
	}
}

func withView(d *schemamodel.Database) { withViewBody("SELECT email FROM users")(d) }

func withViewBody(body string) func(*schemamodel.Database) {
	return func(d *schemamodel.Database) {
		d.Views = []schemamodel.View{{Name: "user_emails", Body: body}}
	}
}

func withTrigger(body string) func(*schemamodel.Database) {
	return func(d *schemamodel.Database) {
		d.Triggers = []schemamodel.Trigger{{
			StructName: "User", Name: "touch_users", Table: "users",
			Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW", Body: body,
		}}
	}
}

func withReader(d *schemamodel.Database) { d.Roles = []schemamodel.Role{{Name: "app_reader"}} }

func withGrant(option bool) func(*schemamodel.Database) {
	return func(d *schemamodel.Database) {
		d.Grants = []schemamodel.Grant{{Role: "app_reader", Privileges: []string{"SELECT"}, OnTable: "users", WithOption: option}}
	}
}

// verdicts is what each assessment says, in statement order.
func verdicts(assessments []safety.StatementAssessment) []verdict {
	out := make([]verdict, 0, len(assessments))
	for _, assessment := range assessments {
		out = append(out, verdict{assessment.Severity, assessment.Reason})
	}
	return out
}
